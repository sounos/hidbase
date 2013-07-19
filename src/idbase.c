#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/statvfs.h>
#include "idbase.h"
#include "cdb.h"
#include "mm32.h"
#include "xmm.h"
#include "logger.h"
#include "mutex.h"
#include "mmtree64.h"
#include "timer.h"
#define IDB_MUTEX_LOCK(xxxx) pthread_mutex_lock(&(xxxx))
#define IDB_MUTEX_UNLOCK(xxxx) pthread_mutex_unlock(&(xxxx))
/* mkdir as path*/
int idbase_mkdir(char *path)
{
    int level = -1, ret = -1;
    struct stat st;
    char fullpath[1024];
    char *p = NULL;

    if(path)
    {
        strcpy(fullpath, path);
        p = fullpath;
        while(*p != '\0')
        {
            if(*p == '/' )
            {
                level++;
                while(*p != '\0' && *p == '/' && *(p+1) == '/')++p;
                if(level > 0)
                {
                    *p = '\0';
                    memset(&st, 0, sizeof(struct stat));
                    ret = stat(fullpath, &st);
                    if(ret == 0 && !S_ISDIR(st.st_mode)) return -1;
                    if(ret != 0 && mkdir(fullpath, 0755) != 0) return -1;
                    *p = '/';
                }
            }
            ++p;
        }
        return 0;
    }
    return -1;
}


IDBASE *idbase_init(char *basedir)
{
    struct stat st = {0};
    IDBASE *db = NULL;
    char path[1024];
    int i = 0;

    if(basedir && (db = (IDBASE *)xmm_mnew(sizeof(IDBASE))))
    {
        /* logger */
        sprintf(path, "%s/%s", basedir, "db.log");
        idbase_mkdir(path);
        LOGGER_INIT(db->logger, path);
        sprintf(path, "%s/%s", basedir, "xdb.state");
        if((db->stateio.fd = open(path, O_CREAT|O_RDWR, 0644)) > 0
                && fstat(db->stateio.fd, &st) == 0)
        {
            if((db->stateio.map = mmap(NULL, sizeof(QSTATE), PROT_READ|PROT_WRITE,
                            MAP_SHARED, db->stateio.fd, 0)) == NULL
                    || db->stateio.map == (void *)-1)
            {
                FATAL_LOGGER(db->logger, "mmap state:%s failed, %s", path, strerror(errno));
                _exit(-1);
            }
            db->state = (QSTATE *)(db->stateio.map);
            db->stateio.end = st.st_size;
            if(st.st_size == 0)
            {
                db->stateio.end = db->stateio.size = sizeof(QSTATE);
                if(ftruncate(db->stateio.fd, db->stateio.end) != 0)
                {
                    FATAL_LOGGER(db->logger, "ftruncate state %s failed, %s", path, strerror(errno));
                    _exit(-1);
                }
                memset(db->state, 0, sizeof(QSTATE));
            }
        }
        else
        {
            fprintf(stderr, "open state file:%s failed, %s", path, strerror(errno));
            _exit(-1);
        }
        /* id map */
        sprintf(path, "%s/db.map", basedir);
        db->map = mmtree64_init(path);
        for(i = 0; i < IDB_HASH_MAX; i++)
        {
            if(db->state->roots[i] < 1)
                db->state->roots[i] = mmtree64_new_tree(db->map);
        }
        sprintf(path, "%s/db.mm32", basedir);
        db->mm32 = mm32_init(path);
        //sprintf(path, "%s/%s", basedir, "mdb/");
        //db->mdb = cdb_init(path, CDB_USE_MMAP);
        MUTEX_INIT(db->mutex);
#ifdef HAVE_PTHREAD
        for(i = 0; i < IDB_MUTEX_MAX; i++)
        {
            pthread_mutex_init(&(db->mutexs[i]), NULL);
        }
#endif
        sprintf(path, "%s/db.m32", basedir);
        if((db->m32io.fd = open(path, O_CREAT|O_RDWR, 0644)) > 0
                && fstat(db->m32io.fd, &st) == 0)
        {
            db->m32io.size = (off_t)sizeof(int) * IDB_NODE_MAX;
            if((db->m32io.map = mmap(NULL, db->m32io.size, PROT_READ|PROT_WRITE,
                            MAP_SHARED, db->m32io.fd, 0)) == NULL
                    || db->m32io.map == (void *)-1)
            {
                FATAL_LOGGER(db->logger, "mmap m32:%s failed, %s", path, strerror(errno));
                _exit(-1);
            }
            db->m32 = (unsigned int *)(db->m32io.map);
            db->m32io.end = st.st_size;
        }
        else
        {
            fprintf(stderr, "open m32 file:%s failed, %s", path, strerror(errno));
            _exit(-1);
        }
        sprintf(path, "%s/db.tab32", basedir);
        if((db->tab32io.fd = open(path, O_CREAT|O_RDWR, 0644)) > 0
                && fstat(db->tab32io.fd, &st) == 0)
        {
            db->tab32io.size = (off_t)sizeof(QTAB)+(off_t)sizeof(QV32) * IDB_NODE_MAX;
            if((db->tab32io.map = mmap(NULL, db->tab32io.size, PROT_READ|PROT_WRITE,
                            MAP_SHARED, db->tab32io.fd, 0)) == NULL
                    || db->tab32io.map == (void *)-1)
            {
                FATAL_LOGGER(db->logger, "mmap tab32:%s failed, %s", path, strerror(errno));
                _exit(-1);
            }
            db->tab32 = (QTAB *)(db->tab32io.map);
            db->tab32io.end = st.st_size;
            if(st.st_size == 0)
            {
                db->tab32io.end = (off_t) sizeof(QTAB);
                ftruncate(db->tab32io.fd, db->tab32io.end);
                memset(db->tab32io.map, 0, db->tab32io.end);
            }
            db->v32 = (QV32 *)(db->tab32io.map + sizeof(QTAB));
        }
        else
        {
            fprintf(stderr, "open tab32 file:%s failed, %s", path, strerror(errno));
            _exit(-1);
        }
 
        for(i = 0; i < IDB_FIELDS_MAX; i++)
        {
#ifdef HAVE_PTHREAD
            pthread_mutex_init(&(db->state->m32[i].mutex), NULL);
            pthread_mutex_init(&(db->state->m64[i].mutex), NULL);
            pthread_mutex_init(&(db->state->m96[i].mutex), NULL);
#endif
        } 
        TIMER_INIT(db->timer);
    }
    return db;
}

void idbase_mutex_lock(IDBASE *db, int id)
{
    if(db)
    {
#ifdef HAVE_PTHREAD
        pthread_mutex_lock(&(db->mutexs[id%IDB_MUTEX_MAX]));
#endif
    }
    return ;
}

void idbase_mutex_unlock(IDBASE *db, int id)
{
    if(db)
    {
#ifdef HAVE_PTHREAD
        pthread_mutex_unlock(&(db->mutexs[id%IDB_MUTEX_MAX]));
#endif
    }
    return ;
}
#define IDX_INCRE(ppt, type, base)                                                      \
do                                                                                      \
{                                                                                       \
    if(ppt->idxio.map && ppt->idxio.fd > 0)                                             \
    {                                                                                   \
        base = ppt->idxio.old = ppt->idxio.end;                                         \
        ppt->idxio.end += (off_t)sizeof(type) * (off_t)IDB_BASE_NUM;                    \
        if(ftruncate(ppt->idxio.fd, ppt->idxio.end) != 0) break;                        \
        memset(ppt->idxio.map + ppt->idxio.old, 0, (ppt->idxio.end - ppt->idxio.old));  \
    }                                                                                   \
}while(0)

/* index m32 */
int idbase_index_m32(IDBASE *db, int fid, int32_t val, int id)
{
    int ret = 0, min = 0, max = 0, des = 0;

    if(db && fid >= 0 && fid < IDB_FIELDS_MAX)
    {
    }
    return ret;
}

/* kid */
int idbase_kid(IDBASE *db, int64_t key)
{
    int id = 0, old = 0, x = 0;

    if(db && key)
    {
        MUTEX_LOCK(db->mutex);
        x = db->state->kid_max + 1;
        mmtree64_try_insert(db->map, db->state->roots[key % IDB_HASH_MAX], key, x, &old);
        if(old == 0) id = ++(db->state->kid_max);
        else id = old;
        MUTEX_UNLOCK(db->mutex);
    }
    return id;
}

/* new db id*/
int idbase_db_id(IDBASE *db)
{
    int id = 0;

    if(db)
    {
        MUTEX_LOCK(db->mutex);
        id = ++db->state->db_id_max;
        MUTEX_UNLOCK(db->mutex);
    }
    return id;
}

/* build index */
int idbase_build(IDBASE *db, int64_t key, MRECORD *record)
{
    int ret = -1, id = 0, i = 0, k = 0, n = 0, xid = 0;
    char path[IDB_PATH_MAX];
    off_t old = 0;

    if(db && key && record && (id = idbase_kid(db, key)) > 0)
    {
        k = id % IDB_HASH_MAX;
        for(i = 0; i < record->m32_num; i++) 
        {
            IDB_MUTEX_LOCK(db->state->m32[i].mutex);
            if(db->state->m32[i].rootid == 0) 
                db->state->m32[i].rootid = mm32_new_tree(db->mm32);
            while(id >= db->state->m32[i].max)
            {
                old = db->m32io.end;
                db->m32io.end += (off_t) IDB_BASE_NUM * (off_t)sizeof(int);
                ftruncate(db->m32io.fd, db->m32io.end);
                memset((char *)(db->m32io.map) + old, 0, db->m32io.end - old);
                db->state->m32[i].max += IDB_BASE_NUM;
                n = db->state->m32[i].hmap_max++;
                db->state->m32[i].hmap[n] = (off_t)old / (off_t)sizeof(int);
            }
            xid = db->state->m32[i].hmap[(id/IDB_BASE_NUM)] + (id % IDB_BASE_NUM);
            TIMER_SAMPLE(db->timer);
            if(db->m32[xid] > 0)
            {
                db->m32[xid] = mm32_rebuild(db->mm32, db->state->m32[i].rootid, db->m32[xid], record->m32[i]);
            }
            else
            {
                db->m32[xid] = mm32_build(db->mm32, db->state->m32[i].rootid, record->m32[i], id);
            }
            TIMER_SAMPLE(db->timer);
            db->state->time_used += PT_LU_USEC(db->timer);
            IDB_MUTEX_UNLOCK(db->state->m32[i].mutex);
        }
    }
    return ret;
}

/* close db */
void idbase_close(IDBASE *db)
{
    int i = 0;

    if(db)
    {
        //cdb_clean(db->mdb);
        mmtree64_close(db->map);
        mm32_close(db->mm32);
        if(db->stateio.map)munmap(db->stateio.map, db->stateio.size);
        if(db->stateio.fd > 0)close(db->stateio.fd);
        if(db->m32io.map)munmap(db->m32io.map, db->m32io.size);
        if(db->m32io.fd > 0)close(db->m32io.fd);
#ifdef HAVE_PTHREAD
        for(i = 0; i < IDB_MUTEX_MAX; i++)
        {
            pthread_mutex_destroy(&(db->mutexs[i]));
        }
#endif
        for(i = 0; i < IDB_FIELDS_MAX; i++)
        {
#ifdef HAVE_PTHREAD
            pthread_mutex_destroy(&(db->state->m32[i].mutex));
            pthread_mutex_destroy(&(db->state->m64[i].mutex));
            pthread_mutex_destroy(&(db->state->m96[i].mutex));
#endif
        }
        LOGGER_CLEAN(db->logger);
        MUTEX_DESTROY(db->mutex);
        TIMER_CLEAN(db->timer);
        xmm_free(db, sizeof(IDBASE));
    }
    return ;
}

#ifdef _DEBUG_IDB
//gcc -o mdb idbase.c -I utils/ utils/mm32.c utils/mmtree64.c utils/cdb.c utils/logger.c utils/xmm.c utils/mmtrie.c -DHAVE_PTHREAD -D_DEBUG_IDB && ./mdb
int main()
{
    int i = 0, j = 0, num = 10000000;
    void *timer = NULL;
    IDBASE *db = NULL;
    MRECORD record;

    if((db = idbase_init("/data/tmp/mdb")))
    {
        memset(&record, 0, sizeof(MRECORD));
        for(i = 0; i < num; i++)
        {
            record.m32_num = 8;
            for(j = 0; j < record.m32_num; j++) 
            {
                record.m32[j] = rand();;
            }
            idbase_build(db, (int64_t)(2000000000 + i), &record);
        }
        fprintf(stdout, "time_used:%lld build:%lld avg:%lld\n", PT_USEC_U(db->timer), db->state->time_used, PT_USEC_U(db->timer)/num);
        idbase_close(db);
    }
    return 0;
}
#endif
