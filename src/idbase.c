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
        strcpy(db->basedir, basedir);
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
        sprintf(path, "%s/%s", basedir, "db.map");
        db->map = mmtree64_init(path);
        for(i = 0; i < IDB_HASH_MAX; i++)
        {
            if(db->state->roots[i] < 1)
                db->state->roots[i] = mmtree64_new_tree(db->map);
        }
        sprintf(path, "%s/%s", basedir, "db.mm32");
        db->mm32 = mm32_init(path);
        sprintf(path, "%s/%s", basedir, "mdb/");
        db->mdb = cdb_init(path, CDB_USE_MMAP);
        MUTEX_INIT(db->mutex);
#ifdef HAVE_PTHREAD
        for(i = 0; i < IDB_MUTEX_MAX; i++)
        {
            pthread_mutex_init(&(db->mutexs[i]), NULL);
        }
#endif
        sprintf(path, "%s/mm/", basedir);
        idbase_mkdir(path);
        for(i = 0; i < IDB_FIELDS_MAX; i++)
        {
            if(db->state->m32[i].max > 0)
            {
                sprintf(path, "%s/mm/%d.m32", basedir, i);
                if((db->state->m32[i].fd = open(path, O_CREAT|O_RDWR, 0644)) > 0)
                {
                    db->state->m32[i].map = (unsigned int *)mmap(NULL, db->state->m32[i].size, 
                            PROT_READ|PROT_WRITE, MAP_SHARED, db->state->m32[i].fd, 0);
                }
            }
#ifdef HAVE_PTHREAD
            pthread_mutex_init(&(db->state->m32[i].mutex), NULL);
            pthread_mutex_init(&(db->state->m64[i].mutex), NULL);
            pthread_mutex_init(&(db->state->m96[i].mutex), NULL);
#endif
        } 
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
    int ret = -1, id = 0, i = 0, k = 0;
    char path[IDB_PATH_MAX];
    off_t old = 0;

    if(db && key && record && (id = idbase_kid(db, key)) > 0)
    {
        k = id % IDB_HASH_MAX;
        for(i = 0; i < record->m32_num; i++) 
        {
            IDB_MUTEX_LOCK(db->state->m32[i].mutex);
            if(db->state->m32[i].roots[k] == 0) 
                db->state->m32[i].roots[k] = mm32_new_tree(db->mm32);
            if(id >= db->state->m32[i].max)
            {
                old = db->state->m32[i].end;
                if(db->state->m32[i].max == 0)
                {
                    sprintf(path, "%s/mm/%d.m32", db->basedir, i);
                    db->state->m32[i].size = (off_t)IDB_NODE_MAX * (off_t)sizeof(unsigned int);
                    if((db->state->m32[i].fd = open(path, O_CREAT|O_RDWR, 0644)) > 0)
                    {
                        db->state->m32[i].map = (unsigned int*)mmap(NULL, db->state->m32[i].size,
                                PROT_READ|PROT_WRITE, MAP_SHARED, db->stateio.fd, 0);
                    }
                }
                db->state->m32[i].max = ((id / IDB_INCRE_NUM) + 1) * IDB_INCRE_NUM;
                db->state->m32[i].end = (off_t)db->state->m32[i].max * (off_t)sizeof(unsigned int);
                ftruncate(db->state->m32[i].fd, db->state->m32[i].end);
                memset((char *)(db->state->m32[i].map) + old, 0, db->state->m32[i].end - old);
            }
            if(db->state->m32[i].map[id] > 0)
            {
                db->state->m32[i].map[id] = mm32_rebuild(db->mm32, db->state->m32[i].roots[k], db->state->m32[i].map[id], record->m32[i]);
            }
            else
            {
                db->state->m32[i].map[id] = mm32_build(db->mm32, db->state->m32[i].roots[k], record->m32[i], id);
            }
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
        cdb_clean(db->mdb);
        mmtree64_close(db->map);
        mm32_close(db->mm32);
        if(db->stateio.map)munmap(db->stateio.map, db->stateio.end);
        if(db->stateio.fd)close(db->stateio.fd);
#ifdef HAVE_PTHREAD
        for(i = 0; i < IDB_MUTEX_MAX; i++)
        {
            pthread_mutex_destroy(&(db->mutexs[i]));
        }
#endif
        for(i = 0; i < IDB_FIELDS_MAX; i++)
        {
            if(db->state->m32[i].map) munmap(db->state->m32[i].map, db->state->m32[i].size);
            db->state->m32[i].map = NULL;
            if(db->state->m32[i].fd > 0)
            {
                close(db->state->m32[i].fd);
                db->state->m32[i].fd = 0;
            }
#ifdef HAVE_PTHREAD
            pthread_mutex_destroy(&(db->state->m32[i].mutex));
            pthread_mutex_destroy(&(db->state->m64[i].mutex));
            pthread_mutex_destroy(&(db->state->m96[i].mutex));
#endif
        }
        LOGGER_CLEAN(db->logger);
        MUTEX_DESTROY(db->mutex);
        xmm_free(db, sizeof(IDBASE));
    }
    return ;
}

#ifdef _DEBUG_IDB
//gcc -o mdb idbase.c -I utils/ utils/mm32.c utils/mmtree64.c utils/cdb.c utils/logger.c utils/xmm.c utils/mmtrie.c -DHAVE_PTHREAD -D_DEBUG_IDB && ./mdb
#include "timer.h"
int main()
{
    int i = 0, j = 0, num = 10000000;
    void *timer = NULL;
    IDBASE *db = NULL;
    MRECORD record;

    if((db = idbase_init("/data/tmp/mdb")))
    {
        memset(&record, 0, sizeof(MRECORD));
        TIMER_INIT(timer);
        for(i = 0; i < num; i++)
        {
            record.m32_num = 8;
            for(j = 0; j < record.m32_num; j++) 
            {
                record.m32[j] = rand();;
            }
            idbase_build(db, (int64_t)(2000000000 + i), &record);
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "time_used:%lld avg:%lld\n", PT_LU_USEC(timer), PT_LU_USEC(timer)/num);
        idbase_close(db);
    }
    return 0;
}
#endif
