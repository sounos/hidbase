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
                    *p = '\0';                    memset(&st, 0, sizeof(struct stat));
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
    IDBASE *idb = NULL;
    char path[1024];
    int i = 0;

    if(basedir && (idb = (IDBASE *)xmm_mnew(sizeof(IDBASE))))
    {
        /* logger */
        sprintf(path, "%s/%s", basedir, "idb.log");
        idbase_mkdir(path);
        LOGGER_INIT(idb->logger, path);
        sprintf(path, "%s/%s", basedir, "xdb.state");
        if((idb->stateio.fd = open(path, O_CREAT|O_RDWR, 0644)) > 0
                && fstat(idb->stateio.fd, &st) == 0)
        {
            if((idb->stateio.map = mmap(NULL, sizeof(QSTATE), PROT_READ|PROT_WRITE,
                            MAP_SHARED, idb->stateio.fd, 0)) == NULL
                    || idb->stateio.map == (void *)-1)
            {
                FATAL_LOGGER(idb->logger, "mmap state:%s failed, %s", path, strerror(errno));
                _exit(-1);
            }
            idb->state = (QSTATE *)(idb->stateio.map);
            idb->stateio.end = st.st_size;
            if(st.st_size == 0)
            {
                idb->stateio.end = idb->stateio.size = sizeof(QSTATE);
                if(ftruncate(idb->stateio.fd, idb->stateio.end) != 0)
                {
                    FATAL_LOGGER(idb->logger, "ftruncate state %s failed, %s", path, strerror(errno));
                    _exit(-1);
                }
                memset(idb->state, 0, sizeof(QSTATE));
            }
        }
        else
        {
            fprintf(stderr, "open state file:%s failed, %s", path, strerror(errno));
            _exit(-1);
        }
        /* id map */
        sprintf(path, "%s/%s", basedir, "idb.map");
        idb->map = mmtree64_init(path);
        if(idb->state->rootid < 1)
            idb->state->rootid = mmtree64_new_tree(idb->map);
        sprintf(path, "%s/%s", basedir, "idb.mm32");
        idb->mm32 = mm32_init(path);
        MUTEX_INIT(idb->mutex);
#ifdef HAVE_PTHREAD
        for(i = 0; i < IDB_MUTEX_MAX; i++)
        {
            pthread_mutex_init(&(idb->mutexs[i]), NULL);
        }
        for(i = 0; i < IDB_FIELDS_MAX; i++)
        {
            pthread_mutex_init(&(idb->state->m32[i].mutex), NULL);
            pthread_mutex_init(&(idb->state->m64[i].mutex), NULL);
            pthread_mutex_init(&(idb->state->m96[i].mutex), NULL);
        } 
#endif
    }
    return idb;
}

void idb_mutex_lock(IDBASE *db, int id)
{
    if(db)
    {
#ifdef HAVE_PTHREAD
        pthread_mutex_lock(&(db->mutexs[id%IDB_MUTEX_MAX]));
#endif
    }
    return ;
}

void idb_mutex_unlock(IDBASE *db, int id)
{
    if(db)
    {
#ifdef HAVE_PTHREAD
        pthread_mutex_unlock(&(db->mutexs[id%IDB_MUTEX_MAX]));
#endif
    }
    return ;
}

/* close db */
void idbase_close(IDBASE *idb)
{
    int i = 0;

    if(idb)
    {
        mmtree64_close(idb->map);
        mm32_close(idb->mm32);
        if(idb->stateio.map)munmap(idb->stateio.map, idb->stateio.end);
        if(idb->stateio.fd)close(idb->stateio.fd);
#ifdef HAVE_PTHREAD
        for(i = 0; i < IDB_MUTEX_MAX; i++)
        {
            pthread_mutex_destroy(&(idb->mutexs[i]));
        }
        for(i = 0; i < IDB_FIELDS_MAX; i++)
        {
            pthread_mutex_destroy(&(idb->state->m32[i].mutex));
            pthread_mutex_destroy(&(idb->state->m64[i].mutex));
            pthread_mutex_destroy(&(idb->state->m96[i].mutex));
        }
#endif
        LOGGER_CLEAN(idb->logger);
        MUTEX_DESTROY(idb->mutex);
        xmm_free(idb, sizeof(IDBASE));
    }
    return ;
}

#ifdef _DEBUG_IDB
//gcc -o mdb idbase.c -I utils/ utils/mm32.c utils/mmtree64.c utils/cdb.c utils/logger.c utils/xmm.c utils/mmtrie.c -DHAVE_PTHREAD -D_DEBUG_IDB && ./mdb
int main()
{
    IDBASE *idb = NULL;

    if((idb = idbase_init("/tmp/idb")))
    {
        idbase_close(idb);
    }
    return 0;
}
#endif
