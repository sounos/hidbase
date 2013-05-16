#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <sys/statvfs.h>
#include "xmap.h"
#include "logger.h"
#include "mutex.h"
#include "mtrie.h"
#include "db.h"
#include "mmqueue.h"
#include "mmtrie.h"
#include "mmtree.h"
#include "mmtree64.h"
#include "dbase.h"
#include "xmm.h"
#define LL64(zz) ((long long int)zz)
/* mkdir as path*/
int xmap_mkdir(char *path)
{
    struct stat st;
    char fullpath[XM_PATH_MAX];
    char *p = NULL;
    int level = -1, ret = -1;

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
#define CHECK_XMMETAIO(ox, kid)                                                                 \
do                                                                                              \
{                                                                                               \
    if(ox && kid >= ((off_t)ox->metaio.end/(off_t)sizeof(XMMETA)))                              \
    {                                                                                           \
        ox->metaio.old = ox->metaio.end;                                                        \
        ox->metaio.end = (off_t)((kid/XM_META_BASE)+1)*(off_t)XM_META_BASE*(off_t)sizeof(XMMETA); \
        if(ftruncate(ox->metaio.fd, ox->metaio.end)) break;                                     \
        if(ox->metaio.map)                                                                      \
        {                                                                                       \
            memset(ox->metaio.map + ox->metaio.old, 0, ox->metaio.end - ox->metaio.old);        \
        }                                                                                       \
    }                                                                                           \
}while(0)

#define CHECK_MDISKIO(ox, kid)                                                                 \
do                                                                                              \
{                                                                                               \
    if(ox && kid >= ((off_t)ox->diskio.end/(off_t)sizeof(MDISK)))                              \
    {                                                                                           \
        ox->diskio.old = ox->diskio.end;                                                        \
        ox->diskio.end = (off_t)((kid/XM_DISK_BASE)+1)*(off_t)XM_DISK_BASE*(off_t)sizeof(MDISK); \
        if(ftruncate(ox->diskio.fd, ox->diskio.end)) break;                                     \
        if(ox->diskio.map)                                                                      \
        {                                                                                       \
            memset(ox->diskio.map + ox->diskio.old, 0, ox->diskio.end - ox->diskio.old);        \
        }                                                                                       \
    }                                                                                           \
}while(0)

/* initialize XMAP */
XMAP *xmap_init(char *basedir)
{
    char path[XM_PATH_MAX];
    struct stat st = {0};
    XMAP *xmap = NULL;
    int i = 0, k = 0, v = 0;
    unsigned int id  = 0;

    if(basedir && (xmap = (XMAP *)xmm_mnew(sizeof(XMAP))))
    {
        xmap->mtrie = mtrie_init();
        MUTEX_INIT(xmap->mutex);
        /* logger */
        sprintf(path, "%s/%s", basedir, "xmap.log");        
        xmap_mkdir(path);
        LOGGER_INIT(xmap->logger, path);
        /* tree */
        sprintf(path, "%s/%s", basedir, "xmap.tree");
        xmap->tree = mmtree_init(path);
        sprintf(path, "%s/%s", basedir, "xmap.tree64");
        xmap->tree64 = mmtree64_init(path);
        /* kmap */
        sprintf(path, "%s/%s", basedir, "xmap.kmap");
        xmap->kmap = mmtrie_init(path);
        /* queue */
        sprintf(path, "%s/%s", basedir, "xmap.queue");
        xmap->queue = mmqueue_init(path);
        /* db */
        sprintf(path, "%s/%s", basedir, "db/");
        xmap->db = db_init(path, DB_USE_MMAP);
        /* state */
        sprintf(path, "%s/%s", basedir, "xmap.state");
        if((xmap->stateio.fd = open(path, O_CREAT|O_RDWR, 0644)) > 0
                && fstat(xmap->stateio.fd, &st) == 0)
        {
            if((xmap->stateio.map = mmap(NULL, sizeof(XMSTATE), PROT_READ|PROT_WRITE,
                            MAP_SHARED, xmap->stateio.fd, 0)) == NULL
                    || xmap->stateio.map == (void *)-1)
            {
                FATAL_LOGGER(xmap->logger, "mmap state:%s failed, %s", path, strerror(errno));
                _exit(-1);
            }
            xmap->state = (XMSTATE *)(xmap->stateio.map);
            xmap->stateio.end = st.st_size;
            if(st.st_size == 0)
            {
                xmap->stateio.end = xmap->stateio.size = sizeof(XMSTATE);
                if(ftruncate(xmap->stateio.fd, xmap->stateio.end) != 0)
                {
                    FATAL_LOGGER(xmap->logger, "ftruncate state %s failed, %s", path, strerror(errno));
                    _exit(-1);
                }
                memset(xmap->state, 0, sizeof(XMSTATE));
            }
            for(i = 0; i < DBASE_MASK; i++)
            {
                if(xmap->state->masks[i].root == 0)
                    xmap->state->masks[i].root = mmtree64_new_tree(xmap->tree64);
            }
        }
        /* disk */
        sprintf(path, "%s/%s", basedir, "xmap.disk");
        if((xmap->diskio.fd = open(path, O_CREAT|O_RDWR, 0644)) > 0
                && fstat(xmap->diskio.fd, &st) == 0)
        {
            if((xmap->diskio.map = mmap(NULL, sizeof(MDISK) * XM_DISK_MAX, PROT_READ|PROT_WRITE,
                            MAP_SHARED, xmap->diskio.fd, 0)) == NULL
                    || xmap->diskio.map == (void *)-1)
            {
                FATAL_LOGGER(xmap->logger, "mmap disk:%s failed, %s", path, strerror(errno));
                _exit(-1);
            }
            xmap->disks = (MDISK *)(xmap->diskio.map);
            xmap->diskio.end = st.st_size;
        }
        /* meta */
        sprintf(path, "%s/%s", basedir, "xmap.meta");
        if((xmap->metaio.fd = open(path, O_CREAT|O_RDWR, 0644)) > 0
                && fstat(xmap->metaio.fd, &st) == 0)
        {
            if((xmap->metaio.map = mmap(NULL, sizeof(XMMETA) * XM_META_MAX, PROT_READ|PROT_WRITE,
                            MAP_SHARED, xmap->metaio.fd, 0)) == NULL
                    || xmap->metaio.map == (void *)-1)
            {
                FATAL_LOGGER(xmap->logger, "mmap meta:%s failed, %s", path, strerror(errno));
                _exit(-1);
            }
            xmap->metas = (XMMETA *)(xmap->metaio.map);
            xmap->metaio.end = st.st_size;
            xmap->start_time = time(NULL);
        }
        if(!xmap->state->qwait) xmap->state->qwait = mmtree_new_tree(xmap->tree);
        if(!xmap->state->qleft) xmap->state->qleft = mmqueue_new(xmap->queue);
        while((id = mmtree_min(xmap->tree, xmap->state->qwait, &k, &v)))
        {
            mmtree_remove(xmap->tree, xmap->state->qwait, id, NULL, NULL);
            mmqueue_push(xmap->queue, xmap->state->qleft, k);
        }
        //db_destroy(xmap->db);
    }
    return xmap;
}

/* kid */
int xmap_kid(XMAP *xmap, int64_t key)
{
    int id = -1, k = 0, x = 0, old = 0, root = 0;
    if(xmap && key) 
    {
        MUTEX_LOCK(xmap->mutex); 
        k = (key % DBASE_MASK);
        if((root = xmap->state->masks[k].root) > 0)
        {
            x = xmap->state->id_max + 1;
            if(mmtree64_try_insert(xmap->tree64, root, key, x, &old) > 0)
            {
                if(old > 0)
                {
                    id = old;
                }
                else
                {
                    id = ++(xmap->state->id_max);
                    xmap->state->masks[k].total++;
                    CHECK_XMMETAIO(xmap, id);
                }
            }
            else
            {
                FATAL_LOGGER(xmap->logger, "Try_insert(%lld,%d) failed", LL64(key), x);
            }
        }
        MUTEX_UNLOCK(xmap->mutex); 
    }
    return id;
}

/* query id */
int xmap_qid(XMAP *xmap, int64_t key, int *status, XMHOST *xhost)
{
    int qid = -1, k = 0;

    if(xmap && key && xhost && (qid = xmap_kid(xmap, key)) > 0)
    {
        MUTEX_LOCK(xmap->mutex);  
        if(xmap->metas[qid].modtime < xmap->start_time) 
        {
            xmap->metas[qid].count = 0;
            xmap->metas[qid].modtime = 0;
            xmap->metas[qid].status = 0;
            memset(xmap->metas[qid].hosts, 0, sizeof(XMHOST) * XM_HOST_MAX);
        }
        else
        {
            if((k = xmap->metas[qid].count) > 0)
                memcpy(xhost, &(xmap->metas[qid].hosts[k - 1]), sizeof(XMHOST));
        }
        if((*status = xmap->metas[qid].status) == XM_STATUS_FREE)
        {
            xmap->metas[qid].status = XM_STATUS_WAIT;
        }
        MUTEX_UNLOCK(xmap->mutex);
    }
    return qid;
}

int xmap_check(XMAP *xmap, int qid, XMHOST *xhost)
{
    int k = 0, ret = -1;

    if(xmap && xhost && qid > 0)
    {
        MUTEX_LOCK(xmap->mutex);  
        if(qid < xmap->state->id_max && (k = xmap->metas[qid].count) > 0)
        {
            memcpy(xhost, &(xmap->metas[qid].hosts[k - 1]), sizeof(XMHOST));
            ret = qid;
        }
        MUTEX_UNLOCK(xmap->mutex);
    }
    return ret;
}


/* query over */
int xmap_over(XMAP *xmap, int qid, XMHOST *xhost)
{
    int ret = -1, k = 0;

    if(xmap && qid > 0 && xhost)
    {
        MUTEX_LOCK(xmap->mutex);
        if(qid <= xmap->state->id_max)
        {
            if(xmap->metas[qid].status == XM_STATUS_WAIT)
            {
                xmap->metas[qid].count = 0;
                xmap->metas[qid].status = XM_STATUS_FREE;
            }
            k = xmap->metas[qid].count++;
            if(k < XM_HOST_MAX)
                memcpy(&(xmap->metas[qid].hosts[k]), xhost, sizeof(XMHOST));
            ret = qid;
        }
        MUTEX_UNLOCK(xmap->mutex);
    }
    return ret;
}

/* check host exists */
int xmap_check_host(XMAP *xmap, char *ip, int port)
{
    char host[XM_PATH_MAX];
    int ret = 0, nhost = 0;

    if(xmap && ip && port  && (nhost = sprintf(host, "%s:%d", ip, port)) > 0)
    {
        ret = mtrie_get(xmap->mtrie, host, nhost);
    }
    return ret;
}

/* add host */
int xmap_add_host(XMAP *xmap, char *ip, int port, int val)
{
    char host[XM_PATH_MAX];
    int ret = 0, nhost = 0;

    if(xmap && ip && port && (nhost = sprintf(host, "%s:%d", ip, port)) > 0)
    {
        ret = mtrie_add(xmap->mtrie, host, nhost, val);
    }
    return ret;
}

/* return diskid */
int xmap_diskid(XMAP *xmap, MDISK *disk)
{
    unsigned char *ch = NULL;
    char line[XM_PATH_MAX];
    int ret = -1, id = 0, n = 0;

    if(xmap && disk && disk->ip && disk->port > 0 
            && (ch = (unsigned char *)&(disk->ip))
            && (n = sprintf(line, "%u.%u.%u.%u:%u", 
                    ch[0], ch[1], ch[2], ch[3], disk->port)) > 0)
    {
        MUTEX_LOCK(xmap->mutex);
        id = xmap->state->disk_id_max + 1;
        if((ret = mmtrie_add(xmap->kmap, line, n, id)) == id)
        {
            xmap->state->disk_id_max++;
            CHECK_MDISKIO(xmap,id);
            memcpy(&(xmap->disks[id]), disk, sizeof(MDISK));
        }
        MUTEX_UNLOCK(xmap->mutex);
    }
    return ret;
}

/* cache data */
int xmap_cache(XMAP *xmap, char *data, int ndata)
{
    int id = 0;

    if(xmap && data && ndata > 0)
    {
        MUTEX_LOCK(xmap->cmutex);
        if(mmqueue_pop(xmap->queue, xmap->state->qleft, &id) < 0)
            id = ++(xmap->state->id_wait);
        db_set_data(xmap->db, id, data, ndata);
        mmtree_try_insert(xmap->tree, xmap->state->qwait, id, id, NULL);
        MUTEX_UNLOCK(xmap->cmutex);
    }
    return id;
}

/* get cache len */
int xmap_cache_len(XMAP *xmap, int id)
{
    int ret = 0;

    if(xmap && id)
    {
        ret = db_get_data_len(xmap->db, id);
        //FATAL_LOGGER(xmap->logger, "get_data_len(%d) => %d", id, ret);
    }
    return ret;
}

/* read cache */
int xmap_read_cache(XMAP *xmap, int id, char *data)
{
    int ret = 0;

    if(xmap && id && data)
    {
        ret = db_read_data(xmap->db, id, data);
    }
    return ret;
}

/* drop cache */
int xmap_drop_cache(XMAP *xmap, int id)
{
    unsigned int oid = 0;
    int ret = 0;

    if(xmap && id)
    {
        oid = mmtree_find(xmap->tree, xmap->state->qwait, id, NULL);
        if(oid > 0)
        {
            mmtree_remove(xmap->tree, xmap->state->qwait, oid, NULL, NULL);
            mmqueue_push(xmap->queue, xmap->state->qleft, id);
        }
        ret = db_del_data(xmap->db, id);
        //FATAL_LOGGER(xmap->logger, "del_data(%d) => %d", id, ret);
    }
    return ret;
}

/* clean xmap */
void xmap_clean(XMAP *xmap)
{
    if(xmap)
    {
        mmtree_close(xmap->tree);
        mmtree64_close(xmap->tree64);
        mmqueue_clean(xmap->queue);
        mmtrie_clean(xmap->kmap);
        mtrie_clean(xmap->mtrie);
        db_reset(xmap->db);
        db_clean(xmap->db);
        if(xmap->diskio.map) 
        {
            munmap(xmap->diskio.map, xmap->diskio.size);
            xmap->diskio.map = NULL;
        }
        if(xmap->diskio.fd > 0) 
        {
            close(xmap->diskio.fd);
            xmap->diskio.fd = 0;
        }
        if(xmap->metaio.map) 
        {
            munmap(xmap->metaio.map, xmap->metaio.size);
            xmap->metaio.map = NULL;
        }
        if(xmap->metaio.fd > 0) 
        {
            close(xmap->metaio.fd);
            xmap->metaio.fd = 0;
        }
        if(xmap->stateio.map) 
        {
            munmap(xmap->stateio.map, xmap->stateio.size);
            xmap->stateio.map = NULL;
        }
        if(xmap->stateio.fd > 0) 
        {
            close(xmap->stateio.fd);
            xmap->stateio.fd = 0;
        }
        MUTEX_DESTROY(xmap->mutex);
        MUTEX_DESTROY(xmap->cmutex);
        LOGGER_CLEAN(xmap->logger);
        xmm_free(xmap, sizeof(XMAP));
    }
    return ;
}
