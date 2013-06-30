#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/statvfs.h>
#include "xmap.h"
#include "logger.h"
#include "mutex.h"
#include "cdb.h"
#include "mtrie.h"
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

#define CHECK_XMDISKIO(ox, kid)                                                                 \
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
        //xmap->mtrie = mtrie_init();
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
        sprintf(path, "%s/%s", basedir, "cache/");
        xmap->db = cdb_init(path, CDB_USE_MMAP);
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
            for(i = 0; i < DBASE_MASK_MAX; i++)
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
        xmap->state->id_wait = 0;
        while((id = mmtree_min(xmap->tree, xmap->state->qwait, &k, &v)))
        {
            mmtree_remove(xmap->tree, xmap->state->qwait, id, NULL, NULL);
        }
        while(mmqueue_pop(xmap->queue, xmap->state->qleft, (int *)&id) > 0);
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
        k = DBKMASK(key);
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
int xmap_qid(XMAP *xmap, int64_t key, int *status, XMSETS *xsets, int *num)
{
    int qid = -1, k = 0, n = 0;

    if(xmap && key && xsets && num && (qid = xmap_kid(xmap, key)) > 0)
    {
        memset(xsets, 0, sizeof(XMSETS));
        MUTEX_LOCK(xmap->mutex);  
        if(xmap->metas[qid].modtime < xmap->start_time) 
        {
            *status = XM_STATUS_FREE;
            *num = xmap->metas[qid].count = 0;
            xmap->metas[qid].modtime = time(NULL);
            xmap->metas[qid].status = XM_STATUS_WAIT;
            memset(xmap->metas[qid].disks, 0, sizeof(uint32_t) * XM_HOST_MAX);
        }
        else
        {
            if(xmap->metas[qid].status == XM_STATUS_FREE)
            {
                *num = n = xmap->metas[qid].count;
                while(--n >= 0)
                {
                    if((k = xmap->metas[qid].disks[n]) > 0
                            && k <= xmap->state->disk_id_max)
                    {
                        xsets->lists[n].ip = xmap->disks[k].ip;
                        xsets->lists[n].port = xmap->disks[k].port; 
                        xsets->lists[n].gid = xmap->disks[k].groupid;
                        xsets->lists[n].diskid = k;
                    }
                }
            }
            *status = xmap->metas[qid].status;
        }
        MUTEX_UNLOCK(xmap->mutex);
    }
    return qid;
}

/* check meta */
int xmap_check_meta(XMAP *xmap, int qid, int *status, XMSETS *xsets)
{
    int ret = -1, n = 0, k = 0;

    if(xmap && xsets && qid > 0)
    {
        MUTEX_LOCK(xmap->mutex);  
        if(qid <= xmap->state->id_max && (ret = n = xmap->metas[qid].count) > 0)
        {
            *status = xmap->metas[qid].status;
            while(--n >= 0)
            {
                if((k = xmap->metas[qid].disks[n]) > 0 && k <= xmap->state->disk_id_max)
                {
                    xsets->lists[n].ip = xmap->disks[k].ip;
                    xsets->lists[n].port = xmap->disks[k].port; 
                    xsets->lists[n].gid = xmap->disks[k].groupid;
                }
            }
        }
        MUTEX_UNLOCK(xmap->mutex);
    }
    return ret;
}

/* reset meta */
int xmap_reset_meta(XMAP *xmap, int qid)
{
    int ret = -1;

    if(xmap && qid > 0)
    {
        MUTEX_LOCK(xmap->mutex);
        if(qid <= xmap->state->id_max)
        {
            xmap->metas[qid].status = XM_STATUS_FREE;
        }
        MUTEX_UNLOCK(xmap->mutex);
    }
    return ret;
}

/* query over meta */
int xmap_over_meta(XMAP *xmap, int qid, int diskid, int *status)
{
    int ret = -1, k = 0;

    if(xmap && qid > 0 && diskid > 0 && status)
    {
        MUTEX_LOCK(xmap->mutex);
        if(qid <= xmap->state->id_max)
        {
            if((k = xmap->metas[qid].count) < XM_HOST_MAX)
            {
                xmap->metas[qid].disks[k] = diskid;
                ret = ++(xmap->metas[qid].count);
                if(ret == XM_HOST_MAX) 
                {
                    xmap->metas[qid].status = XM_STATUS_FREE;
                    xmap->metas[qid].modtime = time(NULL);
                }
                *status = xmap->metas[qid].status;
            }
        }
        MUTEX_UNLOCK(xmap->mutex);
    }
    return ret;
}


/* set groupid */
int xmap_set_groupid(XMAP *xmap, int diskid, int groupid)
{
    int ret = -1;

    if(xmap && groupid > 0 && diskid > 0 
            && diskid <= xmap->state->disk_id_max)
    {
        MUTEX_LOCK(xmap->mutex);
        ret = xmap->disks[diskid].groupid = groupid;
        xmap->disks[diskid].last = time(NULL);
        MUTEX_UNLOCK(xmap->mutex);
    }
    return ret;
}

/* get diskid */
int xmap_diskid(XMAP *xmap, char *ip, int port, int *groupid)
{
    char line[XM_PATH_MAX];
    int ret = -1, id = 0, n = 0;

    if(xmap && ip && port > 0 && (n = sprintf(line, "%s:%d", ip, port)) > 0)
    {
        MUTEX_LOCK(xmap->mutex);
        id = xmap->state->disk_id_max + 1;
        if((ret = mmtrie_add(xmap->kmap, line, n, id)) == id)
        {
            xmap->state->disk_id_max++;
            CHECK_XMDISKIO(xmap,id);
            xmap->disks[id].ip = inet_addr(ip);
            xmap->disks[id].port = port;
            xmap->disks[id].groupid = -1;
            *groupid = XM_NO_GROUPID;
            xmap->disks[id].last = time(NULL);
        }
        else
        {
            if(ret > 0 && ret <= xmap->state->disk_id_max) 
            {
                if(xmap->start_time > xmap->disks[ret].last)
                {
                    xmap->disks[ret].last = time(NULL);
                    xmap->disks[ret].groupid = -1;
                    *groupid = XM_NO_GROUPID;
                }
                if(xmap->disks[ret].groupid > 0) 
                {
                    *groupid = xmap->disks[ret].groupid; 
                }
            }
        }
        MUTEX_UNLOCK(xmap->mutex);
    }
    return ret;
}

/* return diskid */
int xmap_set_disk(XMAP *xmap, MDISK *disk)
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
            CHECK_XMDISKIO(xmap,id);
        }
        disk->groupid = xmap->disks[ret].groupid;
        memcpy(&(xmap->disks[ret]), disk, sizeof(MDISK));
        MUTEX_UNLOCK(xmap->mutex);
    }
    return ret;
}


/* lisk all disks */
int xmap_list_disks(XMAP *xmap, char *out)
{
    int ret = -1, i = 0, j = 0;
    char *p = NULL, *pp = NULL;
    unsigned char *s = NULL;

    if(xmap && (p = out) && xmap->state->disk_id_max > 0)
    {
        p += sprintf(p, "({'disklist':{");
        pp = p;
        for(i = 1; i <= xmap->state->disk_id_max; i++) 
        {
            s = (unsigned char *)&(xmap->disks[i].ip);
            p += sprintf(p, "'%d':{'ip':'%d.%d.%d.%d','sport':'%d','mode':'%d','disk':'%s','total':'%u','left':'%llu','all':'%llu', 'limit':'%llu'", i, s[0], s[1], s[2], s[3], xmap->disks[i].port, xmap->disks[i].mode, xmap->disks[i].disk, xmap->disks[i].total, LLU(xmap->disks[i].left), LLU(xmap->disks[i].all), LLU(xmap->disks[i].limit));
            if(xmap->disks[i].nmasks > 0)
            {
                p += sprintf(p, ", 'masks':{");
                for(j = 0; j < xmap->disks[i].nmasks; j++)
                {
                    if(xmap->disks[i].masks[j])
                    {
                        s = ((unsigned char *)&(xmap->disks[i].masks[j]));
                        if(j == (xmap->disks[i].nmasks - 1))
                            p += sprintf(p, "'%d':'%d.%d.%d.%d'", j, s[0], s[1], s[2], s[3]);
                        else
                            p += sprintf(p, "'%d':'%d.%d.%d.%d', ", j, s[0], s[1], s[2], s[3]);
                    }
                }
                p += sprintf(p, "}");
            }
            p += sprintf(p, "},");
        }
        if(p > pp) --p;
        p += sprintf(p, "}})");
        ret = p - out;
    }
    return ret;
}


/* truncate cache block */
int xmap_truncate_block(XMAP *xmap, int ndata, char **block)
{
    int id = 0;

    if(xmap && ndata > 0 && block)
    {
        MUTEX_LOCK(xmap->cmutex);
        if(mmqueue_pop(xmap->queue, xmap->state->qleft, &id) < 0)
            id = ++(xmap->state->id_wait);
        if((*block = cdb_truncate_block(xmap->db, id, ndata)))
        {
            mmtree_try_insert(xmap->tree, xmap->state->qwait, id, 0, NULL);
        }
        else
        {
            mmqueue_push(xmap->queue, xmap->state->qleft, id);
            id = 0;
        }
        MUTEX_UNLOCK(xmap->cmutex);
    }
    return id;
}

/* get cache info */
int xmap_cache_info(XMAP *xmap, int id, char **block)
{
    int ret = -1;

    if(xmap && id > 0 && id <= xmap->state->id_wait)
    {
        ret = cdb_exists_block(xmap->db, id, block);
    }
    return ret;
}

/* get cache len */
int xmap_cache_len(XMAP *xmap, int id)
{
    int ret = 0;

    if(xmap && id)
    {
        ret = cdb_get_data_len(xmap->db, id);
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
        ret = cdb_read_data(xmap->db, id, data);
    }
    return ret;
}

/* new task */
int xmap_new_task(XMAP *xmap, int id)
{
    unsigned int oid = 0;
    int ret = 0, n = 0;

    if(xmap && id)
    {
        MUTEX_LOCK(xmap->cmutex);
        if((oid = mmtree_find(xmap->tree, xmap->state->qwait, id, &n)) > 0)
        {
            mmtree_set_data(xmap->tree, oid, ++n);
        }
        MUTEX_UNLOCK(xmap->cmutex);
    }
    return ret;
}

/* over task */
int xmap_over_task(XMAP *xmap, int id)
{
    unsigned int oid = 0;
    int ret = -1, n = -1;

    if(xmap && id)
    {
        MUTEX_LOCK(xmap->cmutex);
        if((oid = mmtree_find(xmap->tree, xmap->state->qwait, id, &n)) > 0)
        {
            if(++n == XM_HOST_MAX)
            {
                mmtree_remove(xmap->tree, xmap->state->qwait, oid, NULL, NULL);
                mmqueue_push(xmap->queue, xmap->state->qleft, id);
                ret = cdb_del_data(xmap->db, id);
                ret = 0;
            }
            else
            {
                mmtree_set_data(xmap->tree, oid, n);
            }
        }
        MUTEX_UNLOCK(xmap->cmutex);
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
        ret = cdb_del_data(xmap->db, id);
        //FATAL_LOGGER(xmap->logger, "del_data(%d) => %d", id, ret);
    }
    return ret;
}

/* clean xmap */
void xmap_clean(XMAP *xmap)
{
    if(xmap)
    {
        WARN_LOGGER(xmap->logger, "Ready clean tree[%p]", xmap->tree);
        mmtree_close(xmap->tree);
        WARN_LOGGER(xmap->logger, "Ready clean tree64[%p]", xmap->tree64);
        mmtree64_close(xmap->tree64);
        WARN_LOGGER(xmap->logger, "Ready clean queue[%p]", xmap->queue);
        mmqueue_clean(xmap->queue);
        WARN_LOGGER(xmap->logger, "Ready clean kmap[%p]", xmap->kmap);
        mmtrie_clean(xmap->kmap);
        WARN_LOGGER(xmap->logger, "Ready reset db[%p]", xmap->db);
        cdb_reset(xmap->db);
        WARN_LOGGER(xmap->logger, "Ready clean db[%p]", xmap->db);
        cdb_clean(xmap->db);
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
        WARN_LOGGER(xmap->logger, "Ready clean mutex[%p]", xmap->mutex);
        MUTEX_DESTROY(xmap->mutex);
        MUTEX_DESTROY(xmap->cmutex);
        LOGGER_CLEAN(xmap->logger);
        xmm_free(xmap, sizeof(XMAP));
    }
    return ;
}
