#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/statvfs.h>
#include "xdbase.h"
#include "logger.h"
#include "mutex.h"
#include "mmtrie.h"
#include "mmqueue.h"
#include "mmtree64.h"
#include "dbase.h"
#include "xmm.h"
#include "db.h"
#define X_DB_DIR_NAME       "chunks"
#define X_WAIT_DIR_NAME     "wait"
#define X_CACHE_DIR_NAME    "cache"
#define LL64(zz) ((long long int)zz)
#define ULL64(zz) ((unsigned long long int)zz)
#define UPDATE_ID_MAX(xdbase)       \
do                                  \
{                                   \
    MUTEX_LOCK(xdbase->mutex);      \
    ++(xdbase->state->id_max);      \
    MUTEX_UNLOCK(xdbase->mutex);    \
}while(0)
/* mkdir as path*/
int xdbase_mkdir(char *path)
{
    struct stat st;
    char fullpath[DB_PATH_MAX];
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

/* initialize XDB */
XDBASE *xdbase_init(char *basedir, int mode)
{
    char path[XDB_PATH_MAX];
    struct stat st = {0};
    XDBASE *xdbase = NULL;
    int i = 0;

    if(basedir && (xdbase = (XDBASE *)xmm_mnew(sizeof(XDBASE))))
    {
        /* logger */
        sprintf(path, "%s/%s", basedir, "xdb.log");        
        xdbase_mkdir(path);
        LOGGER_INIT(xdbase->logger, path);
        sprintf(path, "%s/%s", basedir, "xdb.map");
        xdbase->map = mmtrie_init(path);
        sprintf(path, "%s/%s", basedir, "xdb.kmap");
        xdbase->kmap = mmtrie_init(path);
        sprintf(path, "%s/%s", basedir, "xdb.queue");
        xdbase->queue = mmqueue_init(path);
        sprintf(path, "%s/%s", basedir, "xdb.state");
        if((xdbase->stateio.fd = open(path, O_CREAT|O_RDWR, 0644)) > 0
                && fstat(xdbase->stateio.fd, &st) == 0)
        {
            if((xdbase->stateio.map = mmap(NULL, sizeof(XDBSTATE), PROT_READ|PROT_WRITE,
                            MAP_SHARED, xdbase->stateio.fd, 0)) == NULL 
                    || xdbase->stateio.map == (void *)-1)
            {
                FATAL_LOGGER(xdbase->logger, "mmap state:%s failed, %s", path, strerror(errno));
                _exit(-1);
            }
            xdbase->state = (XDBSTATE *)(xdbase->stateio.map);
            xdbase->stateio.end = st.st_size;
            if(st.st_size == 0)
            {
                xdbase->stateio.end = xdbase->stateio.size = sizeof(XDBSTATE);
                if(ftruncate(xdbase->stateio.fd, xdbase->stateio.end) != 0)
                {
                    FATAL_LOGGER(xdbase->logger, "ftruncate state %s failed, %s", path, strerror(errno));
                    _exit(-1);
                }
                memset(xdbase->state, 0, sizeof(XDBSTATE));
            }
        }
        else
        {
            fprintf(stderr, "open state file:%s failed, %s", path, strerror(errno));
            _exit(-1);
        }
        /* id map */
        sprintf(path, "%s/%s", basedir, "xdb.idmap");
        xdbase->idmap = mmtree64_init(path);
        if(xdbase->state->rootid < 1)
            xdbase->state->rootid = mmtree64_new_tree(xdbase->idmap);
        xdbase->state->mode = mode;
        /* initialize disks */
        MUTEX_INIT(xdbase->mutex); 
        for(i = 0; i < DBASE_MASK_MAX; i++)
        {
            if(xdbase->state->xdisks[i].status > 0)
            {
                sprintf(path, "%s/%s", xdbase->state->xdisks[i].disk, X_DB_DIR_NAME);
                xdbase->state->xdisks[i].db = db_init(path, xdbase->state->xdisks[i].mode);
                sprintf(path, "%s/%s", xdbase->state->xdisks[i].disk, X_WAIT_DIR_NAME);
                xdbase->state->xdisks[i].wait = db_init(path, DB_USE_MMAP);
                //sprintf(path, "%s/%s", xdbase->state->xdisks[i].disk, X_CACHE_DIR_NAME);
                //xdbase->state->xdisks[i].cache = db_init(path, DB_USE_MMAP);
                BJSON_INIT(xdbase->state->xdisks[i].record);
                MUTEX_INIT(xdbase->state->xdisks[i].mutex);
            }
            else
            {
                xdbase->state->xdisks[i].wait = NULL;
                xdbase->state->xdisks[i].db = NULL;
                //xdbase->state->xdisks[i].cache = NULL;
            }
        }
    }
    return xdbase;
}

/* add disk */
//int xdbase_add_disk(XDBASE *xdbase, int port, off_t limit, int mode, char *disk, int mask)
int xdbase_add_disk(XDBASE *xdbase, int port, off_t limit, int mode, char *disk)
{
    int diskid = -1, x = 0, n = 0;
    char path[XDB_PATH_MAX];

    if(xdbase && disk && xdbase->state && xdbase->state->nxdisks < DBASE_MASK_MAX)
    {
        MUTEX_LOCK(xdbase->mutex);
        n = strlen(disk);
        if((x = mmtrie_get(MMTR(xdbase->map), disk, n)) < 1)
        {
            x = 0;
            while(x < DBASE_MASK_MAX && xdbase->state->xdisks[x].status)++x; 
            if(x < DBASE_MASK_MAX && xdbase->state->xdisks[x].status == 0)
            {
                mmtrie_add(MMTR(xdbase->map), disk, n, x+1);
                strcpy(xdbase->state->xdisks[x].disk, disk);
                xdbase->state->xdisks[x].status = 1;
                xdbase->state->xdisks[x].limit = limit;
                xdbase->state->xdisks[x].port = port;
                xdbase->state->xdisks[x].mode = mode;
                //xdbase->state->xdisks[x].mask = mask;
                sprintf(path, "%s/%s", disk, X_DB_DIR_NAME);
                xdbase->state->xdisks[x].db = db_init(path, mode);
                sprintf(path, "%s/%s", disk, X_WAIT_DIR_NAME);
                xdbase->state->xdisks[x].wait = db_init(path, 1);
                //sprintf(path, "%s/%s", disk, X_CACHE_DIR_NAME);
                //xdbase->state->xdisks[x].cache = db_init(path, 1);
                xdbase->state->xdisks[x].qwait = mmqueue_new(xdbase->queue);
                xdbase->state->xdisks[x].qleft = mmqueue_new(xdbase->queue);
                xdbase->state->xdisks[x].qrelay = mmqueue_new(xdbase->queue);
                BJSON_INIT(xdbase->state->xdisks[x].record);
                MUTEX_INIT(xdbase->state->xdisks[x].mutex);
                xdbase->state->nxdisks++;
                diskid = x;
            }
        }
        /*
        else 
        {
            if(xdbase->state->xdisks[x].mode != mode)
            {
                xdbase->state->xdisks[x].mode = mode;
                if(xdbase->state->xdisks[x].db)
                    db_clean(xdbase->state->xdisks[x].db);
                sprintf(path, "%s/%s", disk, X_DB_DIR_NAME);
                xdbase->state->xdisks[x].db = db_init(path, mode);
            }
            diskid = x;
        }
        */
        MUTEX_UNLOCK(xdbase->mutex);
    }
    return diskid;
}

/* update disk limit */
int xdbase_set_disk_limit(XDBASE *xdbase, int diskid, off_t limit)
{
    int ret = -1;

    if(xdbase && diskid >= 0 && diskid < DBASE_MASK_MAX)
    {
        MUTEX_LOCK(xdbase->mutex);
        if(xdbase->state && xdbase->state->nxdisks > 0 
                && xdbase->state->xdisks[diskid].status)
        {
            xdbase->state->xdisks[diskid].limit = limit;
            ret = 0;
        }
        MUTEX_UNLOCK(xdbase->mutex);
    }
    return ret;
}

/* set disk mmap mode */
int xdbase_set_disk_mode(XDBASE *xdbase, int diskid, int mode)
{
    char path[XDB_PATH_MAX];
    int ret = -1;

    if(xdbase && diskid >= 0 && diskid < DBASE_MASK_MAX)
    {
        MUTEX_LOCK(xdbase->mutex);
        if(xdbase->state && xdbase->state->nxdisks > 0 
                && xdbase->state->xdisks[diskid].status > 0)
        {
            if(xdbase->state->xdisks[diskid].mode != mode)
            {
                xdbase->state->xdisks[diskid].mode = mode;
                if(PDB(xdbase->state->xdisks[diskid].db))
                    db_clean(PDB(xdbase->state->xdisks[diskid].db));
                sprintf(path, "%s/%s", xdbase->state->xdisks[diskid].disk, X_DB_DIR_NAME);
                xdbase->state->xdisks[diskid].db = db_init(path, mode);
            }
            ret = 0;
        }
        MUTEX_UNLOCK(xdbase->mutex);
    }
    return ret;
}

/* del disk */
int xdbase_del_disk(XDBASE *xdbase, int diskid)
{
    int ret = -1;

    if(xdbase && diskid >= 0 && diskid < DBASE_MASK_MAX)
    {
        MUTEX_LOCK(xdbase->mutex);
        if(xdbase->state && xdbase->state->nxdisks > 0 
                && xdbase->state->xdisks[diskid].status > 0)
        {
            if(xdbase->state->xdisks[diskid].db) 
            {
                db_destroy(xdbase->state->xdisks[diskid].db);
                db_clean(PDB(xdbase->state->xdisks[diskid].db));
            }
            if(xdbase->state->xdisks[diskid].wait) 
            {
                db_destroy(xdbase->state->xdisks[diskid].wait);
                db_clean(PDB(xdbase->state->xdisks[diskid].wait));
            }
            /*
            if(xdbase->state->xdisks[diskid].cache) 
            {
                db_destroy(xdbase->state->xdisks[diskid].cache);
                db_clean(PDB(xdbase->state->xdisks[diskid].cache));
            }
            */
            mmqueue_close(xdbase->queue, xdbase->state->xdisks[diskid].qwait);
            mmqueue_close(xdbase->queue, xdbase->state->xdisks[diskid].qleft);
            mmqueue_close(xdbase->queue, xdbase->state->xdisks[diskid].qrelay);
            MUTEX_DESTROY(xdbase->state->xdisks[diskid].mutex);
            bjson_clean(&(xdbase->state->xdisks[diskid].record));
            memset(&(xdbase->state->xdisks[diskid]), 0, sizeof(XDISK));
            ret = 0;
        }
        MUTEX_UNLOCK(xdbase->mutex);
    }
    return ret;
}

/* add mask */
int xdbase_add_mask(XDBASE *xdbase, int diskid, int mask)
{
    unsigned char *ch = NULL;
    int ret = -1, i = 0;

    if(xdbase && diskid >= 0 && diskid < DBASE_MASK_MAX)
    {
        MUTEX_LOCK(xdbase->mutex);
        if(xdbase->state && xdbase->state->xdisks[diskid].status > 0
                && xdbase->state->xdisks[diskid].nmasks < DBASE_MASK_MAX)
        {
            ch = (unsigned char *)&mask;
            if((i = DBKMASK(ch[3])) >= 0 && xdbase->state->xdisks[diskid].masks[i].mask_ip == 0)
            {
                xdbase->state->xdisks[diskid].masks[i].mask_ip = mask;
                xdbase->state->xdisks[diskid].masks[i].root = mmtree64_new_tree(xdbase->idmap);
                xdbase->state->xdisks[diskid].masks[i].total = 0;
                xdbase->state->xdisks[diskid].nmasks++;
                ret = i;
                //fprintf(stdout, "%s::%d mask:%d/%d\n", __FILE__, __LINE__, i, mask);
            }
        }
        MUTEX_UNLOCK(xdbase->mutex);
    }
    return ret;
}

/* del mask */
int xdbase_del_mask(XDBASE *xdbase, int diskid, int mask)
{
    unsigned char *ch = NULL;
    int ret = -1, i = 0;

    if(xdbase && diskid >= 0 && diskid < DBASE_MASK_MAX)
    {
        MUTEX_LOCK(xdbase->mutex);
        if(xdbase->state && xdbase->state->xdisks[diskid].status > 0
                && xdbase->state->xdisks[diskid].nmasks > 0)
        {
            ch = (unsigned char *)&mask;
            if((i = DBKMASK(ch[3])) >= 0 && xdbase->state->xdisks[diskid].masks[i].mask_ip == mask)
            {
                xdbase->state->xdisks[diskid].masks[i].mask_ip = 0;
                mmtree64_remove_tree(xdbase->idmap, xdbase->state->xdisks[diskid].masks[i].root);
                xdbase->state->xdisks[diskid].masks[i].root = 0;
                xdbase->state->xdisks[diskid].masks[i].total = 0;
                xdbase->state->xdisks[diskid].nmasks--;
                ret = i;
            }
        }
        MUTEX_UNLOCK(xdbase->mutex);
    }
    return ret;
}

/* list disks */
int xdbase_list_disks(XDBASE *xdbase, char *out)
{
    struct statvfs fs = {0};
    int ret = -1, i = 0, j = 0;
    char *p = NULL, *pp = NULL;
    unsigned char *s = NULL;

    if(xdbase && (p = out) && xdbase->state->nxdisks > 0)
    {
        MUTEX_LOCK(xdbase->mutex);
        p += sprintf(p, "({'disklist':{");
        pp = p;
        for(i = 0; i < DBASE_MASK_MAX; i++) 
        {
            if(xdbase->state->xdisks[i].status 
                    && statvfs(xdbase->state->xdisks[i].disk, &fs) == 0)
            {
                //s = ((unsigned char *)&(xdbase->state->xdisks[i].mask));
                //p += sprintf(p, "'%d':{'sport':'%d', 'name':'%s', 'bsize':'%lld', 'frsize':'%lld', 'total':'%lld', 'free':'%lld', 'max':'%d', 'mode':'%d', 'limit':'%lld', 'mask':'%d.%d.%d.%d'},", i, xdbase->state->xdisks[i].port, xdbase->state->xdisks[i].disk, (long long)fs.f_bsize, (long long)fs.f_frsize, (long long)fs.f_blocks, (long long)fs.f_bfree, xdbase->state->xdisks[i].total,xdbase->state->xdisks[i].mode,(long long)(xdbase->state->xdisks[i].limit), s[0], s[1], s[2], s[3]);
                p += sprintf(p, "'%d':{'sport':'%d', 'name':'%s', 'bsize':'%lld', 'frsize':'%lld', 'total':'%lld', 'free':'%lld', 'all':'%lld', 'left':'%lld', 'max':'%d', 'mode':'%d', 'limit':'%lld'", i, xdbase->state->xdisks[i].port, xdbase->state->xdisks[i].disk, (long long)fs.f_bsize, (long long)fs.f_frsize, (long long)fs.f_blocks, (long long)fs.f_bfree, (long long)fs.f_blocks * (long long)fs.f_bsize, (long long)fs.f_bfree * (long long)fs.f_bsize, xdbase->state->xdisks[i].total,xdbase->state->xdisks[i].mode,(long long)(xdbase->state->xdisks[i].limit));
                if(xdbase->state->xdisks[i].nmasks > 0)
                {
                    p += sprintf(p, ", 'masks':{");
                    for(j = 0; j < DBASE_MASK_MAX; j++)
                    {
                        if(xdbase->state->xdisks[i].masks[j].mask_ip)
                        {
                            s = ((unsigned char *)&(xdbase->state->xdisks[i].masks[j].mask_ip));
                            if(j == (xdbase->state->xdisks[i].nmasks - 1))
                                p += sprintf(p, "'%d':'%d.%d.%d.%d'", j, s[0], s[1], s[2], s[3]);
                            else
                                p += sprintf(p, "'%d':'%d.%d.%d.%d', ", j, s[0], s[1], s[2], s[3]);
                        }
                    }
                    p += sprintf(p, "}");
                }
                p += sprintf(p, "},");
            }
        }
        if(p > pp) --p;
        p += sprintf(p, "}})");
        ret = p - out;
        MUTEX_UNLOCK(xdbase->mutex);
    }
    return ret;
}

/* get key id */
int xdbase__kid(XDBASE *xdbase, int diskid, int64_t key)
{
    char line[DBASE_LINE_MAX];
    int id = -1, n = 0;

    if(xdbase && diskid >= 0 && diskid < DBASE_MASK_MAX 
        && xdbase->state->xdisks[diskid].status > 0 && xdbase->kmap)
    {
        n = sprintf(line, "%u/%llu", (unsigned int)diskid, ULL64(key));
        id = mmtrie_get(xdbase->kmap, line, n);
    }
    return id;
}

/* get key id */
int xdbase_kid(XDBASE *xdbase, int diskid, int64_t key)
{
    int id = -1, x = 0, k = 0, root = 0;

    if(xdbase && diskid >= 0 && diskid < DBASE_MASK_MAX 
        && xdbase->state->xdisks[diskid].status > 0 && xdbase->idmap)
    {
        if(xdbase->state->mode)
        {
            k = DBKMASK(key);
            if(xdbase->state->xdisks[diskid].masks[k].mask_ip
                && xdbase->state->xdisks[diskid].masks[k].total > 0)
                root = xdbase->state->xdisks[diskid].masks[k].root;
        }
        else
        {
            root = xdbase->state->rootid;
        }
        if(root > 0 && mmtree64_find(xdbase->idmap, root, key, &x) > 0)
        {
            id = x;
        }
    }
    return id;
}


/* gen id with key */
int xdbase__key__id(XDBASE *xdbase, int diskid, int64_t key)
{
    int id = -1, usec = 0, n = 0, k = 0;
    struct timeval tv = {0}, now = {0};
    char line[DBASE_LINE_MAX];

    if(xdbase && diskid >= 0 && diskid < DBASE_MASK_MAX && xdbase->kmap 
        && xdbase->state->xdisks[diskid].status > 0 
        && (n = sprintf(line, "%u/%llu", (unsigned int)diskid, ULL64(key))) > 0
        && (id = mmtrie_get(xdbase->kmap, line, n)) <= 0)
    {
        MUTEX_LOCK(xdbase->state->xdisks[diskid].mutex);
        gettimeofday(&tv, NULL);
        id = mmtrie_add(xdbase->kmap, line, n, ++(xdbase->state->xdisks[diskid].total));
        if(xdbase->state->mode)
        {
            k = DBKMASK(key);
            if(xdbase->state->xdisks[diskid].masks[k].mask_ip)
            {
                xdbase->state->xdisks[diskid].masks[k].total++;
            }
        }
        gettimeofday(&now, NULL);
        usec = ((off_t)now.tv_usec + (off_t)now.tv_sec * (off_t)1000000) 
            - ((off_t)tv.tv_usec + (off_t)tv.tv_sec * (off_t)1000000); 
        xdbase->state->op_total++;
        if(usec > 1000)
        {
            xdbase->state->op_slow++;
            WARN_LOGGER(xdbase->logger, "key:%lld diskid:%d usec:%d op_total:%lld op_slow:%lld", LL64(key), diskid, usec, LL64(xdbase->state->op_total), LL64(xdbase->state->op_slow));
        }
        MUTEX_UNLOCK(xdbase->state->xdisks[diskid].mutex);
        //update xdbase->state->id_max
        UPDATE_ID_MAX(xdbase);
    }
    return id;
}


/* gen id with key */
int xdbase_key_id(XDBASE *xdbase, int diskid, int64_t key)
{
    int id = -1, old = -1, x = 0, k = 0, root = 0, usec = 0;
    struct timeval tv = {0}, now = {0};

    if(xdbase && diskid >= 0 && diskid < DBASE_MASK_MAX && xdbase->idmap 
        && xdbase->state->xdisks[diskid].status > 0)
    {
        MUTEX_LOCK(xdbase->mutex);
        if(xdbase->state->mode)
        {
            k = DBKMASK(key);
            if(xdbase->state->xdisks[diskid].masks[k].mask_ip)
            {
                root = xdbase->state->xdisks[diskid].masks[k].root;
            }
        }
        else
        {
            root = xdbase->state->rootid;
        }
        if(root > 0)
        {
            x = xdbase->state->xdisks[diskid].total + 1;
            //WARN_LOGGER(xdbase->logger, "key:%lld diskid:%d id_max:%d mask:%d", key, diskid, x, k);
            gettimeofday(&tv, NULL);
            if(mmtree64_try_insert(xdbase->idmap, root, key, x, &old) > 0)
            {
                if(old > 0)
                {
                    id = old;
                }
                else
                {
                    ++(xdbase->state->id_max);
                    id = ++(xdbase->state->xdisks[diskid].total);
                    xdbase->state->xdisks[diskid].masks[k].total++;
                }
                gettimeofday(&now, NULL);
                usec = ((off_t)now.tv_usec + (off_t)now.tv_sec * (off_t)1000000) 
                    - ((off_t)tv.tv_usec + (off_t)tv.tv_sec * (off_t)1000000); 
                xdbase->state->op_total++;
                if(usec > 10000)
                {
                    xdbase->state->op_slow++;
                    WARN_LOGGER(xdbase->logger, "key:%lld diskid:%d id_max:%d mask:%d usec:%d op_total:%lld op_slow:%lld", LL64(key), diskid, x, k, usec, LL64(xdbase->state->op_total), LL64(xdbase->state->op_slow));
                }
            }
            else
            {
                FATAL_LOGGER(xdbase->logger, "Try_insert(%lld,%d) failed", LL64(key), x);
            }
        }
        MUTEX_UNLOCK(xdbase->mutex);
    }
    return id;
}

/* add data */
int xdbase_add_data(XDBASE *xdbase, int diskid, int64_t key, char *data, int ndata)
{
    int ret = -1, id = 0;
    void *db = NULL;

    if(xdbase && xdbase->state && diskid >= 0 && diskid < DBASE_MASK_MAX && data 
            && xdbase->state->xdisks[diskid].status > 0 
            && (id = xdbase_key_id(xdbase, diskid, key)) > 0
            && (db = xdbase->state->xdisks[diskid].db))
    {
        ret = db_add_data(PDB(db), id, data, ndata);
    }
    return ret;
}

/* set data */
int xdbase_set_data(XDBASE *xdbase, int diskid, int64_t key, char *data, int ndata)
{
    int ret = -1, id = 0;
    void *db = NULL;
    if(xdbase && xdbase->state && diskid >= 0 && diskid < DBASE_MASK_MAX && data 
            && xdbase->state->xdisks[diskid].status > 0
            && (db = xdbase->state->xdisks[diskid].db))
    {
        if((id = xdbase_key_id(xdbase, diskid, key)) > 0)
        {
            ret = db_set_data(PDB(db), id, data, ndata);
        }
    }
    return ret;
}
/* update data */
int xdbase_update_data(XDBASE *xdbase, int diskid, int64_t key, char *data, int ndata)
{
    int ret = -1, id = 0, nblock = 0, nold = 0;
    char *block = NULL, *old = NULL;
    BELEMENT *e1 = NULL, *e2 = NULL;
    BJSON rec = {0}, *record = &rec;
    void *db = NULL;

    if(xdbase && xdbase->state && diskid >= 0 && diskid < DBASE_MASK_MAX && data 
            && xdbase->state->xdisks[diskid].status > 0 
            && (id = xdbase_key_id(xdbase, diskid, key)) > 0
            && (db = xdbase->state->xdisks[diskid].db))
    {
        old = NULL;nold = 0;
        block = NULL; nblock = 0;
        if((nold = db_get_data(PDB(db), id, &old)) && old)
        {
            e1 = (BELEMENT *)old;
            e2 = (BELEMENT *)(data);
            bjson_reset(record);
            bjson_merge_element(record, e1, e2);
            bjson_finish(record);
            block = record->data;
            nblock = record->current;
            db_free_data(PDB(db), old, nold);
        }
        else
        {
            block = data;
            nblock = ndata;
        }
        if(block && nblock > 0)
        {
            ret = db_set_data(PDB(db), key, block, nblock);
        }
        bjson_clean(record);
    }
    return ret;
}

/* del data */
int xdbase_del_data(XDBASE *xdbase, int diskid, int64_t key)
{
    int ret = -1, id = 0;
    void *db = NULL;

    if(xdbase && xdbase->state && diskid >= 0 && diskid < DBASE_MASK_MAX 
            && xdbase->state->xdisks[diskid].status > 0 
            && (id = xdbase_kid(xdbase, diskid, key)) > 0
            && (db = xdbase->state->xdisks[diskid].db))
    {
        ret = db_del_data(PDB(db), id);
    }
    return ret;
}

/* queue wait */
int xdbase_qwait(XDBASE *xdbase, int diskid, char *chunk, int nchunk)
{
    void *wait = NULL, *db = NULL;
    int id = 0, ret = -1;

    if(xdbase && xdbase->state && diskid >= 0 && diskid < DBASE_MASK_MAX 
            && xdbase->state->xdisks[diskid].status > 0 
            && (wait = xdbase->state->xdisks[diskid].wait)
            && (db = xdbase->state->xdisks[diskid].db))
    {
        if(mmqueue_pop(xdbase->queue, xdbase->state->xdisks[diskid].qleft, &id) < 1)
        {
            MUTEX_LOCK(xdbase->state->xdisks[diskid].mutex);
            id = ++(xdbase->state->xdisks[diskid].qid);
            MUTEX_UNLOCK(xdbase->state->xdisks[diskid].mutex);
        }
        //fprintf(stdout, "%s::%d qid:%d total:%d\n", __FILE__, __LINE__, xdbase->state->xdisks[diskid].qwait, mmqueue_total(xdbase->queue, xdbase->state->xdisks[diskid].qwait));
        if(id > 0 && (ret = db_set_data(PDB(wait), id, chunk, nchunk)) > 0)
        {
            mmqueue_push(xdbase->queue, xdbase->state->xdisks[diskid].qwait, id);
        }
        //fprintf(stdout, "%s::%d ret:%d qid:%d total:%d\n", __FILE__, __LINE__, ret, xdbase->state->xdisks[diskid].qwait, mmqueue_total(xdbase->queue, xdbase->state->xdisks[diskid].qwait));
    }
    return ret;
}

/* queue relay */
int xdbase_qrelay(XDBASE *xdbase, int diskid, int id)
{
    int ret = -1;

    if(xdbase && xdbase->state && diskid >= 0 && diskid < DBASE_MASK_MAX 
            && xdbase->state->xdisks[diskid].status > 0
            && xdbase->state->xdisks[diskid].qrelay > 0) 
    {
        ret = mmqueue_push(xdbase->queue, xdbase->state->xdisks[diskid].qrelay, id);
    }
    return ret;
}

/* work*/
int xdbase_work(XDBASE *xdbase, int diskid)
{
    int id = 0, i = 0, n = 0, nblock = 0, key = 0, ndata = 0, nold = 0;
    char *block = NULL, *data = NULL, *old = NULL, *p = NULL, *end = NULL;
    void *wait = NULL, *db = NULL;
    BELEMENT *e1 = NULL, *e2 = NULL;
    int64_t *keys = NULL;
    BJSON *record = NULL;
    DBHEAD *head = NULL;
    DBMETA *meta = NULL;

    if(xdbase && xdbase->state && diskid >= 0 && diskid < DBASE_MASK_MAX 
            && xdbase->state->xdisks[diskid].status > 0 
            && (record = &(xdbase->state->xdisks[diskid].record))
            && (wait = xdbase->state->xdisks[diskid].wait)
            && (db = xdbase->state->xdisks[diskid].db))
    {
        //fprintf(stdout, "%s::%d qid:%d total:%d\n", __FILE__, __LINE__, xdbase->state->xdisks[diskid].qwait, mmqueue_total(xdbase->queue, xdbase->state->xdisks[diskid].qwait));
        while(mmqueue_pop(xdbase->queue, xdbase->state->xdisks[diskid].qwait, &id) > 0)
        {
            //fprintf(stdout, "%s::%d id:%d qid:%d total:%d\n", __FILE__, __LINE__, id, xdbase->state->xdisks[diskid].qwait, mmqueue_total(xdbase->queue, xdbase->state->xdisks[diskid].qwait));
            if((nblock = db_exists_block(PDB(wait), id, &block)) > 0)
            {
                if((head = (DBHEAD *)block) && (head->length + sizeof(DBHEAD)) == nblock)
                {
                    //fprintf(stdout, "%s::%d qid:%d total:%d cmd:%d\n", __FILE__, __LINE__, xdbase->state->xdisks[diskid].qwait, mmqueue_total(xdbase->queue, xdbase->state->xdisks[diskid].qwait), head->cmd);
                    if(head->cmd == DBASE_REQ_DELETE)
                    {
                        keys = (int64_t *)(block + sizeof(DBHEAD));
                        n = head->length/sizeof(int64_t);
                        for(i = 0; i < n; i++)
                        {
                            if((key = xdbase_key_id(xdbase, diskid, keys[i])) > 0)
                                    db_del_data(PDB(db), key);
                        }
                    }
                    else if(head->cmd == DBASE_REQ_UPBATCH)
                    {
                        p = block + sizeof(DBHEAD);
                        end = block + nblock;
                        while(p < end)    
                        {
                            meta = (DBMETA *)p;
                            p += sizeof(DBMETA);
                            old = NULL;nold = 0;
                            data = NULL; ndata = 0;
                            if(meta->length > 0 
                                    && (key = xdbase_key_id(xdbase, diskid, meta->id)) > 0 
                                    && (nold = db_get_data(PDB(db), meta->id, &old)) && old)
                            {
                                //rec = brecord_merge(oldrec, newrec);
                                e1 = (BELEMENT *)old;
                                e2 = (BELEMENT *)p;
                                bjson_reset(record);
                                bjson_merge_element(record, e1, e2);
                                bjson_finish(record);
                                data = record->data;
                                ndata = record->current;
                                db_free_data(PDB(db), old, nold);
                                if(data && ndata > 0)
                                {
                                    db_set_data(PDB(db), key, data, ndata);
                                }
                            }
                            else if(meta->cmd == DBASE_REQ_DELETE)
                            {
                                if((key = xdbase_key_id(xdbase, diskid, meta->id)) > 0)
                                    db_del_data(PDB(db), key);
                            }
                            p += meta->length;
                        }
                    }
                    else
                    {
                        if((key = xdbase_key_id(xdbase, diskid, head->id)) > 0)
                        {
                            if(head->cmd == DBASE_REQ_UPDATE || head->cmd == DBASE_REQ_APPEND)
                            {
                                old = NULL;nold = 0;
                                data = NULL; ndata = 0;
                                if((nold = db_get_data(PDB(db), key, &old)) && old)
                                {
                                    //rec = brecord_merge(oldrec, newrec);
                                    e1 = (BELEMENT *)old;
                                    e2 = (BELEMENT *)(block + sizeof(DBMETA));
                                    bjson_reset(record);
                                    bjson_merge_element(record, e1, e2);
                                    bjson_finish(record);
                                    data = record->data;
                                    ndata = record->current;
                                    db_free_data(PDB(db), old, nold);
                                }
                                if(data && ndata > 0)
                                {
                                    db_set_data(PDB(db), key, data, ndata);
                                }
                                //bjson_clean(record);
                            }
                            else
                            {
                                db_set_data(PDB(db), key, block + sizeof(DBHEAD), head->length);
                            }
                        }
                    }
                }
                db_del_data(PDB(wait), id);
                mmqueue_push(xdbase->queue, xdbase->state->xdisks[diskid].qleft, id);
            }
            ++n;
        }
    }
    return n;
}


/* get data */
int xdbase_get_data(XDBASE *xdbase, int diskid, int64_t key, char **data)
{
    int ret = -1, id = 0;
    void *db = NULL;

    if(xdbase && xdbase->state && diskid >= 0 && diskid < DBASE_MASK_MAX && data 
            && xdbase->state->xdisks[diskid].status > 0 
            && (id = xdbase_kid(xdbase, diskid, key)) > 0
            && (db = xdbase->state->xdisks[diskid].db))
    {
        ret = db_get_data(PDB(db), id, data);
    }
    return ret;
}

/* free data */
void xdbase_free_data(XDBASE *xdbase, int diskid, char *data, int ndata)
{
    void *db = NULL;

    if(xdbase && xdbase->state && diskid >= 0 && diskid < DBASE_MASK_MAX && data 
            && xdbase->state->xdisks[diskid].status > 0 
            && (db = xdbase->state->xdisks[diskid].db))
    {
        db_free_data(PDB(db), data, (size_t)ndata);
    }
    return;
}

/* get data */
int xdbase_read_data(XDBASE *xdbase, int diskid, int64_t key, char *data)
{
    int ret = -1, id = 0;
    void *db = NULL;

    if(xdbase && xdbase->state && diskid >= 0 && diskid < DBASE_MASK_MAX && data 
            && xdbase->state->xdisks[diskid].status > 0 
            && (id = xdbase_kid(xdbase, diskid, key)) > 0
            && (db = xdbase->state->xdisks[diskid].db))
    {
        ret = db_read_data(PDB(db), id, data);
    }
    return ret;
}

/* get data len */
int xdbase_get_data_len(XDBASE *xdbase, int diskid, int64_t key)
{
    int ret = -1, id = 0;
    void *db = NULL;

    if(xdbase && xdbase->state && diskid >= 0 && diskid < DBASE_MASK_MAX 
            && xdbase->state->xdisks[diskid].status > 0 
            && (id = xdbase_kid(xdbase, diskid, key)) > 0
            && (db = xdbase->state->xdisks[diskid].db))
    {
        ret = db_get_data_len(PDB(db), id);
    }
    return ret;
}

/* check disk with required length */
int xdbase_check_disk(XDBASE *xdbase, int diskid, int64_t key, int length)
{
    struct statvfs fs = {0};
    int ret = -1;

    if(xdbase && xdbase->state && diskid >= 0 && diskid < DBASE_MASK_MAX 
            && xdbase->state->xdisks[diskid].status > 0 
            && statvfs(xdbase->state->xdisks[diskid].disk, &fs) == 0
            && length < (fs.f_bfree * fs.f_bsize - DBASE_LEFT_MAX)) 
    {
        ret = 0;
    }
    return ret;
}

/* xdbase bound */
int xdbase_bound(XDBASE *xdbase, int diskid, int count)
{
    void *db = NULL;
    int n = -1;

    if(xdbase && xdbase->state && diskid >= 0 && diskid < DBASE_MASK_MAX 
            && xdbase->state->xdisks[diskid].status > 0 && count > 0 
            && (db = xdbase->state->xdisks[diskid].db))
    {
        n = PDB(db)->state->data_len_max * count;
    }
    return n;
}

/* close XDB */
void xdbase_close(XDBASE *xdbase)
{
    int i = 0;

    if(xdbase)
    {
        mmtrie_clean(MMTR(xdbase->map));
        mmtrie_clean(MMTR(xdbase->kmap));
        mmtree64_close(xdbase->idmap);
        mmqueue_clean(xdbase->queue);
        MUTEX_DESTROY(xdbase->mutex);
        LOGGER_CLEAN(xdbase->logger);
        if(xdbase->state)
        {
            for(i = 0; i < DBASE_MASK_MAX; i++)
            {
                //db_clean(PDB(xdbase->state->xdisks[i].wait));
                db_clean(PDB(xdbase->state->xdisks[i].db));
                //db_clean(PDB(xdbase->state->xdisks[i].cache));
                bjson_clean(&(xdbase->state->xdisks[i].record));
                MUTEX_DESTROY(xdbase->state->xdisks[i].mutex);
                //xdbase->state->xdisks[i].wait = NULL;
                xdbase->state->xdisks[i].db = NULL;
                //xdbase->state->xdisks[i].cache = NULL;
                xdbase->state->xdisks[i].mutex = NULL;
            }
        }
        if(xdbase->stateio.map)
        {
            munmap(xdbase->stateio.map, sizeof(XDBSTATE));
            xdbase->stateio.map = NULL;
        }
        if(xdbase->stateio.fd > 0)
        {
            close(xdbase->stateio.fd);
            xdbase->stateio.fd = -1;
        }
        xmm_free(xdbase, sizeof(XDBASE));
    }
    return ;
}

#ifdef _DEBUG_XDBASE
int main()
{
    XDBASE *xdb = NULL;
    char buf[65536];
    int n = 0, id = 0;

    if((xdb = xdbase_init("/tmp/xdbase", 0)))
    {
        //add disk mask:235.8.8.1/17303787 
        id = xdbase_add_disk(xdb, 6001, 1024 * 1024 * 1024, 0, "/tmp/db1", 17303787);
        id = xdbase_add_disk(xdb, 6002, 1024 * 1024 * 1024, 0, "/tmp/db2", 17303787);
        id = xdbase_add_disk(xdb, 6003, 1024 * 1024 * 1024, 0, "/tmp/db3", 17303787);
        id = xdbase_add_disk(xdb, 6004, 1024 * 1024 * 1024, 0, "/tmp/db4", 17303787);
        if((n = xdbase_list_disks(xdb, buf)) > 0) fprintf(stdout, "list:%s\n", buf);
        xdbase_del_disk(xdb, id);
        if((n = xdbase_list_disks(xdb, buf)) > 0) fprintf(stdout, "list:%s\n", buf);
        xdbase_close(xdb);
    }
}
//gcc -o xdb xdbase.c utils/xmm.c utils/mmtrie.c utils/db.c -I utils -D_DEBUG_XDBASE && ./xdb
#endif
