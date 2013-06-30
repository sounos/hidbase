#include "dbase.h"
#ifndef __XMAP__H__
#define __XMAP__H__
#define XM_HASH_SIZE    64
#define XM_HOST_MAX     3
#define XM_PATH_MAX     256
#define XM_META_MAX     2000000000
#define XM_META_BASE    1000000
#define XM_META_INCRE   1000000
#define XM_DISK_MAX     2000000
#define XM_DISK_BASE    1000
#define XM_DISK_INCRE   1000
#define XM_STATUS_FREE  0x00
#define XM_STATUS_WAIT  0x01
#define XM_NO_GROUPID   -2
typedef struct _XMHOST
{
    int ip;
    int diskid;
    short port;
    short gid;
}XMHOST;
typedef struct _XMSETS
{
    XMHOST lists[XM_HOST_MAX];
}XMSETS;
typedef struct _XMMETA
{
    short    status;
    short    count;
    uint32_t modtime;
    uint32_t disks[XM_HOST_MAX];
}XMMETA;
typedef struct _XMASK
{
    int total;
    int root;
}XMASK;
typedef struct _XMSTATE
{
    int status;
    int id_max;
    int disk_id_max;
    int qwait;
    int id_wait;
    int qleft;
    XMASK masks[DBASE_MASK_MAX];
}XMSTATE;
typedef struct _XMIO
{
    int     fd;
    int     status;
    void    *map;
    off_t   old;
    off_t   end;
    off_t   size;
}XMIO;
typedef struct _XMAP
{
    XMIO stateio;
    XMIO diskio;
    XMIO metaio;
    XMSTATE *state;
    XMMETA *metas;
    MDISK *disks;
    void *mutex;
    void *cmutex;
    void *tree64;
    void *tree;
    void *kmap;
    void *mtrie;
    void *logger;
    void *db;
    void *queue;
    time_t start_time;
}XMAP;

/* initialize XMAP */
XMAP *xmap_init(char *basedir);
/* query id */
int xmap_qid(XMAP *xmap, int64_t id, int *status, XMSETS *sets, int *nsets);
/* status */
int xmap_check_meta(XMAP *xmap, int qid, int *status, XMSETS *sets);
/* reset */
int xmap_reset_meta(XMAP *xmap, int qid);
/* query over */
int xmap_over_meta(XMAP *xmap, int qid, int diskid, int *status);
/* return diskid */
int xmap_set_disk(XMAP *xmap, MDISK *disk);
/* set groupid */
int xmap_set_groupid(XMAP *xmap, int diskid, int groupid);
/* get diskid */
int xmap_diskid(XMAP *xmap, char *ip, int port, int *groupid);
/* lisk all disks */
int xmap_list_disks(XMAP *xmap, char *out);
/* truncate block */
int xmap_truncate_block(XMAP *xmap, int len, char **block);
/* get cache info*/
int xmap_cache_info(XMAP *xmap, int id, char **block);
/* cache data */
int xmap_cache(XMAP *xmap, char *data, int ndata);
/* cache data size */
int xmap_cache_len(XMAP *xmap, int id);
/* read cache */
int xmap_read_cache(XMAP *xmap, int id, char *data);
/* drop cache */
int xmap_drop_cache(XMAP *xmap, int id);
/* new task */
int xmap_new_task(XMAP *xmap, int id);
/* over task */
int xmap_over_task(XMAP *xmap, int id);
/* xmap clean */
void xmap_clean(XMAP *xmap);
#endif
