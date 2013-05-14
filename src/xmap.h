#include "dbase.h"
#ifndef __XMAP__H__
#define __XMAP__H__
#define XM_HASH_SIZE    64
#define XM_HOST_MAX     3
#define XM_PATH_MAX     256
#define XM_META_MAX     2000000000
#define XM_META_BASE    1000000
#define XM_META_INCRE   1000000
#define XM_STATUS_FREE  0x00
#define XM_STATUS_WAIT  0x01

typedef struct _XMHOST
{
    int ip;
    short port;
    short version;
}XMHOST;
typedef struct _XMMETA
{
    short status;
    short count;
    time_t modtime;
    XMHOST hosts[XM_HOST_MAX];
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
    int qwait;
    int id_wait;
    int qleft;
    int bits;
    XMASK masks[DBASE_MASK];
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
    XMIO metaio;
    XMSTATE *state;
    XMMETA *metas;
    void *mutex;
    void *cmutex;
    void *tree64;
    void *tree;
    void *mtrie;
    void *logger;
    void *db;
    void *queue;
    time_t start_time;
}XMAP;

/* initialize XMAP */
XMAP *xmap_init(char *basedir);
/* query id */
int xmap_qid(XMAP *xmap, int64_t id, int *status, XMHOST *xhost);
/* status */
int xmap_check(XMAP *xmap, int qid, XMHOST *xhost);
/* query over */
int xmap_over(XMAP *xmap, int qid, XMHOST *xhost);
/* check host exists */
int xmap_check_host(XMAP *xmap, char *ip, int port);
/* add host */
int xmap_add_host(XMAP *xmap, char *ip, int port, int val);
/* cache data */
int xmap_cache(XMAP *xmap, char *data, int ndata);
/* cache data size */
int xmap_cache_len(XMAP *xmap, int id);
/* read cache */
int xmap_read_cache(XMAP *xmap, int id, char *data);
/* drop cache */
int xmap_drop_cache(XMAP *xmap, int id);
/* xmap clean */
void xmap_clean(XMAP *xmap);
#endif
