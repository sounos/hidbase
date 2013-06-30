#ifndef _XDBASE_H_
#define _XDBASE_H_
#include "mutex.h"
#include "dbase.h"
#define XDB_PATH_MAX        256
#define XDB_BUF_SIZE        1024
#define XDB_KEYS_MAX        256
#define XDB_MODE_MMAP       0x01
typedef struct _XMASK
{
    int root;
    int mask_ip; 
    int total;
}XMASK;
typedef struct _XDISK
{
    ushort     status;
    ushort     nmasks;
    ushort     port;
    ushort     mode;
    uint32_t   total;
    uint32_t   modtime;
    uint32_t   qwait;
    uint32_t   qleft;
    uint32_t   qrelay;
    uint32_t   qid;
    BJSON      record;
    void       *db;
    void       *wait;
    void       *cache;
    MUTEX      *mutex;
    uint64_t   limit;
    char       disk[XDB_PATH_MAX];
    XMASK      masks[DBASE_MASK_MAX];
}XDISK;

typedef struct _XDBSTATE
{
    int     data_len_max;
    int     nxdisks;
    int     id_max;
    int     rootid;
    int     mode;
    int     bits;
    off_t   op_total;
    off_t   op_slow;
    XDISK   xdisks[DBASE_MASK_MAX];
}XDBSTATE;
typedef struct _XDBIO
{
    int     fd;
    int     status;
    void    *map;
    off_t   end;
    off_t   size;
}XDBIO;
typedef struct _XDBASE
{
    XDBIO stateio;
    MUTEX  *mutex;
    XDBSTATE *state;
    void *map;
    void *kmap;
    void *idmap;
    void *logger;
    void *queue;
}XDBASE;
/* initialize XDB */
XDBASE *xdbase_init(char *path, int mode);
/* add disk */
//int xdbase_add_disk(XDBASE *xdbase, int port, off_t limit, int mode, char *disk, int mask);
int xdbase_add_disk(XDBASE *xdbase, int port, off_t limit, int mode, char *disk);
/* update disk limit */
int xdbase_set_disk_limit(XDBASE *xdbase, int diskid, off_t limit);
/* set disk mmap mpde */
int xdbase_set_disk_mode(XDBASE *xdbase, int diskid, int mode);
/* del disk */
int xdbase_del_disk(XDBASE *xdbase, int diskid);
/* add mask */
int xdbase_add_mask(XDBASE *xdbase, int diskid, int mask);
/* del mask */
int xdbase_del_mask(XDBASE *xdbase, int diskid, int mask);
/* list diak */
int xdbase_list_disks(XDBASE *xdbase, char *out);
/* add data */
int xdbase_add_data(XDBASE *xdbase, int diskid, int64_t id, char *data, int ndata);
/* update data */
int xdbase_set_data(XDBASE *xdbase, int diskid, int64_t id, char *data, int ndata);
/* update data */
int xdbase_update_data(XDBASE *xdbase, int diskid, int64_t key, char *data, int ndata);
/* del data */
int xdbase_del_data(XDBASE *xdbase, int diskid, int64_t id);
/* read data */
int xdbase_read_data(XDBASE *xdbase, int diskid, int64_t id, char *data);
/* get data */
int xdbase_get_data(XDBASE *xdbase, int diskid, int64_t id, char **data);
/* free data */
void xdbase_free_data(XDBASE *xdbase, int diskid, char *data, int ndata);
/* get data len*/
int xdbase_get_data_len(XDBASE *xdbase, int diskid, int64_t id);
/* check disk with required length */
int xdbase_check_disk(XDBASE *xdbase, int diskid, int64_t key, int length);
/* xdbase bound */
int xdbase_bound(XDBASE *xdbase, int diskid, int count);
/* queue wait */
int xdbase_qwait(XDBASE *xdbase, int diskid, char *chunk, int nchunk);
/* work*/
int xdbase_work(XDBASE *xdbase, int diskid);
/* close XDB */
void xdbase_close(XDBASE *xdbase);
#endif
