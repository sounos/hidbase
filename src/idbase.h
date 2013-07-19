#ifndef __IDBASE__H__
#define __IDBASE__H__
#ifdef HAVE_PTHREAD
#include <pthread.h>
#endif
#define IDB_PATH_MAX   256
#define IDB_FIELDS_MAX 32
#define IDB_HASH_MAX   1024
#define IDB_MUTEX_MAX  8192
#define IDB_BASE_NUM   100000
#define IDB_NODE_MAX   2000000000
#define IDB_HMAP_MAX   20000
#define IDB_TAB_MAX    256
#define IDB_TAB_NUM    256
typedef struct _MRECORD
{
    int     bits;
    int     m32_num;
    int     m64_num;
    int     m96_num;
    int32_t m32[IDB_FIELDS_MAX];
    int64_t m64[IDB_FIELDS_MAX];
    double  m96[IDB_FIELDS_MAX];
}MRECORD;
typedef struct _RWIO
{
    int     fd;
    int     status;
    void    *map;
    off_t   end;
    off_t   size;
}RWIO;
typedef struct _QNODE
{
    unsigned int base;
    int num;
    int32_t  min;
    int32_t  max;
}QNODE;
typedef struct _QV32
{
    unsigned int id;
    int32_t  val;
}QV32;
typedef struct _QTAB
{
    int num;
    QNODE table[IDB_FIELDS_MAX][IDB_TAB_MAX][IDB_TAB_MAX];
}QTAB;
typedef struct _QSET
{
    unsigned int  hmap[IDB_HMAP_MAX];        
    int  hmap_max;
    int  rootid;
    int  max;
    int  num;
#ifdef HAVE_PTHREAD
    pthread_mutex_t mutex;
#endif
}QSET;
typedef struct _QSTATE
{
    int db_id_max;
    int kid_max;
    off_t time_used;
    int roots[IDB_HASH_MAX];
    QSET m32[IDB_FIELDS_MAX];
    QSET m64[IDB_FIELDS_MAX];
    QSET m96[IDB_FIELDS_MAX];
}QSTATE;
typedef struct _IDBASE
{
    RWIO stateio;
    RWIO m32io;
    RWIO tab32io;
    QSTATE *state;
    void *logger;
    void *timer;
    unsigned int *m32;
    unsigned int *m64;
    unsigned int *m96;
    QTAB *tab32;
    QV32 *v32;
    void *map;
    void *mm32;
    void *mm64;
    void *mm96;
    void *mdb;
    void *mutex;
#ifdef HAVE_PTHREAD
    pthread_mutex_t mutexs[IDB_MUTEX_MAX];
#endif
}IDBASE;
IDBASE *idbase_init(char *basedir);
void idbase_close(IDBASE *idb);
#endif
