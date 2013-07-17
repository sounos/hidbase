#ifndef __IDBASE__H__
#define __IDBASE__H__
#ifdef HAVE_PTHREAD
#include <pthread.h>
#endif
#define IDB_PATH_MAX   256
#define IDB_FIELDS_MAX 256
#define IDB_HASH_MAX   512
#define IDB_MUTEX_MAX  8192
#define IDB_INCRE_NUM  1000000
#define IDB_NODE_MAX   2000000000
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
typedef struct _QSET
{
    int          roots[IDB_HASH_MAX];
    int          fd;
    int          max;
    unsigned int *map;
    off_t        end;
    off_t        size;
#ifdef HAVE_PTHREAD
    pthread_mutex_t mutex;
#endif
}QSET;
typedef struct _QSTATE
{
    int db_id_max;
    int kid_max;
    int roots[IDB_HASH_MAX];
    QSET m32[IDB_FIELDS_MAX];
    QSET m64[IDB_FIELDS_MAX];
    QSET m96[IDB_FIELDS_MAX];
}QSTATE;
typedef struct _IDBASE
{
    char basedir[IDB_PATH_MAX];
    RWIO stateio;
    QSTATE *state;
    void *logger;
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
