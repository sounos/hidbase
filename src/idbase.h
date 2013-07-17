#ifndef __IDBASE__H__
#define __IDBASE__H__
#ifdef HAVE_PTHREAD
#include <pthread.h>
#endif
#define IDB_FIELDS_MAX 256
#define IDB_MUTEX_MAX  8192
typedef struct _M32
{
    int32_t val;
    unsigned int nodeid;
}M32;
typedef struct M64
{
    int64_t val;
    unsigned int nodeid;
}M64;
typedef struct _M96
{
    double val;
    unsigned int nodeid;
}M96;
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
    int rootid;
    int dbid;
    int id_max;
#ifdef HAVE_PTHREAD
    pthread_mutex_t mutex;
#endif
}QSET;
typedef struct _QSTATE
{
    int rootid;
    int id_max;
    QSET m32[IDB_FIELDS_MAX];
    QSET m64[IDB_FIELDS_MAX];
    QSET m96[IDB_FIELDS_MAX];
}QSTATE;
typedef struct _IDBASE
{
    RWIO stateio;
    QSTATE *state;
    void *logger;
    void *map;
    void *mm32;
    void *mm64;
    void *mm96;
    void *mutex;
#ifdef HAVE_PTHREAD
    pthread_mutex_t mutexs[IDB_MUTEX_MAX];
#endif
}IDBASE;
IDBASE *idbase_init(char *basedir);
int idbase_update(IDBASE *idb, int64_t key);
int idbase_del(IDBASE *idb, int64_t key);
int idbase_query(IDBASE *idb);
void idbase_close(IDBASE *idb);
#endif
