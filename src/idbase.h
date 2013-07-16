#ifndef __IDBASE__H__
#define __IDBASE__H__
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
typedef struct _IDBASE
{
    void *map;
    void *mm32;
    void *mm64;
    void *mm96;
    void *m32;
    void *m64;
    void *m96;
    void *mutex;
}IDBASE;
IDBASE *idbase_init(char *basedir);
int idbase_update(IDBASE *idb, int64_t key);
int idbase_del(IDBASE *idb, int64_t key);
int idbase_query(IDBASE *idb);
void idbase_close(IDBASE *idb);
#endif
