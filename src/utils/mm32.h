#ifndef _MM32_H
#define _MM32_H
#include "mutex.h"
#define MM32_INCRE_NUM    100000
#define MM32_NODES_MAX    2000000000
#define MM32_ROOT_MAX     262144
typedef struct _MM32NODE
{
    unsigned int left;
    unsigned int right;
    unsigned int parent;
    unsigned int color;
    int32_t key;
    int data;
}MM32NODE;
typedef struct _MM32ROOT
{
    int status;
    int bits;
    unsigned int total;
    unsigned int rootid;
}MM32ROOT;
typedef struct _MM32STATE
{
    int kmax;
    int kmin;
    unsigned int nmax;
    unsigned int nmin;
    unsigned int count;
    unsigned int left;
    unsigned int current;
    unsigned int total;
    unsigned int qleft;
    unsigned int qfirst;
    unsigned int qlast;
    unsigned int nroots;
    MM32ROOT roots[MM32_ROOT_MAX];
}MM32STATE;
typedef struct _MM32
{
    int fd;
    int status;
    off_t size;
    off_t end;
    off_t old;
    void    *start;
    MM32STATE *state;
    MM32NODE  *map;
    MUTEX   *mutex;
}MM32;
void *mm32_init(char *file);
int mm32_new_tree(void *mm32);
unsigned int mm32_total(void *mm32, int rootid);
unsigned int mm32_try_insert(void *mm32, int rootid, int key, int data, int *old);
unsigned int mm32_insert(void *mm32, int rootid, int key, int data, int *old);
unsigned int mm32_build(void *mm32, int rootid, int key, int data);
unsigned int mm32_rebuild(void *mm32, int rootid, unsigned int nodeid, int key);
unsigned int mm32_get(void *mm32, unsigned int nodeid, int *key, int *data);
unsigned int mm32_find(void *mm32, int rootid, int key, int *data);
unsigned int mm32_find_gt(void *mm32, int rootid, int key, int *data);
unsigned int mm32_find_gt2(void *mm32, int rootid, int key, int *data);
unsigned int mm32_find_lt(void *mm32, int rootid, int key, int *data);
unsigned int mm32_find_gt2(void *mm32, int rootid, int key, int *data);
unsigned int mm32_min(void *mm32, int rootid, int *key, int *data);
unsigned int mm32_max(void *mm32, int rootid, int *key, int *data);
unsigned int mm32_next(void *mm32, int rootid, unsigned int nodeid, int *key, int *data);
unsigned int mm32_prev(void *mm32, int rootid, unsigned int nodeid, int *key, int *data);
int mm32_set_data(void *mm32, unsigned int nodeid, int data);
void mm32_view_tree(void *mm32, int rootid, FILE *fp);
void mm32_remove(void *mm32, int rootid, unsigned int nodeid, int *key, int *data);
void mm32_remove_tree(void *mm32, int rootid);
void mm32_close(void *mm32);
#endif
