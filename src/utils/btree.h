#include <stdio.h>
#ifndef __BTREE_H__
#define __BTREE_H__
#define BT_CHILDS_MAX  256
#define BT_CHILDS_HALF 128
#define BT_LEFT_MAX    1024
#define BT_NODE_MAX    4000000000
#define BT_NODE_INCRE  10000
typedef struct _BTNODE
{
    ushort nkeys;
    ushort nchilds;
    unsigned pranet;
    unsigned int childs[BT_CHILDS_MAX+1];
    int32_t keys[BT_CHILDS_MAX];
}BTNODE;
typedef struct _BTROOT
{
    unsigned int id_max;
    unsigned int rootid;
}BTROOT;
typedef struct _BTSTATE
{
    BTROOT   roots[BT_CHILDS_MAX];
    unsigned int qwait[BT_LEFT_MAX];
    unsigned int nqwait;
    unsigned int current;
    unsigned int left;
}BTSTATE;
typedef struct _BTIO
{
    int fd;
    int status;
    char *map;
    off_t old;
    off_t end;
    off_t size;
}BTIO;
typedef struct _BTREE
{
    BTIO btio;
    BTSTATE *state;
    BTNODE *nodes;
    void *mutex;
    void *timer;
}BTREE;
#endif
