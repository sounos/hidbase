#ifndef _MMKEY_H_
#define _MMKEY_H_
#include "mutex.h"
#ifdef __cplusplus
extern "C" {
#endif
#define MMKEY_PATH_MAX             256
#define MMKEY_LINE_MAX             65536
#define MMKEY_INCREMENT_NUM        10000000
#define MMKEY_NODES_MAX            2000000000
#define MMKEY_WORD_MAX             4096
typedef struct _MKEYLIST
{
    unsigned int count;
    unsigned int head;
}MKEYLIST;
/* trie node */
typedef struct _MKEYNODE
{
    unsigned short key;
    unsigned short nchilds;
    unsigned int data;
    unsigned int childs;
}MKEYNODE;
/* state */
typedef struct _MKEYSTATE
{
    unsigned int id;
    unsigned int current;
    unsigned int total;
    unsigned int left;
    MKEYLIST list[MMKEY_LINE_MAX];
}MKEYSTATE;
/* MEM trie */
typedef struct _MMKEY
{
    MKEYSTATE   *state;
    MKEYNODE     *nodes;
    void        *map;
    off_t       size;
    off_t       file_size;
    int         fd;
    MUTEX mutex;

    int  (*add)(struct _MMKEY *, unsigned short *key, int num, int data);
    int  (*xadd)(struct _MMKEY *, unsigned short *key, int num);
    int  (*get)(struct _MMKEY *, unsigned short *key, int num);
    int  (*del)(struct _MMKEY *, unsigned short *key, int num);
    int  (*find)(struct _MMKEY *, unsigned short *key, int num, int *len);
    int  (*maxfind)(struct _MMKEY *, unsigned short *key, int num, int *len);
    int  (*radd)(struct _MMKEY *, unsigned short *key, int num, int data);
    int  (*rxadd)(struct _MMKEY *, unsigned short *key, int num);
    int  (*rget)(struct _MMKEY *, unsigned short *key, int num);
    int  (*rdel)(struct _MMKEY *, unsigned short *key, int num);
    int  (*rfind)(struct _MMKEY *, unsigned short *key, int num, int *len);
    int  (*rmaxfind)(struct _MMKEY *, unsigned short *key, int num, int *len);
    void (*clean)(struct _MMKEY *);
}MMKEY;
/* initialize */
MMKEY   *mmkey_init(char *file);
/* add */
int   mmkey_add(struct _MMKEY *, unsigned short *key, int num, int data);
/* add return auto_increment_id */
int   mmkey_xadd(struct _MMKEY *, unsigned short *key, int num);
/* get */
int   mmkey_get(struct _MMKEY *, unsigned short *key, int num);
/* delete */
int   mmkey_del(struct _MMKEY *, unsigned short *key, int num);
/* find/min */
int   mmkey_find(struct _MMKEY *, unsigned short *key, int num, int *len);
/* find/max */
int   mmkey_maxfind(struct _MMKEY *, unsigned short *key, int num, int *len);
/* add/reverse */
int   mmkey_radd(struct _MMKEY *, unsigned short *key, int num, int data);
/* add/reverse return auto_increment_id */
int   mmkey_rxadd(struct _MMKEY *, unsigned short *key, int num);
/* get/reverse */
int   mmkey_rget(struct _MMKEY *, unsigned short *key, int num);
/* del/reverse */
int   mmkey_rdel(struct _MMKEY *, unsigned short *key, int num);
/* find/min/reverse */
int   mmkey_rfind(struct _MMKEY *, unsigned short *key, int num, int *len);
/* find/max/reverse */
int   mmkey_rmaxfind(struct _MMKEY *, unsigned short *key, int num, int *len);
/* destroy */
void mmkey_destroy(struct _MMKEY *);
/* clean/reverse */
void  mmkey_clean(struct _MMKEY *);
#define MKEY(x) ((MMKEY *)x)
#ifdef __cplusplus
     }
#endif
#endif
