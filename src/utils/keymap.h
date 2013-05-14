#ifndef _KEYMAP_H_
#define _KEYMAP_H_
#include "mutex.h"
#ifdef __cplusplus
extern "C" {
#endif
#define KEYMAP_PATH_MAX             256
#define KEYMAP_LINE_MAX             65536
#define KEYMAP_INCREMENT_NUM        5000000
#define KEYMAP_WORD_MAX             4096
typedef struct _MKEYLIST
{
    int count;
    int head;
}MKEYLIST;
/* trie node */
typedef struct _MKEYNODE
{
    unsigned short key;
    unsigned short nchilds;
    int data;
    int childs;
}MKEYNODE;
/* state */
typedef struct _MKEYSTATE
{
    int id;
    int current;
    int total;
    int left;
    MKEYLIST list[KEYMAP_LINE_MAX];
}MKEYSTATE;
/* MEM trie */
typedef struct _KEYMAP
{
    MKEYSTATE   *state;
    MKEYNODE     *nodes;
    void        *map;
    off_t       file_size;
    int         fd;
    MUTEX mutex;

    int  (*add)(struct _KEYMAP *, char *key, int nkey, int data);
    int  (*xadd)(struct _KEYMAP *, char *key, int nkey);
    int  (*get)(struct _KEYMAP *, char *key, int nkey);
    int  (*del)(struct _KEYMAP *, char *key, int nkey);
    int  (*find)(struct _KEYMAP *, char *key, int nkey, int *len);
    int  (*maxfind)(struct _KEYMAP *, char *key, int nkey, int *len);
    int  (*radd)(struct _KEYMAP *, char *key, int nkey, int data);
    int  (*rxadd)(struct _KEYMAP *, char *key, int nkey);
    int  (*rget)(struct _KEYMAP *, char *key, int nkey);
    int  (*rdel)(struct _KEYMAP *, char *key, int nkey);
    int  (*rfind)(struct _KEYMAP *, char *key, int nkey, int *len);
    int  (*rmaxfind)(struct _KEYMAP *, char *key, int nkey, int *len);
    int  (*import)(struct _KEYMAP *, char *dictfile, int direction);
    void (*clean)(struct _KEYMAP *);
}KEYMAP;
/* initialize */
KEYMAP   *keymap_init(char *file);
/* add */
int   keymap_add(struct _KEYMAP *, char *key, int nkey, int data);
/* add return auto_increment_id */
int   keymap_xadd(struct _KEYMAP *, char *key, int nkey);
/* get */
int   keymap_get(struct _KEYMAP *, char *key, int nkey);
/* delete */
int   keymap_del(struct _KEYMAP *, char *key, int nkey);
/* find/min */
int   keymap_find(struct _KEYMAP *, char *key, int nkey, int *len);
/* find/max */
int   keymap_maxfind(struct _KEYMAP *, char *key, int nkey, int *len);
/* add/reverse */
int   keymap_radd(struct _KEYMAP *, char *key, int nkey, int data);
/* add/reverse return auto_increment_id */
int   keymap_rxadd(struct _KEYMAP *, char *key, int nkey);
/* get/reverse */
int   keymap_rget(struct _KEYMAP *, char *key, int nkey);
/* del/reverse */
int   keymap_rdel(struct _KEYMAP *, char *key, int nkey);
/* find/min/reverse */
int   keymap_rfind(struct _KEYMAP *, char *key, int nkey, int *len);
/* find/max/reverse */
int   keymap_rmaxfind(struct _KEYMAP *, char *key, int nkey, int *len);
/* import dict if direction value is -1, add word reverse */
int   keymap_import(struct _KEYMAP *, char *dictfile, int direction);
/* destroy */
void keymap_destroy(struct _KEYMAP *);
/* clean/reverse */
void  keymap_clean(struct _KEYMAP *);
#define MKEY(x) ((KEYMAP *)x)
#ifdef __cplusplus
     }
#endif
#endif
