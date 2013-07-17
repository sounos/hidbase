#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include "mm32.h"
#include "mutex.h"
#ifdef MAP_LOCKED
#define MMAP_SHARED MAP_SHARED|MAP_LOCKED
#else
#define MMAP_SHARED MAP_SHARED
#endif
#define MM32(px) ((MM32 *)px)
#define MM32_COLOR_BLACK  0
#define MM32_COLOR_RED    1
#define MM32_MIN_MAX(x, key, xid)                                                   \
do                                                                                  \
{                                                                                   \
    if(MM32(x) && MM32(x)->state)                                                   \
    {                                                                               \
        if(MM32(x)->state->count == 0)                                              \
        {                                                                           \
            MM32(x)->state->nmin = MM32(x)->state->nmax = xid;                      \
            MM32(x)->state->kmin = MM32(x)->state->kmax = key;                      \
        }                                                                           \
        else if(key > MM32(x)->state->kmax)                                         \
        {                                                                           \
            MM32(x)->state->nmax = xid;                                             \
            MM32(x)->state->kmax = key;                                             \
        }                                                                           \
        else if(key < MM32(x)->state->kmin)                                         \
        {                                                                           \
            MM32(x)->state->nmin = xid;                                             \
            MM32(x)->state->kmin = key;                                             \
        }                                                                           \
    }                                                                               \
}while(0)
#define MM32_MUNMAP(x)                                                              \
do                                                                                  \
{                                                                                   \
    if(x && MM32(x)->size > 0)                                                      \
    {                                                                               \
        if(MM32(x)->start && MM32(x)->start != (void *)-1)                          \
        {                                                                           \
            munmap(MM32(x)->start, MM32(x)->size);                                  \
            MM32(x)->start = NULL;                                                  \
            MM32(x)->state = NULL;                                                  \
            MM32(x)->map = NULL;                                                    \
        }                                                                           \
    }                                                                               \
}while(0)

#define MM32_MMAP(x)                                                                \
do                                                                                  \
{                                                                                   \
    if(x)                                                                           \
    {                                                                               \
        if((MM32(x)->start = (char*)mmap(NULL,MM32(x)->size,PROT_READ|PROT_WRITE,   \
                    MAP_SHARED, MM32(x)->fd, 0)) != (void *)-1)                     \
        {                                                                           \
            MM32(x)->state = (MM32STATE *)MM32(x)->start;                           \
            MM32(x)->map = (MM32NODE *)(MM32(x)->start + sizeof(MM32STATE));        \
        }                                                                           \
    }                                                                               \
}while(0)

#define MM32_INCRE(x)                                                               \
do                                                                                  \
{                                                                                   \
    if(x &&  MM32(x)->end <  MM32(x)->size)                                         \
    {                                                                               \
        MM32(x)->old += MM32(x)->end ;                                              \
        MM32(x)->end += (off_t)MM32_INCRE_NUM * (off_t)sizeof(MM32NODE);            \
        if(ftruncate(MM32(x)->fd, MM32(x)->end) == 0)                               \
        {                                                                           \
            if(MM32(x)->old == sizeof(MM32STATE))                                   \
            {                                                                       \
                memset(MM32(x)->state, 0, sizeof(MM32STATE));                       \
                MM32(x)->state->left += MM32_INCRE_NUM - 1;                         \
            }                                                                       \
            else MM32(x)->state->left += MM32_INCRE_NUM;                            \
            MM32(x)->state->total += MM32_INCRE_NUM;                                \
            memset(MM32(x)->start + MM32(x)->old, 0, MM32(x)->end - MM32(x)->old);  \
        }                                                                           \
        else                                                                        \
        {                                                                           \
            _exit(-1);                                                              \
        }                                                                           \
    }                                                                               \
}while(0)
#define MM32_ROTATE_LEFT(x, prootid, oid, rid, lid, ppid)                           \
do                                                                                  \
{                                                                                   \
    if(x && (rid = MM32(x)->map[oid].right) > 0)                                    \
    {                                                                               \
        if((lid = MM32(x)->map[oid].right = MM32(x)->map[rid].left) > 0)            \
        {                                                                           \
            MM32(x)->map[lid].parent = oid;                                         \
        }                                                                           \
        if((ppid = MM32(x)->map[rid].parent = MM32(x)->map[oid].parent) > 0)        \
        {                                                                           \
            if(MM32(x)->map[ppid].left == oid)                                      \
                MM32(x)->map[ppid].left = rid;                                      \
            else                                                                    \
                MM32(x)->map[ppid].right = rid;                                     \
        }else *prootid = rid;                                                       \
        MM32(x)->map[rid].left = oid;                                               \
        MM32(x)->map[oid].parent = rid;                                             \
    }                                                                               \
}while(0)

#define MM32_ROTATE_RIGHT(x, prootid, oid, lid, rid, ppid)                          \
do                                                                                  \
{                                                                                   \
    if(x && (lid = MM32(x)->map[oid].left) > 0)                                     \
    {                                                                               \
        if((rid = MM32(x)->map[oid].left = MM32(x)->map[lid].right) > 0)            \
        {                                                                           \
            MM32(x)->map[rid].parent = oid;                                         \
        }                                                                           \
        if((ppid = MM32(x)->map[lid].parent = MM32(x)->map[oid].parent) > 0)        \
        {                                                                           \
            if(MM32(x)->map[ppid].left == oid)                                      \
                MM32(x)->map[ppid].left = lid;                                      \
            else                                                                    \
                MM32(x)->map[ppid].right = lid;                                     \
        }                                                                           \
        else *prootid = lid;                                                        \
        MM32(x)->map[lid].right = oid;                                              \
        MM32(x)->map[oid].parent = lid;                                             \
    }                                                                               \
}while(0)

#define MM32_INSERT_COLOR(x, prootid, oid, lid, rid, uid, pid, gpid, ppid)          \
do                                                                                  \
{                                                                                   \
    while((pid = MM32(x)->map[oid].parent)> 0                                       \
            && MM32(x)->map[pid].color == MM32_COLOR_RED)                           \
    {                                                                               \
        gpid = MM32(x)->map[pid].parent;                                            \
        if(pid == MM32(x)->map[gpid].left)                                          \
        {                                                                           \
            uid = MM32(x)->map[gpid].right;                                         \
            if(uid > 0 && MM32(x)->map[uid].color == MM32_COLOR_RED)                \
            {                                                                       \
                MM32(x)->map[uid].color = MM32_COLOR_BLACK;                         \
                MM32(x)->map[pid].color = MM32_COLOR_BLACK;                         \
                MM32(x)->map[gpid].color = MM32_COLOR_RED;                          \
                oid = gpid;                                                         \
                continue;                                                           \
            }                                                                       \
            if(MM32(x)->map[pid].right == oid)                                      \
            {                                                                       \
                MM32_ROTATE_LEFT(x, prootid, pid, rid, lid, ppid);                  \
                uid = pid; pid = oid; oid = uid;                                    \
            }                                                                       \
            MM32(x)->map[pid].color = MM32_COLOR_BLACK;                             \
            MM32(x)->map[gpid].color = MM32_COLOR_RED;                              \
            MM32_ROTATE_RIGHT(x, prootid, gpid, lid, rid, ppid);                    \
        }                                                                           \
        else                                                                        \
        {                                                                           \
            uid = MM32(x)->map[gpid].left;                                          \
            if(uid > 0 && MM32(x)->map[uid].color == MM32_COLOR_RED)                \
            {                                                                       \
                MM32(x)->map[uid].color = MM32_COLOR_BLACK;                         \
                MM32(x)->map[pid].color = MM32_COLOR_BLACK;                         \
                MM32(x)->map[gpid].color = MM32_COLOR_RED;                          \
                oid = gpid;                                                         \
                continue;                                                           \
            }                                                                       \
            if(MM32(x)->map[pid].left == oid)                                       \
            {                                                                       \
                MM32_ROTATE_RIGHT(x, prootid, pid, lid, rid, ppid);                 \
                uid = pid; pid = oid; oid = uid;                                    \
            }                                                                       \
            MM32(x)->map[pid].color = MM32_COLOR_BLACK;                             \
            MM32(x)->map[gpid].color = MM32_COLOR_RED;                              \
            MM32_ROTATE_LEFT(x, prootid, gpid, rid, lid, ppid);                     \
        }                                                                           \
    }                                                                               \
    if(*prootid > 0)MM32(x)->map[*prootid].color = MM32_COLOR_BLACK;                \
}while(0)

#define MM32_REMOVE_COLOR(x, prootid, oid, xpid, lid, rid, uid, ppid)               \
do                                                                                  \
{                                                                                   \
    while((oid == 0 || MM32(x)->map[oid].color == MM32_COLOR_BLACK)                 \
            && oid != *prootid)                                                     \
    {                                                                               \
        if(MM32(x)->map[xpid].left == oid)                                          \
        {                                                                           \
            uid = MM32(x)->map[xpid].right;                                         \
            if(MM32(x)->map[uid].color == MM32_COLOR_RED)                           \
            {                                                                       \
                MM32(x)->map[uid].color = MM32_COLOR_BLACK;                         \
                MM32(x)->map[xpid].color = MM32_COLOR_RED;                          \
                MM32_ROTATE_LEFT(x, prootid, xpid, rid, lid, ppid);                 \
                uid = MM32(x)->map[xpid].right;                                     \
            }                                                                       \
            lid = MM32(x)->map[uid].left;                                           \
            rid = MM32(x)->map[uid].right;                                          \
            if((lid == 0 || MM32(x)->map[lid].color == MM32_COLOR_BLACK)            \
                && (rid == 0 || MM32(x)->map[rid].color == MM32_COLOR_BLACK))       \
            {                                                                       \
                MM32(x)->map[uid].color = MM32_COLOR_RED;                           \
                oid = xpid;                                                         \
                xpid = MM32(x)->map[oid].parent;                                    \
            }                                                                       \
            else                                                                    \
            {                                                                       \
                rid = MM32(x)->map[uid].right;                                      \
                lid = MM32(x)->map[uid].left;                                       \
                if(rid == 0 || MM32(x)->map[rid].color == MM32_COLOR_BLACK)         \
                {                                                                   \
                    if(lid > 0)MM32(x)->map[lid].color = MM32_COLOR_BLACK;          \
                    MM32(x)->map[uid].color = MM32_COLOR_RED;                       \
                    MM32_ROTATE_RIGHT(x, prootid, uid, lid, rid, ppid);             \
                    uid = MM32(x)->map[xpid].right;                                 \
                }                                                                   \
                MM32(x)->map[uid].color = MM32(x)->map[xpid].color;                 \
                MM32(x)->map[xpid].color = MM32_COLOR_BLACK;                        \
                if((rid = MM32(x)->map[uid].right) > 0)                             \
                    MM32(x)->map[rid].color = MM32_COLOR_BLACK;                     \
                MM32_ROTATE_LEFT(x, prootid, xpid, rid, lid, ppid);                 \
                oid = *prootid;                                                     \
                break;                                                              \
            }                                                                       \
        }                                                                           \
        else                                                                        \
        {                                                                           \
            uid = MM32(x)->map[xpid].left;                                          \
            if(MM32(x)->map[uid].color == MM32_COLOR_RED)                           \
            {                                                                       \
                MM32(x)->map[uid].color = MM32_COLOR_BLACK;                         \
                MM32(x)->map[xpid].color = MM32_COLOR_RED;                          \
                MM32_ROTATE_RIGHT(x, prootid, xpid, lid, rid, ppid);                \
                uid = MM32(x)->map[xpid].left;                                      \
            }                                                                       \
            lid = MM32(x)->map[uid].left;                                           \
            rid = MM32(x)->map[uid].right;                                          \
            if((lid == 0 || MM32(x)->map[lid].color == MM32_COLOR_BLACK)            \
                && (rid == 0 || MM32(x)->map[rid].color == MM32_COLOR_BLACK))       \
            {                                                                       \
                MM32(x)->map[uid].color = MM32_COLOR_RED;                           \
                oid = xpid;                                                         \
                xpid = MM32(x)->map[oid].parent;                                    \
            }                                                                       \
            else                                                                    \
            {                                                                       \
                rid = MM32(x)->map[uid].right;                                      \
                lid = MM32(x)->map[uid].left;                                       \
                if(lid == 0 || MM32(x)->map[lid].color == MM32_COLOR_BLACK)         \
                {                                                                   \
                    if(rid > 0)MM32(x)->map[rid].color = MM32_COLOR_BLACK;          \
                    MM32(x)->map[uid].color = MM32_COLOR_RED;                       \
                    MM32_ROTATE_LEFT(x, prootid, uid, rid, lid, ppid);              \
                    uid = MM32(x)->map[xpid].left;                                  \
                }                                                                   \
                MM32(x)->map[uid].color = MM32(x)->map[xpid].color;                 \
                MM32(x)->map[xpid].color = MM32_COLOR_BLACK;                        \
                if((lid = MM32(x)->map[uid].left) > 0)                              \
                    MM32(x)->map[lid].color = MM32_COLOR_BLACK;                     \
                MM32_ROTATE_RIGHT(x, prootid, xpid, lid, rid, ppid);                \
                oid = *prootid;                                                     \
                break;                                                              \
            }                                                                       \
        }                                                                           \
    }                                                                               \
    if(oid > 0) MM32(x)->map[oid].color = MM32_COLOR_BLACK;                         \
}while(0)
/* init mm32 */
void *mm32_init(char *file)
{
    void *x = NULL;
    struct stat  st = {0};

    if((x = (MM32 *)calloc(1, sizeof(MM32))))
    {
        if((MM32(x)->fd = open(file, O_CREAT|O_RDWR, 0644)) > 0 
                && fstat(MM32(x)->fd, &st) == 0)
        {
            MUTEX_INIT(MM32(x)->mutex);
            MM32(x)->end = st.st_size;
            MM32(x)->size = (off_t)sizeof(MM32STATE) + (off_t)sizeof(MM32NODE) * (off_t)MM32_NODES_MAX;
            //mmap
            MM32_MMAP(x);
            //init truncate
            if(st.st_size == 0)
            {
                MM32(x)->end = (off_t)sizeof(MM32STATE);
                MM32_INCRE(x);
            }
        }
        else 
        {
            if(MM32(x)->fd > 0) close(MM32(x)->fd);
            free(x);
            x = NULL;
        }
    }
    return x;
}

/* insert new root */
int mm32_new_tree(void *x)
{
    int id = 0, i = 0;
    if(x)
    {
        MUTEX_LOCK(MM32(x)->mutex);
        if(MM32(x)->state->nroots == 0) MM32(x)->state->nroots = 1;
        if(MM32(x)->state && MM32(x)->state->nroots < MM32_ROOT_MAX)
        {
            for(i = 1; i < MM32_ROOT_MAX; i++)
            {
                if(MM32(x)->state->roots[i].status == 0)
                {
                    MM32(x)->state->roots[i].status = 1;
                    MM32(x)->state->nroots++;
                    id = i;
                    break;
                }
            }
        }
        MUTEX_UNLOCK(MM32(x)->mutex);
    }
    return id;
}

/* total */
unsigned int mm32_total(void *x, int rootid)
{
    unsigned int total = 0;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(MM32(x)->mutex);
        if(MM32(x)->state && MM32(x)->map && rootid < MM32_ROOT_MAX)
        {
            total =  MM32(x)->state->roots[rootid].total;
        }
        MUTEX_UNLOCK(MM32(x)->mutex);
    }
    return total;
}

/* build */
unsigned int mm32_build(void *x, int rootid, int key, int data)
{
    unsigned int id = 0, nodeid = 0, rid = 0, lid = 0, uid = 0, pid = 0, 
        gpid = 0, ppid = 0, *prootid = NULL;
    MM32NODE *node = NULL;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(MM32(x)->mutex);
        if(MM32(x)->state && MM32(x)->map && rootid < MM32_ROOT_MAX
                && MM32(x)->state->roots[rootid].status > 0)
        {
            nodeid = MM32(x)->state->roots[rootid].rootid;
            while(nodeid > 0 && nodeid < MM32(x)->state->total)
            {
                node = &(MM32(x)->map[nodeid]);
                if(key > node->key)
                {
                    if(node->right == 0) break;
                    nodeid = node->right;
                }
                else 
                {
                    if(node->left == 0) break;
                    nodeid = node->left;
                }
            }
            //new node
            if(id == 0)
            {
                if(MM32(x)->state->left == 0)
                {
                    MM32_INCRE(x);
                }
                if(MM32(x)->state->qleft > 0)
                {
                    id = MM32(x)->state->qfirst;
                    MM32(x)->state->qfirst = MM32(x)->map[id].parent;
                    MM32(x)->state->qleft--;
                }
                else
                {
                    id = ++(MM32(x)->state->current);
                }
                MM32(x)->state->left--;
                MM32(x)->map[id].parent = nodeid;
                MM32(x)->map[id].key = key;
                MM32(x)->map[id].data = data;
                MM32_MIN_MAX(x, id, key);
                if(nodeid > 0)
                {
                    if(key > MM32(x)->map[nodeid].key) 
                        MM32(x)->map[nodeid].right = id;
                    else
                        MM32(x)->map[nodeid].left = id;
                }
                MM32(x)->state->roots[rootid].total++;
            }
        }
        if((nodeid = id) > 0)
        {
            if(MM32(x)->state->roots[rootid].rootid > 0)
            {
                prootid = &(MM32(x)->state->roots[rootid].rootid);
                MM32(x)->map[nodeid].color = MM32_COLOR_RED;
                MM32_INSERT_COLOR(x, prootid, nodeid, lid, rid, uid, pid, gpid, ppid);
            }
            else
            {
                MM32(x)->state->roots[rootid].rootid = nodeid;
            }
        }
end:
        MUTEX_UNLOCK(MM32(x)->mutex);
    }
    return id;
}

/* rebuild */
unsigned int mm32_rebuild(void *x, int rootid, unsigned int nodeid, int key)
{
    int ret = nodeid, data = 0;
    MM32NODE *node = NULL;

    if(x && rootid > 0 && nodeid > 0 && (node = &(MM32(x)->map[nodeid])))
    {
        if(node->key != key)
        {
            mm32_remove(x, rootid, nodeid, NULL, &data);
            ret = mm32_build(x, rootid, key, data);
        }
    }
    return ret;
}


/* insert new node */
unsigned int mm32_insert(void *x, int rootid, int key, int data, int *old)
{
    unsigned int id = 0, nodeid = 0, rid = 0, lid = 0, uid = 0, pid = 0, 
        gpid = 0, ppid = 0, *prootid = NULL;
    MM32NODE *node = NULL;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(MM32(x)->mutex);
        if(MM32(x)->state && MM32(x)->map && rootid < MM32_ROOT_MAX
                && MM32(x)->state->roots[rootid].status > 0)
        {
            nodeid = MM32(x)->state->roots[rootid].rootid;
            while(nodeid > 0 && nodeid < MM32(x)->state->total)
            {
                node = &(MM32(x)->map[nodeid]);
                if(key == node->key)
                {
                    id = nodeid;
                    if(old) *old = node->data;
                    node->data = data;
                    goto end;
                }
                else if(key > node->key)
                {
                    if(node->right == 0) break;
                    nodeid = node->right;
                }
                else 
                {
                    if(node->left == 0) break;
                    nodeid = node->left;
                }
            }
            //new node
            if(id == 0)
            {
                //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, MM32(x)->start, MM32(x)->state, MM32(x)->map, MM32(x)->state->current, MM32(x)->state->left, MM32(x)->state->total, MM32(x)->state->qleft, MM32(x)->state->qfirst, MM32(x)->state->qlast);
                if(MM32(x)->state->left == 0)
                {
                    MM32_INCRE(x);
                }
                if(MM32(x)->state->qleft > 0)
                {
                    id = MM32(x)->state->qfirst;
                    MM32(x)->state->qfirst = MM32(x)->map[id].parent;
                    MM32(x)->state->qleft--;
                }
                else
                {
                    id = ++(MM32(x)->state->current);
                }
                //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, MM32(x)->start, MM32(x)->state, MM32(x)->map, MM32(x)->state->current, MM32(x)->state->left, MM32(x)->state->total, MM32(x)->state->qleft, MM32(x)->state->qfirst, MM32(x)->state->qlast);
                MM32(x)->state->left--;
                //memset(&(MM32(x)->map[id]), 0, sizeof(MM32NODE));
                MM32(x)->map[id].parent = nodeid;
                MM32(x)->map[id].key = key;
                MM32(x)->map[id].data = data;
                MM32_MIN_MAX(x, id, key);
                if(nodeid > 0)
                {
                    if(key > MM32(x)->map[nodeid].key) 
                        MM32(x)->map[nodeid].right = id;
                    else
                        MM32(x)->map[nodeid].left = id;
                }
                MM32(x)->state->roots[rootid].total++;
            }
            else
            {
                //fprintf(stdout, "%s::%d old id:%d pid:%d key:%d\n", __FILE__, __LINE__, id, parentid, key);
            }
        }
        if((nodeid = id) > 0)
        {
            if(MM32(x)->state->roots[rootid].rootid > 0)
            {
                prootid = &(MM32(x)->state->roots[rootid].rootid);
                MM32(x)->map[nodeid].color = MM32_COLOR_RED;
                MM32_INSERT_COLOR(x, prootid, nodeid, lid, rid, uid, pid, gpid, ppid);
            }
            else
            {
                MM32(x)->state->roots[rootid].rootid = nodeid;
            }
        }
end:
        MUTEX_UNLOCK(MM32(x)->mutex);
    }
    return id;
}

/* try insert  node */
unsigned int mm32_try_insert(void *x, int rootid, int key, int data, int *old)
{
    unsigned int id = 0, nodeid = 0, rid = 0, lid = 0, uid = 0, pid = 0, 
        gpid = 0, ppid = 0, *prootid = NULL;
    MM32NODE *node = NULL;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(MM32(x)->mutex);
        if(MM32(x)->state && MM32(x)->map && rootid < MM32_ROOT_MAX
                && MM32(x)->state->roots[rootid].status > 0)
        {
            nodeid = MM32(x)->state->roots[rootid].rootid;
            while(nodeid > 0 && nodeid < MM32(x)->state->total)
            {
                node = &(MM32(x)->map[nodeid]);
                if(key == node->key)
                {
                    id = nodeid;
                    if(old) *old = node->data;
                    //fprintf(stdout, "%s::%d id:%d key:%lld old[%lld]->data:%d\n", __FILE__, __LINE__, nodeid, (long long)key, (long long)node->key, node->data);
                    goto end;
                }
                else if(key > node->key)
                {
                    if(node->right == 0) break;
                    nodeid = node->right;
                }
                else 
                {
                    if(node->left == 0) break;
                    nodeid = node->left;
                }
            }
            //new node
            if(id == 0)
            {
                //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, MM32(x)->start, MM32(x)->state, MM32(x)->map, MM32(x)->state->current, MM32(x)->state->left, MM32(x)->state->total, MM32(x)->state->qleft, MM32(x)->state->qfirst, MM32(x)->state->qlast);
                if(MM32(x)->state->left == 0)
                {
                    MM32_INCRE(x);
                }
                if(MM32(x)->state->qleft > 0)
                {
                    id = MM32(x)->state->qfirst;
                    MM32(x)->state->qfirst = MM32(x)->map[id].parent;
                    MM32(x)->state->qleft--;
                }
                else
                {
                    id = ++(MM32(x)->state->current);
                }
                //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, MM32(x)->start, MM32(x)->state, MM32(x)->map, MM32(x)->state->current, MM32(x)->state->left, MM32(x)->state->total, MM32(x)->state->qleft, MM32(x)->state->qfirst, MM32(x)->state->qlast);
                MM32(x)->state->left--;
                //memset(&(MM32(x)->map[id]), 0, sizeof(MM32NODE));
                MM32(x)->map[id].parent = nodeid;
                MM32(x)->map[id].key = key;
                MM32(x)->map[id].data = data;
                MM32_MIN_MAX(x, id, key);
                if(nodeid > 0)
                {
                    if(key > MM32(x)->map[nodeid].key) 
                        MM32(x)->map[nodeid].right = id;
                    else
                        MM32(x)->map[nodeid].left = id;
                }
                MM32(x)->state->roots[rootid].total++;
            }
            else
            {
                //fprintf(stdout, "%s::%d old id:%d pid:%d key:%d\n", __FILE__, __LINE__, id, parentid, key);
            }
        }
        if((nodeid = id) > 0)
        {
            if(MM32(x)->state->roots[rootid].rootid > 0)
            {
                prootid = &(MM32(x)->state->roots[rootid].rootid);
                MM32(x)->map[nodeid].color = MM32_COLOR_RED;
                MM32_INSERT_COLOR(x, prootid, nodeid, lid, rid, uid, pid, gpid, ppid);
            }
            else
            {
                MM32(x)->state->roots[rootid].rootid = nodeid;
            }
        }
end:
        MUTEX_UNLOCK(MM32(x)->mutex);
    }
    return id;
}


/* get node key/data */
unsigned int mm32_get(void *x, unsigned int tnodeid, int *key, int *data)
{
    unsigned int id = 0;

    if(x && tnodeid > 0)
    {
        MUTEX_LOCK(MM32(x)->mutex);
        if(MM32(x)->map && MM32(x)->state && tnodeid <  MM32(x)->state->total)
        {
            if(key) *key = MM32(x)->map[tnodeid].key;
            if(data) *data = MM32(x)->map[tnodeid].data;
            id = tnodeid;
        }
        MUTEX_UNLOCK(MM32(x)->mutex);
    }
    return id;
}

/* find key/data */
unsigned int mm32_find(void *x, int rootid, int key, int *data)
{
    unsigned int id = 0, ret = 0;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(MM32(x)->mutex);
        if(MM32(x)->map && MM32(x)->state && rootid < MM32_ROOT_MAX
                && MM32(x)->state->roots[rootid].status > 0)
        {
            id = MM32(x)->state->roots[rootid].rootid;
            while(id > 0 && id < MM32(x)->state->total)
            {
                if(key == MM32(x)->map[id].key)
                {
                    if(data) *data = MM32(x)->map[id].data;
                    ret = id;
                    break;
                }
                else if(MM32(x)->map[id].key < key)
                {
                    id = MM32(x)->map[id].right;
                }
                else
                {
                    id = MM32(x)->map[id].left;
                }
            }
        }
        MUTEX_UNLOCK(MM32(x)->mutex);
    }
    return ret;
}

/* find great than key */
unsigned int mm32_find_gt(void *x, int rootid, int key, int *data)
{
    unsigned int id = 0, ret = 0;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(MM32(x)->mutex);
        if(MM32(x)->map && MM32(x)->state && rootid < MM32_ROOT_MAX
                && MM32(x)->state->roots[rootid].status > 0)
        {
            id = MM32(x)->state->roots[rootid].rootid;
            while(id > 0 && id < MM32(x)->state->total)
            {
                if(MM32(x)->map[id].key > key)
                {
                    if(data) *data = MM32(x)->map[id].data;
                    ret = id;
                    break;
                }
                else
                {
                    id = MM32(x)->map[id].right;
                }
            }
        }
        MUTEX_UNLOCK(MM32(x)->mutex);
    }
    return id;
}

unsigned int mm32_find_gt2(void *x, int rootid, int key, int *data)
{
    unsigned int id = 0, ret = 0;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(MM32(x)->mutex);
        if(MM32(x)->map && MM32(x)->state && rootid < MM32_ROOT_MAX
                && MM32(x)->state->roots[rootid].status > 0)
        {
            id = MM32(x)->state->roots[rootid].rootid;
            while(id > 0 && id < MM32(x)->state->total)
            {
                if(MM32(x)->map[id].key >= key)
                {
                    if(data) *data = MM32(x)->map[id].data;
                    ret = id;
                    break;
                }
                else
                {
                    id = MM32(x)->map[id].right;
                }
            }
        }
        MUTEX_UNLOCK(MM32(x)->mutex);
    }
    return id;
}

/* find less than key */
unsigned int mm32_find_lt(void *x, int rootid, int key, int *data)
{
    unsigned int id = 0, ret = 0;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(MM32(x)->mutex);
        if(MM32(x)->map && MM32(x)->state && rootid < MM32_ROOT_MAX
                && MM32(x)->state->roots[rootid].status > 0)
        {
            id = MM32(x)->state->roots[rootid].rootid;
            while(id > 0 && id < MM32(x)->state->total)
            {
                if(MM32(x)->map[id].key < key)
                {
                    if(data) *data = MM32(x)->map[id].data;
                    ret = id;
                    break;
                }
                else
                {
                    id = MM32(x)->map[id].left;
                }
            }
        }
        MUTEX_UNLOCK(MM32(x)->mutex);
    }
    return id;
}

/* find less than key */
unsigned int mm32_find_lt2(void *x, int rootid, int key, int *data)
{
    unsigned int id = 0, ret = 0;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(MM32(x)->mutex);
        if(MM32(x)->map && MM32(x)->state && rootid < MM32_ROOT_MAX
                && MM32(x)->state->roots[rootid].status > 0)
        {
            id = MM32(x)->state->roots[rootid].rootid;
            while(id > 0 && id < MM32(x)->state->total)
            {
                if(MM32(x)->map[id].key <= key)
                {
                    if(data) *data = MM32(x)->map[id].data;
                    ret = id;
                    break;
                }
                else
                {
                    id = MM32(x)->map[id].left;
                }
            }
        }
        MUTEX_UNLOCK(MM32(x)->mutex);
    }
    return id;
}
/* get tree->min key/data */
unsigned int mm32_min(void *x, int rootid, int *key, int *data)
{
    unsigned int id = 0;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(MM32(x)->mutex);
        if(MM32(x)->map && MM32(x)->state && rootid <  MM32_ROOT_MAX
                && MM32(x)->state->roots[rootid].status > 0)
        {
            id = MM32(x)->state->roots[rootid].rootid;
            while(MM32(x)->map[id].left > 0)
            {
                id = MM32(x)->map[id].left;
            }
            if(id > 0 && MM32(x)->state->total)
            {
                if(key) *key = MM32(x)->map[id].key;
                if(data) *data = MM32(x)->map[id].data;
            }
        }
        MUTEX_UNLOCK(MM32(x)->mutex);
    }
    return id;
}

/* get tree->max key/data */
unsigned  int mm32_max(void *x, int rootid, int *key, int *data)
{
    unsigned int id = 0, tmp = 0;

    if(x && rootid > 0)
    {
        MUTEX_LOCK(MM32(x)->mutex);
        if(MM32(x)->map && MM32(x)->state && rootid <  MM32_ROOT_MAX
                && MM32(x)->state->roots[rootid].status > 0)
        {
            tmp = MM32(x)->state->roots[rootid].rootid;
            do
            {
                id = tmp;
            }while(id > 0 && (tmp = MM32(x)->map[id].right) > 0);
            if(id > 0 && MM32(x)->state->total)
            {
                if(key) *key = MM32(x)->map[id].key;
                if(data) *data = MM32(x)->map[id].data;
            }
        }
        MUTEX_UNLOCK(MM32(x)->mutex);
    }
    return id;
}

/* get next node key/data */
unsigned int mm32_next(void *x, int rootid, unsigned int tnodeid, int *key, int *data)
{
    unsigned int id = 0, parentid = 0;

    if(x && tnodeid > 0)
    {
        MUTEX_LOCK(MM32(x)->mutex);
        if(MM32(x)->map && MM32(x)->state && tnodeid <  MM32(x)->state->total)
        {
            id = tnodeid;
            if(MM32(x)->map[id].right > 0)
            {
                id = MM32(x)->map[id].right;
                while(MM32(x)->map[id].left  > 0)
                {
                    id = MM32(x)->map[id].left;
                }
                //fprintf(stdout, "%s::%d id:%d\n", __FILE__, __LINE__, id);
            }
            else
            {
                while(id > 0)
                {
                    parentid = MM32(x)->map[id].parent;
                    if(MM32(x)->map[id].key < MM32(x)->map[parentid].key)
                    {
                        id = parentid;
                        goto end;
                    }
                    else
                    {
                        id = parentid;
                    }
                }
                //fprintf(stdout, "%s::%d id:%d\n", __FILE__, __LINE__, id);
            }
end:
            if(id > 0 && id < MM32(x)->state->total)
            {
                if(key) *key = MM32(x)->map[id].key;
                if(data) *data = MM32(x)->map[id].data;
            }
            //fprintf(stdout, "%s::%d rootid:%d tnodeid:%d id:%d\n",__FILE__, __LINE__, rootid, tnodeid, id);
        }
        MUTEX_UNLOCK(MM32(x)->mutex);
    }
    return id;
}

/* get prev node key/data */
unsigned int mm32_prev(void *x, int rootid, unsigned int tnodeid, int *key, int *data)
{
    unsigned int id = 0, parentid = 0;

    if(x && tnodeid > 0)
    {
        MUTEX_LOCK(MM32(x)->mutex);
        if(MM32(x)->map && MM32(x)->state && tnodeid <  MM32(x)->state->total)
        {
            id = tnodeid;
            if(MM32(x)->map[id].left > 0)
            {
                id = MM32(x)->map[id].left;
                while(MM32(x)->map[id].right  > 0)
                {
                    id = MM32(x)->map[id].right;
                }
                //fprintf(stdout, "%s::%d id:%d\n", __FILE__, __LINE__, id);
            }
            else
            {
                while(id > 0)
                {
                    parentid = MM32(x)->map[id].parent;
                    if(MM32(x)->map[id].key > MM32(x)->map[parentid].key)
                    {
                        id = parentid;
                        goto end;
                    }
                    else
                    {
                        id = parentid;
                    }
                }
                //fprintf(stdout, "%s::%d id:%d\n", __FILE__, __LINE__, id);
            }
end:
            if(id > 0 && id < MM32(x)->state->total)
            {
                if(key)*key = MM32(x)->map[id].key;
                if(data)*data = MM32(x)->map[id].data;
            }
            //fprintf(stdout, "%s::%d rootid:%d tnodeid:%d id:%d\n",__FILE__, __LINE__, rootid, tnodeid, id);
        }
        MUTEX_UNLOCK(MM32(x)->mutex);
    }
    return id;
}

/* view node */
void mm32_view_tnode(void *x, unsigned int tnodeid, FILE *fp)
{
    if(x)
    {
        if(MM32(x)->map[tnodeid].left > 0 && MM32(x)->map[tnodeid].left < MM32(x)->state->total)
        {
            mm32_view_tnode(x, MM32(x)->map[tnodeid].left, fp);
        }
        fprintf(fp, "[%d:%lld:%d]\n", tnodeid, (long long)MM32(x)->map[tnodeid].key, MM32(x)->map[tnodeid].data);
        if(MM32(x)->map[tnodeid].right > 0 && MM32(x)->map[tnodeid].right < MM32(x)->state->total)
        {
            mm32_view_tnode(x, MM32(x)->map[tnodeid].right, fp);
        }
    }
    return ;
}

void mm32_view_tree(void *x, int rootid, FILE *fp)
{
    if(x && rootid > 0)
    {
        MUTEX_LOCK(MM32(x)->mutex);
        if(MM32(x)->map && MM32(x)->state && rootid < MM32_ROOT_MAX)
        {
            fprintf(stdout, "%s::%d rootid:%d\n", __FILE__, __LINE__, MM32(x)->state->roots[rootid].rootid);
             mm32_view_tnode(x, MM32(x)->state->roots[rootid].rootid, fp);
        }
        MUTEX_UNLOCK(MM32(x)->mutex);
    }
    return ;
}

/* set data */
int mm32_set_data(void *x, unsigned int tnodeid, int data)
{
    int old = -1;

    if(x && tnodeid > 0)
    {
        MUTEX_LOCK(MM32(x)->mutex);
        if(MM32(x)->map && MM32(x)->state && tnodeid < MM32(x)->state->total)
        {
            old = MM32(x)->map[tnodeid].data;
            MM32(x)->map[tnodeid].data = data;
        }
        MUTEX_UNLOCK(MM32(x)->mutex);
    }
    return old;
}

/* remove node */
void mm32_remove(void *x, int rootid, unsigned int tnodeid, int *key, int *data)
{
    unsigned int id = 0, pid = 0, parent = 0, child = 0, rid = 0, lid = 0,
        uid = 0, ppid = 0, z = 0, color = 0, *prootid = NULL;

    if(x && rootid > 0 && tnodeid > 0)
    {
        MUTEX_LOCK(MM32(x)->mutex);
        if(MM32(x)->map && MM32(x)->state && tnodeid < MM32(x)->state->total)
        {
            if(key) *key = MM32(x)->map[tnodeid].key;
            if(data) *data = MM32(x)->map[tnodeid].data;
            id = tnodeid;
            if(MM32(x)->map[tnodeid].left == 0)
            {
                child = MM32(x)->map[tnodeid].right;
            }
            else if(MM32(x)->map[tnodeid].right == 0)
            {
                child = MM32(x)->map[tnodeid].left;
            }
            else 
            {
                id = MM32(x)->map[tnodeid].right;
                while(MM32(x)->map[id].left > 0)
                    id = MM32(x)->map[id].left;
                parent = MM32(x)->map[id].parent;
                color = MM32(x)->map[id].color;
                if((child = MM32(x)->map[id].right) > 0)
                    MM32(x)->map[child].parent = parent;
                if((pid = parent) > 0)
                {
                    if(MM32(x)->map[pid].left == id)
                        MM32(x)->map[pid].left = child;
                    else
                        MM32(x)->map[pid].right = child;
                }
                else
                {
                    MM32(x)->state->roots[rootid].rootid = child;
                }
                if(MM32(x)->map[id].parent == tnodeid) parent = id;
                MM32(x)->map[id].color = MM32(x)->map[tnodeid].color;
                MM32(x)->map[id].parent = MM32(x)->map[tnodeid].parent;
                MM32(x)->map[id].left = MM32(x)->map[tnodeid].left;
                MM32(x)->map[id].right = MM32(x)->map[tnodeid].right;
                if((pid = MM32(x)->map[tnodeid].parent) > 0)
                {
                    if(MM32(x)->map[pid].left == tnodeid)
                        MM32(x)->map[pid].left = id;
                    else
                        MM32(x)->map[pid].right = id;
                }
                else
                {
                    MM32(x)->state->roots[rootid].rootid = id;
                }
                lid = MM32(x)->map[tnodeid].left;
                MM32(x)->map[lid].parent = id;
                if((rid = MM32(x)->map[tnodeid].right) > 0)
                    MM32(x)->map[rid].parent = id;
                goto color_remove;
            }
            parent =  MM32(x)->map[tnodeid].parent;
            color = MM32(x)->map[tnodeid].color;
            if(child > 0) 
            {
                MM32(x)->map[child].parent = parent;
            }
            if((pid = parent) > 0)
            {
                if(tnodeid == MM32(x)->map[pid].left) 
                    MM32(x)->map[pid].left = child;
                else 
                    MM32(x)->map[pid].right = child;
            }
            else 
            {
                MM32(x)->state->roots[rootid].rootid = child;
            }
            //remove color set
color_remove:
            MM32(x)->state->roots[rootid].total--;
            if(color == MM32_COLOR_BLACK)
            {
                //fprintf(stdout, "%s::%d node:%d parent:%d left:%d right:%d key:%d data:%d\n", __FILE__, __LINE__, tnodeid, MM32(x)->map[tnodeid].parent, MM32(x)->map[tnodeid].left, MM32(x)->map[tnodeid].right, MM32(x)->map[tnodeid].key, MM32(x)->map[tnodeid].data);
                prootid = &(MM32(x)->state->roots[rootid].rootid);
                MM32_REMOVE_COLOR(x, prootid, child, parent, lid, rid, uid, ppid);
            }
            //add to qleft
            memset(&(MM32(x)->map[tnodeid]), 0, sizeof(MM32NODE));
            if(MM32(x)->state->qleft == 0)
            {
                MM32(x)->state->qfirst = MM32(x)->state->qlast = tnodeid;
            }
            else
            {
                z = MM32(x)->state->qlast;
                MM32(x)->map[z].parent = tnodeid;
                MM32(x)->state->qlast = tnodeid;
            }
            MM32(x)->state->qleft++;
            MM32(x)->state->left++;
            //fprintf(stdout, "%s::%d %d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, id, MM32(x)->start, MM32(x)->state, MM32(x)->map, MM32(x)->state->current, MM32(x)->state->left, MM32(x)->state->total, MM32(x)->state->qleft, MM32(x)->state->qfirst, MM32(x)->state->qlast);
  
        }
        MUTEX_UNLOCK(MM32(x)->mutex);
    }
    return ;
}

/* remove node */
void mm32_remove_tnode(void *x, unsigned int tnodeid)
{
    unsigned int id = 0;

    if(x)
    {
        if(MM32(x)->map[tnodeid].left > 0 && MM32(x)->map[tnodeid].left < MM32(x)->state->total)
        {
            mm32_remove_tnode(x, MM32(x)->map[tnodeid].left);
        }
        if(MM32(x)->map[tnodeid].right > 0 && MM32(x)->map[tnodeid].right < MM32(x)->state->total)
        {
            mm32_remove_tnode(x, MM32(x)->map[tnodeid].right);
        }
        memset(&(MM32(x)->map[tnodeid]), 0, sizeof(MM32NODE));
        if(MM32(x)->state->qleft == 0)
        {
            MM32(x)->state->qfirst = MM32(x)->state->qlast = tnodeid;
        }
        else
        {
            id = MM32(x)->state->qlast;
            MM32(x)->map[id].parent = tnodeid;
            MM32(x)->state->qlast = tnodeid;
        }
        MM32(x)->state->qleft++;
        MM32(x)->state->left++;
    }
    return ;
}

/* remove tree */
void mm32_remove_tree(void *x, int rootid)
{
    if(x && rootid > 0 && rootid < MM32_ROOT_MAX)
    {
        MUTEX_LOCK(MM32(x)->mutex);
        mm32_remove_tnode(x, MM32(x)->state->roots[rootid].rootid);
        MM32(x)->state->roots[rootid].rootid = 0;
        MM32(x)->state->roots[rootid].status = 0;
        //fprintf(stdout, "%s::%d rootid:%d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d\n", __FILE__, __LINE__, rootid, MM32(x)->start, MM32(x)->state, MM32(x)->map, MM32(x)->state->current, MM32(x)->state->left, MM32(x)->state->total, MM32(x)->state->qleft, MM32(x)->state->qfirst, MM32(x)->state->qlast);
 
        MUTEX_UNLOCK(MM32(x)->mutex);
    }
    return ;
}

//close mm32
void mm32_close(void *x)
{
    if(x)
    {
        //fprintf(stdout, "%s::%d start:%p state:%p map:%p current:%d left:%d total:%d qleft:%d qfirst:%d qlast:%d sizeof(MM32STATE):%d\n", __FILE__, __LINE__, MM32(x)->start, MM32(x)->state, MM32(x)->map, MM32(x)->state->current, MM32(x)->state->left, MM32(x)->state->total, MM32(x)->state->qleft, MM32(x)->state->qfirst, MM32(x)->state->qlast, sizeof(MM32STATE));
        MM32_MUNMAP(x);
        MUTEX_DESTROY(MM32(x)->mutex);
        if(MM32(x)->fd) close(MM32(x)->fd);
        free(x);
    }
}


#ifdef _DEBUG_MM32
#include "md5.h"
#include "timer.h"
int main(int argc, char **argv) 
{
    int i = 0, rootid = 0, id = 0, j = 0, old = 0, data = 0, n = 0, count = 50000000;
    unsigned char digest[MD5_LEN];
    void *mm32 = NULL;
    void *timer = NULL;
    char line[1024];
    int key = 0;

    if((mm32 = mm32_init("/tmp/test.mm32")))
    {
        rootid = mm32_new_tree(mm32);
        TIMER_INIT(timer);
        for(j = 1; j <= count; j++)
        {
            n = sprintf(line, "http://www.demo.com/%d.html", j);
            md5(line, n, digest);
            key = *((int *)digest);
            old = -1;
            data = j;
            id = mm32_insert(mm32, rootid, key, data, &old);
            if(old > 0 || id <= 0) 
            {
                fprintf(stdout, "%d:{id:%d key:%d rootid:%d old:%d}\n", j, id, key, rootid, old);
            }
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "%s::%d insert:%d time:%lld\n", __FILE__,__LINE__, count, PT_LU_USEC(timer));
        for(j = 1; j <= count; j++)
        {
            n = sprintf(line, "http://www.demo.com/%d.html", j);
            md5(line, n, digest);
            key = *((int *)digest);
            old = -1;
            data = j;
            id = mm32_try_insert(mm32, rootid, key, data, &old);
            if(old > 0 && old != j) 
            {
                fprintf(stdout, "%d:{id:%d key:%d rootid:%d old:%d}\n", j, id, key, rootid, old);
                _exit(-1);
            }
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "%s::%d try_insert:%d time:%lld\n", __FILE__,__LINE__, count, PT_LU_USEC(timer));
        TIMER_CLEAN(timer);
        mm32_close(mm32);
    }
}
//gcc -o mtree64 mm32.c md5.c -D_DEBUG_MM32 -g && ./mtree64
#endif
