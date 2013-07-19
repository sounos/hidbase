#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include "btree.h"
#include "mutex.h"
#include "timer.h"

BTREE *btree_init(char *file)
{
    struct stat st = {0};
    BTREE *btree = NULL;

    if(file && (btree = (BTREE *)calloc(1, sizeof(BTREE))))
    {
        if((btree->btio.fd = open(file, O_CREAT|O_RDWR, 0644)) > 0)
        {
            fstat(btree->btio.fd, &st);
            btree->btio.size = (off_t)sizeof(BTSTATE) + (off_t)sizeof(BTNODE) * (off_t)BT_NODE_MAX;
            btree->btio.map = (char *)mmap(NULL, btree->btio.size, PROT_READ|PROT_WRITE,
                MAP_SHARED, btree->btio.fd, 0);
            btree->btio.end = st.st_size;
            if(st.st_size == 0)
            {
                btree->btio.end = (off_t)sizeof(BTSTATE);
                ftruncate(btree->btio.fd,  btree->btio.end);
                memset(btree->btio.map, 0, sizeof(BTSTATE));
            }
            btree->state = (BTSTATE *)btree->btio.map;
            btree->nodes = (BTNODE *)(btree->btio.map + sizeof(BTSTATE));
            MUTEX_INIT(btree->mutex);
        }
        else
        {
            free(btree);
            btree = NULL;
        }
    }
    return btree;
}

#define BTREE_CHECK_INCRE(tp)                                                       \
do                                                                                  \
{                                                                                   \
    if(tp->state->left == 0)                                                        \
    {                                                                               \
        tp->btio.old = tp->btio.end;                                                \
        tp->btio.end += (off_t)(sizeof(BTNODE)*BT_NODE_INCRE);                      \
        ftruncate(tp->btio.fd, tp->btio.end);                                       \
        memset(tp->btio.map + tp->btio.old, 0, tp->btio.end - tp->btio.old);        \
        tp->state->left += BT_NODE_INCRE;                                           \
        if(tp->state->current == 0){tp->state->current++;tp->state->left--;}        \
    }                                                                               \
}while(0)

unsigned int btree_new(BTREE *btree)
{
    unsigned int id = 0, x = 0, n = 0;
    if(btree)
    {
        if(btree->state->nqwait > 0)
        {
           n = --(btree->state->nqwait); 
           id = btree->state->qwait[n];
           memset(&(btree->nodes[id]), 0, sizeof(BTNODE));
           btree->state->qwait[n] = 0;
        }
        else
        {
            if(btree->state->left == 0)
            {
                BTREE_CHECK_INCRE(btree);
                id = btree->state->current++;
                btree->state->left--;
            }
        }
    }
    return id;
}

/* left node */
int btree_left(BTREE *btree, unsigned int id)
{
    int ret = -1;

    if(btree && id > 0 && btree->state->nqwait < BT_LEFT_MAX)
    {
        btree->state->qwait[btree->state->nqwait++] = id;    
    }
    return ret;
}

/* insert */
int btree_insert(BTREE *btree, int rootid, int32_t key, int val)
{
    int ret = -1, id = 0, min = 0, max = 0, des = 0, n = 0, newid = 0;

    if(btree && rootid >= 0 && rootid < BT_CHILDS_MAX)
    {
        id = btree->state->roots[rootid].rootid;
        if(btree->state->roots[rootid].rootid == 0)
        {
            id = btree->state->roots[rootid].rootid = btree_new(btree);
            btree->nodes[id].keys[0] = key;
            btree->nodes[id].nkeys++;
        }
        else
        {
            while(id > 0)
            {
                min = 0;
                max = btree->nodes[id].nkeys - 1;
                if(btree->nodes[id].keys[min] == key || btree->nodes[id].keys[max] == key)
                {
                    ret = id;
                    break;
                }
                if(btree->nodes[id].nkeys < BT_CHILDS_MAX)
                {
                    n = btree->nodes[id].nkeys++;
                    while(btree->nodes[id].keys[n-1] > key)
                    {
                        btree->nodes[id].keys[n] = btree->nodes[id].keys[n-1];
                        btree->nodes[id].childs[n+1] = btree->nodes[id].childs[n];
                        --n;
                    }
                    btree->nodes[id].keys[n] = key;
                    btree->nodes[id].childs[n] = 0;
                }
                else
                {
                    des = 0;
                    if(key < btree->nodes[id].keys[min])
                    {
                        des = min;
                    }
                    else if(key > btree->nodes[id].keys[max])
                    {
                        des = max + 1;
                    }
                    else
                    {
                        while(max > min)
                        {
                            des = (min + max)/2;
                            if(btree->nodes[id].keys[des] > key) max = des;
                            else min = des;
                        }
                    }
                    if(btree->nodes[id].nchilds > 0 && btree->nodes[id].childs[des] > 0)
                    {
                        id = btree->nodes[id].childs[des]; 
                    }
                    else
                    {
                        newid = btree_new(btree);
                        //check new
                        if(btree->nodes[id].keys[des] > key)
                        {
                            id = btree->nodes[id].childs[des];
                        }
                        else
                        {
                            id = btree->nodes[id].childs[des+1];
                        }
                    }
                }
            }
        }
    }
    return ret;
}

/* close */
void btree_close(BTREE *btree)
{
    if(btree)
    {
        if(btree->btio.map) munmap(btree->btio.map, btree->btio.size);
        if(btree->btio.fd > 0) close(btree->btio.fd);
        MUTEX_DESTROY(btree->mutex);
        free(btree);
    }
    return ;
}

#ifdef _DEBUG_BTREE
int main()
{
    int i = 0, j = 0, num = 10000000;
    BTREE *btree = NULL;

    if((btree = btree_init("/tmp/i32")))
    {
        //memset(&record, 0, sizeof(MRECORD));
        for(i = 0; i < num; i++)
        {
            for(j = 0; j < 8; j++)
            {
                btree_insert(btree, j, rand(), 0);
            }
        }

        btree_close(btree);
    }
    return 0;
}
#endif
