#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include "mmkey.h"
#include "mutex.h"
#ifdef MAP_LOCKED
#define MMAP_SHARED MAP_SHARED|MAP_LOCKED
#else
#define MMAP_SHARED MAP_SHARED
#endif
#define MKEYNODE_COPY(new, old)                                                 \
do                                                                              \
{                                                                               \
    new.key = old.key;                                                          \
    new.nchilds = old.nchilds;                                                  \
    new.data = old.data;                                                        \
    new.childs = old.childs;                                                    \
}while(0)
#define MKEYNODE_SETK(node, val)                                                \
do                                                                              \
{                                                                               \
    node.key = val;                                                             \
    node.nchilds = 0;                                                           \
    node.data = 0;                                                              \
    node.childs = 0;                                                            \
}while(0)
/* initialize mmap */
#define MMKEY_MAP_INIT(x)                                                                  \
do                                                                                          \
{                                                                                           \
    if(x && x->fd > 0)                                                                      \
    {                                                                                       \
        if(x->file_size == 0)                                                               \
        {                                                                                   \
            x->file_size = (off_t)sizeof(MKEYSTATE)                                         \
            + (off_t)MMKEY_INCREMENT_NUM * (off_t)sizeof(MKEYNODE);                         \
            if(ftruncate(x->fd, x->file_size) != 0)break;                                   \
        }                                                                                   \
        if(x->file_size > 0 && x->map == NULL)                                              \
        {                                                                                   \
            x->size = (off_t)sizeof(MKEYSTATE)                                              \
            + (off_t)MMKEY_NODES_MAX * (off_t)sizeof(MKEYNODE);                             \
            if((x->map = mmap(NULL, x->size, PROT_READ|PROT_WRITE,                          \
                            MAP_SHARED, x->fd, 0)) && x->map != (void *)-1)                 \
            {                                                                               \
                x->state = (MKEYSTATE *)(x->map);                                           \
                if(x->state->total == 0)                                                    \
                {                                                                           \
                    x->state->total = MMKEY_INCREMENT_NUM;                                  \
                    x->state->left = MMKEY_INCREMENT_NUM - MMKEY_LINE_MAX;                  \
                    x->state->current = MMKEY_LINE_MAX;                                     \
                }                                                                           \
                x->nodes = (MKEYNODE *)((char *)(x->map) + sizeof(MKEYSTATE));              \
            }                                                                               \
            else                                                                            \
            {                                                                               \
                x->map = NULL;                                                              \
                x->state = NULL;                                                            \
                x->nodes = NULL;                                                            \
            }                                                                               \
        }                                                                                   \
    }                                                                                       \
}while(0)

/* increment */
#define MMKEY_INCREMENT(x)                                                                  \
do                                                                                          \
{                                                                                           \
    if(x && x->file_size < x->size)                                                         \
    {                                                                                       \
        x->file_size += MMKEY_INCREMENT_NUM * sizeof(MKEYNODE);                             \
        if(ftruncate(x->fd, x->file_size) != 0)break;                                       \
        x->state->total += MMKEY_INCREMENT_NUM;                                             \
        x->state->left += MMKEY_INCREMENT_NUM;                                              \
    }                                                                                       \
}while(0)

/* push node list */
#define MMKEY_PUSH(x, num, pos)                                                             \
do                                                                                          \
{                                                                                           \
    if(x && pos >= MMKEY_LINE_MAX && num > 0 && x->state && x->nodes)                       \
    {                                                                                       \
        x->nodes[pos].childs = x->state->list[num-1].head;                                  \
        x->state->list[num-1].head = pos;                                                   \
        x->state->list[num-1].count++;                                                      \
    }                                                                                       \
}while(0)

/* pop new nodelist */
#define MMKEY_POP(x, num, pos)                                                              \
do                                                                                          \
{                                                                                           \
    pos = -1;                                                                               \
    if(x && num > 0 && num <= MMKEY_LINE_MAX && x->state && x->nodes)                      \
    {                                                                                       \
        if(x->state->list[num-1].count > 0)                                                 \
        {                                                                                   \
            pos = x->state->list[num-1].head;                                               \
            x->state->list[num-1].head = x->nodes[pos].childs;                              \
            x->state->list[num-1].count--;                                                  \
        }                                                                                   \
        else                                                                                \
        {                                                                                   \
            if(x->state->left < num){MMKEY_INCREMENT(x);}                                  \
            pos = x->state->current;                                                        \
            x->state->current += num;                                                       \
            x->state->left -= num;                                                          \
        }                                                                                   \
    }                                                                                       \
}while(0)

        //memset(&(x->nodes[pos]), 0, sizeof(MKEYNODE) * num);                                
/* add */
int mmkey_add(MMKEY *mmkey, unsigned short *list, int num, int data)
{
    unsigned int x = 0, i = 0,j = 0, k = 0, n = 0, pos = 0, 
        z = 0, min = 0, max = 0, m = 0, key = 0;
    MKEYNODE *nodes = NULL, *childs = NULL;
    int ret = -1;

    if(mmkey && list && num > 0)
    {
        MUTEX_LOCK(mmkey->mutex);        
        if((nodes = mmkey->nodes) && mmkey->map && mmkey->state)
        {
            key = list[m];i = key; if(++m < num) key = list[m];
            while(m < num)
            {
                x = 0;
                //check 
                if(nodes[i].nchilds  > 0 && nodes[i].childs >= MMKEY_LINE_MAX) 
                {
                    min = nodes[i].childs;
                    max = min + nodes[i].nchilds - 1;
                    if(key == nodes[min].key) x = min;
                    else if(key == nodes[max].key) x = max;
                    else if(key < nodes[min].key) x = -1;
                    else if(key > nodes[max].key) x = 1;
                    else
                    {
                        while(max > min)
                        {
                            z = (max + min)/2;
                            if(z == min) {x = z;break;}
                            if(key == nodes[z].key) {x = z;break;}
                            else if(key > nodes[z].key) min = z;
                            else max = z;
                        }
                    }
                }
                //new node
                if(x == -1 || x < MMKEY_LINE_MAX || nodes[x].key != key)
                {
                    n  = nodes[i].nchilds + 1;
                    z = nodes[i].childs;
                    MMKEY_POP(mmkey, n, pos);
                    if(pos < MMKEY_LINE_MAX) 
                    {
                        goto end;
                        //fprintf(stdout, "%s::%d key:%s k:%c min:%d max:%d x:%d z:%d n:%d\n", __FILE__, __LINE__, key, *p, min, max, x, z, n);
                    }
                    childs = &(nodes[pos]);
                    if(x == 0)
                    {
                        MKEYNODE_SETK(childs[0], key);
                        j = pos;
                    }
                    else if(x == -1) 
                    {
                        MKEYNODE_SETK(childs[0], key);
                        k = 1;
                        while(k < n)
                        {
                            MKEYNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        j = pos;
                    }
                    else if(x == 1)
                    {
                        k = 0;
                        while(k < (n-1))
                        {
                            MKEYNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        MKEYNODE_SETK(childs[k], key);
                        j = pos + k ;
                    }
                    else
                    {
                        //0 1 3 4(6) 7 9 10
                        k = 0;
                        while(nodes[z].key < key)
                        {
                            MKEYNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        MKEYNODE_SETK(childs[k], key);
                        x = k++;
                        while(k < n)
                        {
                            MKEYNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        j =  pos + x;
                    }
                    MMKEY_PUSH(mmkey, nodes[i].nchilds, nodes[i].childs);
                    nodes[i].nchilds++;
                    nodes[i].childs = pos;
                    i = j;
                }
                else i = x;
                if(++m < num) key = list[m];
            }
            //fprintf(stdout, "rrrrrr:%s::%d i:%d data:%d\r\n", __FILE__, __LINE__, i, data);
            if((ret = nodes[i].data) == 0)
                ret = nodes[i].data = data;
        }
        else 
        {
            ret = -4;
        }
end:
        MUTEX_UNLOCK(mmkey->mutex);        
    }else ret = -5;
    
    return ret;
}

/* add /return auto increment id*/
int mmkey_xadd(MMKEY *mmkey, unsigned short *list, int num)
{
    unsigned int x = 0, i = 0,j = 0, k = 0, n = 0, pos = 0, 
        z = 0, min = 0, max = 0, m = 0, key = 0;
    MKEYNODE *nodes = NULL, *childs = NULL;
    int ret = -1;

    if(mmkey && list && num > 0)
    {
        MUTEX_LOCK(mmkey->mutex);        
        if((nodes = mmkey->nodes) && mmkey->map && mmkey->state)
        {
            key = list[m];i = key; if(++m < num) key = list[m];
            while(m < num)
            {
                x = 0;
                //check 
                if(nodes[i].nchilds  > 0 && nodes[i].childs >= MMKEY_LINE_MAX) 
                {
                    min = nodes[i].childs;
                    max = min + nodes[i].nchilds - 1;
                    if(key == nodes[min].key) x = min;
                    else if(key == nodes[max].key) x = max;
                    else if(key < nodes[min].key) x = -1;
                    else if(key > nodes[max].key) x = 1;
                    else
                    {
                        while(max > min)
                        {
                            z = (max + min)/2;
                            if(z == min) {x = z;break;}
                            if(nodes[z].key == key) {x = z;break;}
                            else if(key > nodes[z].key) min = z;
                            else max = z;
                        }
                    }
                }
                //new node
                if(x == -1 || x < MMKEY_LINE_MAX || nodes[x].key != key)
                {
                    n  = nodes[i].nchilds + 1;
                    z = nodes[i].childs;
                    MMKEY_POP(mmkey, n, pos);
                    if(pos < MMKEY_LINE_MAX) 
                    {
                        ret = -3;
                        goto end;
                    }
                    childs = &(nodes[pos]);
                    if(x == 0)
                    {
                        MKEYNODE_SETK(childs[0], key);
                        j = pos;
                    }
                    else if(x == -1) 
                    {
                        MKEYNODE_SETK(childs[0], key);
                        k = 1;
                        while(k < n)
                        {
                            MKEYNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        j = pos;
                    }
                    else if(x == 1)
                    {
                        k = 0;
                        while(k < (n-1))
                        {
                            MKEYNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        MKEYNODE_SETK(childs[k], key);
                        j = pos + k ;
                    }
                    else
                    {
                        //0 1 3 4(6) 7 9 10
                        k = 0;
                        while(nodes[z].key < key)
                        {
                            MKEYNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        MKEYNODE_SETK(childs[k], key);
                        x = k++;
                        while(k < n)
                        {
                            MKEYNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        j =  pos + x;
                    }
                    MMKEY_PUSH(mmkey, nodes[i].nchilds, nodes[i].childs);
                    nodes[i].nchilds++;
                    nodes[i].childs = pos;
                    i = j;
                }
                else i = x;
                if(++m < num) key = list[m];
            }
            if((ret = nodes[i].data) == 0)
                nodes[i].data = ret = ++(mmkey->state->id);
        }
        else 
        {
            ret = -4;
        }
end:
        MUTEX_UNLOCK(mmkey->mutex);        
    }
    else 
    {
        ret = -5;
    }
    return ret;
}

/* get */
int  mmkey_get(MMKEY *mmkey, unsigned short *list, int num)
{
    unsigned int ret = 0, x = 0, i = 0, z = 0, min = 0, max = 0, m = 0, key = 0;
    MKEYNODE *nodes = NULL;

    if(mmkey && list && num > 0)
    {
        MUTEX_LOCK(mmkey->mutex);        
        if((nodes = mmkey->nodes) && mmkey->map && mmkey->state)
        {
            key = list[m];i = key; if(++m < num) key = list[m];
            if(num == 1 && i < mmkey->state->total){ret = nodes[i].data; goto end;}
            while(m < num)
            {
                x = 0;
                //check 
                if(nodes[i].nchilds > 0 && nodes[i].childs >= MMKEY_LINE_MAX)
                {
                    min = nodes[i].childs;
                    max = min + nodes[i].nchilds - 1;
                    if(key == nodes[min].key) x = min;
                    else if(key == nodes[max].key) x = max;
                    else if(key < nodes[min].key) goto end;
                    else if(key > nodes[max].key) goto end;
                    else
                    {
                        while(max > min)
                        {
                            z = (max + min)/2;
                            if(z == min){x = z;break;}
                            if(nodes[z].key == key){x = z;break;}
                            else if(nodes[z].key < key) min = z;
                            else max = z;
                        }
                        if(nodes[x].key != key) goto end;
                    }
                    i = x;
                }
                if(i < mmkey->state->total 
                        && (nodes[i].nchilds == 0 || (m+1) == num))
                {
                    if(nodes[i].key != key) goto end;
                    if(m+1 == num) ret = nodes[i].data;
                    break;
                }
                if(++m < num) key = list[m];
            }
        }
end:
        MUTEX_UNLOCK(mmkey->mutex);        
    }
    return ret;
}

/* delete */
int  mmkey_del(MMKEY *mmkey, unsigned short *list, int num)
{
    unsigned int ret = 0, x = 0, i = 0, z = 0, min = 0, max = 0, m = 0, key = 0;
    MKEYNODE *nodes = NULL;

    if(mmkey && list && num > 0)
    {
        MUTEX_LOCK(mmkey->mutex);        
        if((nodes = mmkey->nodes) && mmkey->map && mmkey->state)
        {
            key = list[m];i = key; if(++m < num) key = list[m];
            if(num == 1 && i < mmkey->state->total && nodes[i].data != 0){ret = nodes[i].data; nodes[i].data = 0; goto end;}
            while(m < num)
            {
                x = 0;
                //check 
                if(nodes[i].nchilds  > 0 && nodes[i].childs >= MMKEY_LINE_MAX) 
                {
                    min = nodes[i].childs;
                    max = min + nodes[i].nchilds - 1;
                    if(key == nodes[min].key) x = min;
                    else if(key == nodes[max].key) x = max;
                    else if(key < nodes[min].key) goto end;
                    else if(key > nodes[max].key) goto end;
                    else
                    {
                        while(max > min)
                        {
                            z = (max + min)/2;
                            if(z == min){x = z;break;}
                            if(nodes[z].key == key){x = z;break;}
                            else if(nodes[z].key < key) min = z;
                            else max = z;
                        }
                        if(nodes[x].key != key) goto end;
                    }
                    i = x;
                }
                if(i < mmkey->state->total 
                        && (nodes[i].nchilds == 0 || (m+1) == num))
                {
                    if(nodes[i].key != key) goto end;
                    if((m+1) == num) 
                    {
                        ret = nodes[i].data;
                        nodes[i].data = 0;
                    }
                    break;
                }
                if(++m < num) key = list[m];
            }
        }
end:
        MUTEX_UNLOCK(mmkey->mutex);        
    }
    
    return ret;
}

/* find/min */
int  mmkey_find(MMKEY *mmkey, unsigned short *list, int num, int *to)
{
    unsigned int ret = 0, x = 0, i = 0, z = 0, min = 0, max = 0, m = 0, key = 0;
    MKEYNODE *nodes = NULL;

    if(mmkey && list && num > 0)
    {
        *to = 0;
        MUTEX_LOCK(mmkey->mutex);        
        if((nodes = mmkey->nodes) && mmkey->map && mmkey->state)
        {
            key = list[m];i = key; if(++m < num) key = list[m];
            if((ret = nodes[i].data) != 0){*to = 1;goto end;}
            if(num == 1 && i < mmkey->state->total && nodes[i].data != 0){ret = nodes[i].data; *to = 1; goto end;}
            while(m < num)
            {
                x = 0;
                //check 
                if((ret = nodes[i].data) != 0){*to = (m+1);goto end;}
                else if(nodes[i].nchilds  > 0 && nodes[i].childs >= MMKEY_LINE_MAX) 
                {
                    min = nodes[i].childs;
                    max = min + nodes[i].nchilds - 1;
                    if(key == nodes[min].key) x = min;
                    else if(key == nodes[max].key) x = max;
                    else if(key < nodes[min].key) goto end;
                    else if(key > nodes[max].key) goto end;
                    else
                    {
                        while(max > min)
                        {
                            z = (max + min)/2;
                            if(z == min){x = z;break;}
                            if(nodes[z].key == key){x = z;break;}
                            else if(nodes[z].key < key) min = z;
                            else max = z;
                        }
                        if(nodes[x].key != key) goto end;
                    }
                    i = x;
                    if((ret = nodes[i].data) != 0){*to = m+1;goto end;}
                }
                else break; 
                if(++m < num) key = list[m];
            }
        }
end:
        MUTEX_UNLOCK(mmkey->mutex);        
    }
    return ret;
}

/* find/max */
int   mmkey_maxfind(MMKEY *mmkey, unsigned short *list, int num, int *to)
{
    unsigned int ret = 0, x = 0, i = 0, z = 0, min = 0, max = 0, m = 0, key = 0;
    MKEYNODE *nodes = NULL;

    if(mmkey && list && num > 0)
    {
        *to = 0;
        MUTEX_LOCK(mmkey->mutex);        
        if((nodes = mmkey->nodes) && mmkey->map && mmkey->state)
        {
            key = list[m];i = key; if(++m < num) key = list[m];
            if(nodes[i].data != 0){*to = 1;ret = nodes[i].data;}
            if(num == 1 && i < mmkey->state->total && nodes[i].data != 0){ret = nodes[i].data; *to = 1; goto end;}
            while(m < num)
            {
                x = 0;
                //check 
                if(nodes[i].nchilds  > 0 && nodes[i].childs >= MMKEY_LINE_MAX) 
                {
                    min = nodes[i].childs;
                    max = min + nodes[i].nchilds - 1;
                    if(key == nodes[min].key) x = min;
                    else if(key == nodes[max].key) x = max;
                    else if(key < nodes[min].key) goto end;
                    else if(key > nodes[max].key) goto end;
                    else
                    {
                        while(max > min)
                        {
                            z = (max + min)/2;
                            if(z == min){x = z;break;}
                            if(nodes[z].key == key){x = z;break;}
                            else if(nodes[z].key < key) min = z;
                            else max = z;
                        }
                        if(nodes[x].key != key) goto end;
                    }
                    i = x;
                    if(nodes[i].data != 0) 
                    {
                        ret = nodes[i].data;
                        *to = m + 1;
                    }
                }
                else break; 
                if(++m < num) key = list[m];
            }
        }
end:
        MUTEX_UNLOCK(mmkey->mutex);        
    }
    return ret;
}
/* add/reverse */
int   mmkey_radd(MMKEY *mmkey, unsigned short *list, int num, int data)
{
    unsigned int x = 0, i = 0, k = 0, j = 0, n = 0, pos = 0, 
        z = 0, min = 0, max = 0, m = 0, key = 0;
    MKEYNODE *nodes = NULL, *childs = NULL;
    int ret = -1;

    if(mmkey && list && num > 0)
    {
        MUTEX_LOCK(mmkey->mutex);        
        if((nodes = mmkey->nodes) && mmkey->map && mmkey->state)
        {
            key = list[--m]; i = key; key = list[--m];
            while(m >= 0)
            {
                x = 0;
                //check 
                if(nodes[i].nchilds  > 0 && nodes[i].childs >= MMKEY_LINE_MAX) 
                {
                    min = nodes[i].childs;
                    max = min + nodes[i].nchilds - 1;
                    if(key == nodes[min].key) x = min;
                    else if(key == nodes[max].key) x = max;
                    else if(key < nodes[min].key) x = -1;
                    else if(key > nodes[max].key) x = 1;
                    else
                    {
                        while(max > min)
                        {
                            z = (max + min)/2;
                            if(z == min) {x = z;break;}
                            if(nodes[z].key == key) {x = z;break;}
                            else if(nodes[z].key < key) min = z;
                            else max = z;
                        }
                    }
                }
                //new node
                if(x == -1 || x < MMKEY_LINE_MAX || nodes[x].key != key)
                {
                    n  = nodes[i].nchilds + 1;
                    z = nodes[i].childs;
                    MMKEY_POP(mmkey, n, pos);
                    if(pos < MMKEY_LINE_MAX) goto end;
                    childs = &(nodes[pos]);
                    if(x == 0)
                    {
                        MKEYNODE_SETK(childs[0], key);
                        j = pos;
                    }
                    else if(x == -1) 
                    {
                        MKEYNODE_SETK(childs[0], key);
                        k = 1;
                        while(k < n)
                        {
                            MKEYNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        j = pos;
                    }
                    else if(x == 1)
                    {
                        k = 0;
                        while(k < (n-1))
                        {
                            MKEYNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        MKEYNODE_SETK(childs[k], key);
                        j = pos + k ;
                    }
                    else
                    {
                        //0 1 3 4(6) 7 9 10
                        k = 0;
                        while(nodes[z].key < key)
                        {
                            MKEYNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        MKEYNODE_SETK(childs[k], key);
                        x = k++;
                        while(k < n)
                        {
                            MKEYNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        j =  pos + x;
                    }
                    MMKEY_PUSH(mmkey, nodes[i].nchilds, nodes[i].childs);
                    nodes[i].nchilds++;
                    nodes[i].childs = pos;
                    i = j;
                }
                else i = x;
                if((--m) >= 0) key = list[m];
            }
            if((ret = nodes[i].data) == 0)
                ret = nodes[i].data = data;
        }else ret = -4;
end:
        MUTEX_UNLOCK(mmkey->mutex);        
    }else ret = -5;
    return ret;
}

/* add/reverse /return auto increment id */
int   mmkey_rxadd(MMKEY *mmkey, unsigned short *list, int num)
{
    unsigned int x = 0, i = 0,j = 0, k = 0, n = 0, pos = 0, 
        z = 0, min = 0, max = 0, m = 0, key = 0;
    MKEYNODE *nodes = NULL, *childs = NULL;
    int ret = -1;

    if(mmkey && list && num > 0)
    {
        MUTEX_LOCK(mmkey->mutex);        
        if((nodes = mmkey->nodes) && mmkey->map && mmkey->state)
        {
            key = list[--m]; i = key; key = list[--m];
            while(m >= 0)
            {
                x = 0;
                //check 
                if(nodes[i].nchilds  > 0 && nodes[i].childs >= MMKEY_LINE_MAX) 
                {
                    min = nodes[i].childs;
                    max = min + nodes[i].nchilds - 1;
                    if(key == nodes[min].key) x = min;
                    else if(key == nodes[max].key) x = max;
                    else if(key < nodes[min].key) x = -1;
                    else if(key > nodes[max].key) x = 1;
                    else
                    {
                        while(max > min)
                        {
                            z = (max + min)/2;
                            if(z == min) {x = z;break;}
                            if(nodes[z].key == key) {x = z;break;}
                            else if(nodes[z].key < key) min = z;
                            else max = z;
                        }
                    }
                }
                //new node
                if(x == -1 || x < MMKEY_LINE_MAX || nodes[x].key != key)
                {
                    n  = nodes[i].nchilds + 1;
                    z = nodes[i].childs;
                    MMKEY_POP(mmkey, n, pos);
                    if(pos < MMKEY_LINE_MAX) goto end;
                    childs = &(nodes[pos]);
                    if(x == 0)
                    {
                        MKEYNODE_SETK(childs[0], key);
                        j = pos;
                    }
                    else if(x == -1) 
                    {
                        MKEYNODE_SETK(childs[0], key);
                        k = 1;
                        while(k < n)
                        {
                            MKEYNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        j = pos;
                    }
                    else if(x == 1)
                    {
                        k = 0;
                        while(k < (n-1))
                        {
                            MKEYNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        MKEYNODE_SETK(childs[k], key);
                        j = pos + k ;
                    }
                    else
                    {
                        //0 1 3 4(6) 7 9 10
                        k = 0;
                        while(nodes[z].key < key)
                        {
                            MKEYNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        MKEYNODE_SETK(childs[k], key);
                        x = k++;
                        while(k < n)
                        {
                            MKEYNODE_COPY(childs[k], nodes[z]);
                            ++z;
                            ++k;
                        }
                        j =  pos + x;
                    }
                    MMKEY_PUSH(mmkey, nodes[i].nchilds, nodes[i].childs);
                    nodes[i].nchilds++;
                    nodes[i].childs = pos;
                    i = j;
                }
                else i = x;
                if((--m) >= 0) key = list[m];
            }
            if((ret = nodes[i].data) == 0)
                ret =  nodes[i].data = ++(mmkey->state->id);
        }else ret = -4;
end:
        MUTEX_UNLOCK(mmkey->mutex);        
    }else ret = -5;
    return ret;
}

/* get/reverse */
int   mmkey_rget(MMKEY *mmkey, unsigned short *list, int num)
{
    unsigned int ret = 0, x = 0, i = 0, z = 0, min = 0, max = 0, m = 0, key = 0;
    MKEYNODE *nodes = NULL;

    if(mmkey && list && num > 0)
    {
        MUTEX_LOCK(mmkey->mutex);        
        if((nodes = mmkey->nodes) && mmkey->map && mmkey->state)
        {
            key = list[--m]; i = key; if(--m >= 0) key = list[m];
            if(num == 1 && i < mmkey->state->total){ret = nodes[i].data; goto end;}
            while(m >= 0)
            {
                x = 0;
                //check 
                if(nodes[i].nchilds > 0)
                {
                    min = nodes[i].childs;
                    max = min + nodes[i].nchilds - 1;
                    if(key == nodes[min].key) x = min;
                    else if(key == nodes[max].key) x = max;
                    else if(key < nodes[min].key) goto end;
                    else if(key > nodes[max].key) goto end;
                    else
                    {
                        while(max > min)
                        {
                            z = (max + min)/2;
                            if(z == min){x = z;break;}
                            if(nodes[z].key == key){x = z;break;}
                            else if(nodes[z].key < key) min = z;
                            else max = z;
                        }
                        if(nodes[x].key != key) goto end;
                    }
                    i = x;
                }
                if(i < mmkey->state->total 
                        && (nodes[i].nchilds == 0 || m == 0))
                {
                    if(nodes[i].key != key) goto end;
                    if(m == 0) ret = nodes[i].data;
                    break;
                }
                if((--m) >= 0) key = list[m];
            }
        }
end:
        MUTEX_UNLOCK(mmkey->mutex);        
    }
    return ret;
}

/* delete/reverse */
int   mmkey_rdel(MMKEY *mmkey, unsigned short *list, int num)
{
    unsigned int ret = 0, x = 0, i = 0, z = 0, min = 0, max = 0, m = 0, key = 0;
    MKEYNODE *nodes = NULL;

    if(mmkey && list && num > 0)
    {
        MUTEX_LOCK(mmkey->mutex);        
        if((nodes = mmkey->nodes) && mmkey->map && mmkey->state)
        {
            key = list[--m]; i = key; if(--m >= 0) key = list[m];
            if(num == 1 && i < mmkey->state->total &&  nodes[i].data != 0){ret = nodes[i].data; nodes[i].data = 0;goto end;}
            while(m >= 0)
            {
                x = 0;
                //check 
                if(nodes[i].nchilds  > 0 && nodes[i].childs >= MMKEY_LINE_MAX) 
                {
                    min = nodes[i].childs;
                    max = min + nodes[i].nchilds - 1;
                    if(key == nodes[min].key) x = min;
                    else if(key == nodes[max].key) x = max;
                    else if(key < nodes[min].key) goto end;
                    else if(key > nodes[max].key) goto end;
                    else
                    {
                        while(max > min)
                        {
                            z = (max + min)/2;
                            if(z == min){x = z;break;}
                            if(nodes[z].key == key){x = z;break;}
                            else if(nodes[z].key < key) min = z;
                            else max = z;
                        }
                        if(nodes[x].key != key) goto end;
                    }
                    i = x;
                }
                if(i < mmkey->state->total 
                        && (nodes[i].nchilds == 0 || m == 0))
                {
                    if(nodes[i].key != key) goto end;
                    if(m == 0) 
                    {
                        ret = nodes[i].data;
                        nodes[i].data = 0;
                    }
                    break;
                }
                if(--m >= 0) key = list[m];
            }
        }
end:
        MUTEX_UNLOCK(mmkey->mutex);        
    }
    return ret;
}

/* find/min/reverse */
int   mmkey_rfind(MMKEY *mmkey, unsigned short *list, int num, int *to)
{
    unsigned int ret = 0, x = 0, i = 0, z = 0, min = 0, max = 0, m = 0, key = 0;
    unsigned char *p = NULL, *ep = NULL;
    MKEYNODE *nodes = NULL;

    if(mmkey && list && num > 0)
    {
        *to = 0;
        MUTEX_LOCK(mmkey->mutex);        
        if((nodes = mmkey->nodes) && mmkey->map && mmkey->state)
        {
            key = list[--m]; i = key; if(--m >= 0) key = list[m];
            if((ret = nodes[i].data) != 0){*to = 1;goto end;}
            while(m >= 0)
            {
                x = 0;
                //check 
                if((ret = nodes[i].data) != 0)
                {
                    *to = num - m;
                    goto end;
                }
                else if(nodes[i].nchilds  > 0 && nodes[i].childs >= MMKEY_LINE_MAX) 
                {
                    min = nodes[i].childs;
                    max = min + nodes[i].nchilds - 1;
                    if(key == nodes[min].key) x = min;
                    else if(key == nodes[max].key) x = max;
                    else if(key < nodes[min].key) goto end;
                    else if(key > nodes[max].key) goto end;
                    else
                    {
                        while(max > min)
                        {
                            z = (max + min)/2;
                            if(z == min){x = z;break;}
                            if(nodes[z].key == key){x = z;break;}
                            else if(nodes[z].key < key) min = z;
                            else max = z;
                        }
                        if(nodes[x].key != key) goto end;
                    }
                    i = x;
                    if((ret = nodes[i].data) != 0)
                    {
                        *to = num - m;
                        goto end;
                    }
                }
                else break; 
                if(--m >= 0) key = list[m];
            }
        }
end:
        MUTEX_UNLOCK(mmkey->mutex);        
    }
    return ret;
}

/* find/max/reverse */
int   mmkey_rmaxfind(MMKEY *mmkey, unsigned short *list, int num, int *to)
{
    unsigned int ret = 0, x = 0, i = 0, z = 0, min = 0, max = 0, m = 0, key = 0;
    MKEYNODE *nodes = NULL;

    if(mmkey && list && num > 0)
    {
        *to = 0;
        MUTEX_LOCK(mmkey->mutex);        
        if((nodes = mmkey->nodes) && mmkey->map && mmkey->state)
        {
            key = list[--m]; i = key; if(--m >= 0) key = list[m];
            if(nodes[i].data != 0){*to = 1;ret = nodes[i].data;}
            if(num == 1 && i < mmkey->state->total){ret = nodes[i].data;*to = 1;goto end;}
            while(m >= 0)
            {
                x = 0;
                //check 
                if(nodes[i].nchilds  > 0 && nodes[i].childs >= MMKEY_LINE_MAX) 
                {
                    min = nodes[i].childs;
                    max = min + nodes[i].nchilds - 1;
                    if(key == nodes[min].key) x = min;
                    else if(key == nodes[max].key) x = max;
                    else if(key < nodes[min].key) goto end;
                    else if(key > nodes[max].key) goto end;
                    else
                    {
                        while(max > min)
                        {
                            z = (max + min)/2;
                            if(z == min){x = z;break;}
                            if(nodes[z].key == key){x = z;break;}
                            else if(nodes[z].key < key) min = z;
                            else max = z;
                        }
                        if(nodes[x].key != key) goto end;
                    }
                    i = x;
                    if(nodes[i].data != 0) 
                    {
                        ret = nodes[i].data;
                        *to = num - m;
                    }
                }
                else break; 
                if(--m >= 0) key = list[m];
            }
        }
end:
        MUTEX_UNLOCK(mmkey->mutex);        
    }
    return ret;
}

/* destroy */
void mmkey_destroy(MMKEY *mmkey)
{
    if(mmkey)
    {
        MUTEX_LOCK(mmkey->mutex);
        if(mmkey->map) 
        {
            munmap(mmkey->map, mmkey->file_size);
            mmkey->map = NULL;
        }
        if(mmkey->fd > 0) ftruncate(mmkey->fd, 0);
        mmkey->file_size = 0;
        MMKEY_MAP_INIT(mmkey); 
        MUTEX_UNLOCK(mmkey->mutex);
    }
    return ;
}

/* clean/reverse */
void  mmkey_clean(MMKEY *mmkey)
{
    if(mmkey)
    {
        MUTEX_DESTROY(mmkey->mutex);
        if(mmkey->map) 
        {
            munmap(mmkey->map, mmkey->file_size);
        }
        if(mmkey->fd > 0) close(mmkey->fd);
        free(mmkey);
    }
    return ;
}

/* initialize */
MMKEY *mmkey_init(char *file)
{
    MMKEY *mmkey = NULL;
    struct stat st = {0};
    int fd = 0;

    if(file && (fd = open(file, O_CREAT|O_RDWR, 0644)) > 0)
    {
        if((mmkey = (MMKEY *)calloc(1, sizeof(MMKEY))))
        {
            MUTEX_INIT(mmkey->mutex);
            mmkey->fd          = fd;
            fstat(fd, &st);
            mmkey->file_size   = st.st_size;
            MMKEY_MAP_INIT(mmkey);
            mmkey->add         = mmkey_add;
            mmkey->xadd        = mmkey_xadd;
            mmkey->get         = mmkey_get;
            mmkey->del         = mmkey_del;
            mmkey->find        = mmkey_find;
            mmkey->maxfind     = mmkey_maxfind;
            mmkey->radd        = mmkey_radd;
            mmkey->rxadd       = mmkey_rxadd;
            mmkey->rget        = mmkey_rget;
            mmkey->rdel        = mmkey_rdel;
            mmkey->rfind       = mmkey_rfind;
            mmkey->rmaxfind    = mmkey_rmaxfind;
            mmkey->clean       = mmkey_clean;
        }
        else 
            close(fd);
    }
    return mmkey;
}

#ifdef _DEBUG_MMKEY
#include "md5.h"
#include "timer.h"
#define FILE_LINE_MAX 65536
int main(int argc, char **argv)
{
    int i = 0, id = 0, n = 0, x = 0, count = 50000000;
    char line[FILE_LINE_MAX], *p = NULL;
    unsigned short *list = NULL;
    unsigned char digest[MD5_LEN];
    MMKEY *mmkey = NULL;
    void *timer = NULL;

    if((mmkey = mmkey_init("/tmp/test.mmkey")))
    {
        TIMER_INIT(timer);
        for(i = 1; i <= count; i++)
        {
            n = sprintf(line, "http://www.demo.com/%d.html", i);
            md5(line, n, digest);
            list = (unsigned short *)digest;
            id = mmkey_xadd(mmkey, list, 4);
            /*
            if(mmkey_get(mmkey, list, 4) != i)
            {
                fprintf(stderr, "NoFound key:%02X%02X%02X%02X id:%d\n", list[0],list[1],list[2],list[3], i);
                _exit(-1);
            }
            */
        }
        TIMER_SAMPLE(timer);
        fprintf(stdout, "%s::%d insert:%d time:%lld\n", __FILE__,__LINE__, count, PT_LU_USEC(timer));
        TIMER_CLEAN(timer);
        mmkey->clean(mmkey);
    }
}
//gcc -o mkey mmkey.c md5.c -D_DEBUG_MMKEY && ./mkey
#endif
