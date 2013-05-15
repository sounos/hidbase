#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/param.h> 
#include <sys/ioctl.h> 
#include <net/if.h> 
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <fcntl.h>
#include "logger.h"
#include "dbase.h"
#include "xmm.h"
#include "md5.h"
#include "timer.h"
#define LL(xxx) ((long long int)(xxx))
/* set ip port */
int dbase_set(DBASE *dbase, char const *host, int port)
{
    int ret = -1;

    if(dbase && host && port > 0)
    {
        memset(dbase, 0, sizeof(DBASE));
        dbase->rsa.sin_family = AF_INET;
        dbase->rsa.sin_addr.s_addr = inet_addr(host);
        dbase->rsa.sin_port = htons(port);
        TIMER_INIT(dbase->timer);
        ret = 0;
    }
    return ret;
}

/* resize dbae->res */
int dbase_res_resize(DBASE *dbase, int size)
{
    int ret = -1, len = 0;

    if(dbase)
    {
        if(dbase->res.size < size) len = ((size/DBASE_MM_BASE)+1) * DBASE_MM_BASE;
        if(len > 0) 
        {
            if((dbase->res.data = xmm_renew(dbase->res.data, dbase->res.size, len)))
            {
                dbase->res.size = len;
                ret = 0;
            }
        }
        else ret = 0;
    }
    return ret;
}

/* dbase reset */
int dbase_reset(DBASE *dbase)
{
    if(dbase)
    {
        if(dbase->fd > 0)
        {
            shutdown(dbase->fd, SHUT_RD|SHUT_WR);
            close(dbase->fd);
        }
        if(dbase->res.data) xmm_free(dbase->res.data, dbase->res.size);
        dbase->res.data = NULL;
        dbase->res.ndata = 0;
        dbase->fd = -1;
        return 0;
    }
    return -1;
}

/* connect to qtask */
int dbase_connect(DBASE *dbase)
{
    if(dbase)
    {
        if(dbase->fd <= 0 && (dbase->fd = socket(AF_INET, SOCK_STREAM, 0)) <= 0)
        {
            fprintf(stderr, "create socket() failed, %s\n", strerror(errno));
            return -2;
        }
        if(connect(dbase->fd, (struct sockaddr *)&(dbase->rsa), sizeof(struct sockaddr)) != 0)
        {
            fprintf(stderr, "connect() failed, %s\n", strerror(errno));
            close(dbase->fd);
            dbase->fd = -1;
            return -3;
        }
        return 0;
    }
    return -1;
}

/* set record  data
 * return value
 * -1 dbase is NULL
 * -2 connection is bad
 * -3 write() request failed
 * -4 recv() header failed
 * -5 malloc() for packet failed
 * -6 recv() data failed
 * -7 err_status 
 */
int dbase_set_record(DBASE *dbase, int64_t id, BJSON *record)
{
    int ret = 0, n = 0, port = 0;
    struct sockaddr_in lsa = {0};
    socklen_t lsa_len = sizeof(lsa);
    DBHEAD head = {0}, *xhead = NULL;
    char *ip = NULL;

    if(dbase && record  && (xhead = (DBHEAD *)(record->block))) 
    {
        TIMER_RESET(dbase->timer);
        if(dbase->fd <= 0 && dbase_connect(dbase) < 0)
        {
            ret = -2;goto err;
        }
        getsockname(dbase->fd, (struct sockaddr *)&lsa, &lsa_len);
        ip    = inet_ntoa(lsa.sin_addr);
        port  = ntohs(lsa.sin_port);
        xhead->id = id;
        xhead->cmd = DBASE_REQ_SET;
        xhead->length = record->current;
        DEBUG_LOGGER(dbase->logger, "Ready write request data %s:%d via %d", ip, port, dbase->fd)
        if(write(dbase->fd, record->block, xhead->length + sizeof(DBHEAD)) <= 0)
        {
            ERROR_LOGGER(dbase->logger, "write request %s:%d via %d failed, %s", ip, port, dbase->fd, strerror(errno));
            dbase_reset(dbase);
            ret = -3;goto err;
        }
        DEBUG_LOGGER(dbase->logger, "over write request data %s:%d via %d", ip, port, dbase->fd);
        if(recv(dbase->fd, &head, sizeof(DBHEAD), MSG_WAITALL) != sizeof(DBHEAD))
        {
            ERROR_LOGGER(dbase->logger, "recv header %s:%d via %d failed, %s", ip, port, dbase->fd, strerror(errno));
            dbase_reset(dbase);
            ret = -4;goto err;
        }
        DEBUG_LOGGER(dbase->logger, "over recv header %s:%d via %d", ip, port, dbase->fd);
        if(dbase_res_resize(dbase, head.length) != 0)
        {
            ERROR_LOGGER(dbase->logger, "renew(%d) data %s:%d via %d failed, %s", head.length, ip, port, dbase->fd, strerror(errno));
            dbase_reset(dbase);
            ret = -5;goto err;
        }
        DEBUG_LOGGER(dbase->logger, "over recv data %s:%d via %d", ip, port, dbase->fd);
        if(head.length > 0 && (n = recv(dbase->fd, dbase->res.data, head.length,
                        MSG_WAITALL)) != head.length)
        {
            ERROR_LOGGER(dbase->logger, "recv data %s:%d via %d failed, %s", ip, port, dbase->fd, strerror(errno));
            dbase_reset(dbase);
            ret = -6;goto err;
        }
        if(head.status == DBASE_STATUS_ERR) 
        {
            ERROR_LOGGER(dbase->logger, "err_status:%d id:%lld %s:%d via %d failed, %s", head.status, (long long)head.id, ip, port, dbase->fd, strerror(errno));
            ret = -7;goto err;
        }
        dbase->nwrite++;
        ret = 0;
err:
        TIMER_SAMPLE(dbase->timer);
        dbase->time_write += PT_LU_USEC(dbase->timer);
    }
    return ret;
}

/* update record  data
 * return value
 * -1 dbase is NULL
 * -2 connection is bad
 * -3 write() request failed
 * -4 recv() header failed
 * -5 malloc() for packet failed
 * -6 recv() data failed
 * -7 err_status 
 */
int dbase_update_record(DBASE *dbase, int64_t id, BJSON *record)
{
    int ret = 0, n = 0, port = 0;
    struct sockaddr_in lsa = {0};
    socklen_t lsa_len = sizeof(lsa);
    DBHEAD head = {0}, *xhead = NULL;
    char *ip = NULL;

    if(dbase && record  && (xhead = (DBHEAD *)(record->block))) 
    {
        TIMER_RESET(dbase->timer);
        if(dbase->fd <= 0 && dbase_connect(dbase) < 0)
        {
            ret = -2;goto err;
        }
        getsockname(dbase->fd, (struct sockaddr *)&lsa, &lsa_len);
        ip    = inet_ntoa(lsa.sin_addr);
        port  = ntohs(lsa.sin_port);
        xhead->id = id;
        xhead->cmd = DBASE_REQ_UPDATE;
        xhead->length = record->current;
        DEBUG_LOGGER(dbase->logger, "Ready write request data %s:%d via %d", ip, port, dbase->fd)
        if(write(dbase->fd, record->block, xhead->length + sizeof(DBHEAD)) <= 0)
        {
            ERROR_LOGGER(dbase->logger, "write request %s:%d via %d failed, %s", ip, port, dbase->fd, strerror(errno));
            dbase_reset(dbase);
            ret = -3;goto err;
        }
        DEBUG_LOGGER(dbase->logger, "over write request data %s:%d via %d", ip, port, dbase->fd);
        if(recv(dbase->fd, &head, sizeof(DBHEAD), MSG_WAITALL) != sizeof(DBHEAD))
        {
            ERROR_LOGGER(dbase->logger, "recv header %s:%d via %d failed, %s", ip, port, dbase->fd, strerror(errno));
            dbase_reset(dbase);
            ret = -4;goto err;
        }
        DEBUG_LOGGER(dbase->logger, "over recv header %s:%d via %d", ip, port, dbase->fd);
        if(dbase_res_resize(dbase, head.length) != 0)
        {
            ERROR_LOGGER(dbase->logger, "renew(%d) data %s:%d via %d failed, %s", head.length, ip, port, dbase->fd, strerror(errno));
            dbase_reset(dbase);
            ret = -5;goto err;
        }
        DEBUG_LOGGER(dbase->logger, "over recv data %s:%d via %d", ip, port, dbase->fd);
        if(head.length > 0 && (n = recv(dbase->fd, dbase->res.data, head.length,
                        MSG_WAITALL)) != head.length)
        {
            ERROR_LOGGER(dbase->logger, "recv data %s:%d via %d failed, %s", ip, port, dbase->fd, strerror(errno));
            dbase_reset(dbase);
            ret = -6;goto err;
        }
        if(head.status == DBASE_STATUS_ERR) 
        {
            ERROR_LOGGER(dbase->logger, "err_status:%d id:%lld %s:%d via %d failed, %s", head.status, (long long)head.id, ip, port, dbase->fd, strerror(errno));
            ret = -7;goto err;
        }
        dbase->nwrite++;
        ret = 0;
err:
        TIMER_SAMPLE(dbase->timer);
        dbase->time_write += PT_LU_USEC(dbase->timer);
    }
    return ret;
}

/* get record  data*/
BREC *dbase_get_record(DBASE *dbase, int64_t id)
{
    int ret = -1, n = 0, port = 0;
    struct sockaddr_in lsa = {0};
    socklen_t lsa_len = sizeof(lsa);
    BREC *record = NULL;
    DBHEAD head = {0};
    char *ip = NULL;

    if(dbase)
    {
        TIMER_RESET(dbase->timer);
        if(dbase->fd <= 0 && dbase_connect(dbase) < 0)
        {
            ret = -2;goto err;
        }
        getsockname(dbase->fd, (struct sockaddr *)&lsa, &lsa_len);
        ip    = inet_ntoa(lsa.sin_addr);
        port  = ntohs(lsa.sin_port);
        head.length = 0;
        head.id = id;
        head.cmd = DBASE_REQ_GET;
        DEBUG_LOGGER(dbase->logger, "Ready write request data %s:%d via %d", ip, port, dbase->fd)
            if(write(dbase->fd, &head, sizeof(DBHEAD)) <= 0)
            {
                ERROR_LOGGER(dbase->logger, "write request %s:%d via %d failed, %s", ip, port, dbase->fd, strerror(errno));
                dbase_reset(dbase);
                ret = -3;goto err;
            }
        DEBUG_LOGGER(dbase->logger, "over write request data %s:%d via %d", ip, port, dbase->fd);
        if(recv(dbase->fd, &head, sizeof(DBHEAD), MSG_WAITALL) != sizeof(DBHEAD))
        {
            ERROR_LOGGER(dbase->logger, "recv header %s:%d via %d failed, %s", ip, port, dbase->fd, strerror(errno));
            dbase_reset(dbase);
            ret = -4;goto err;
        }
        DEBUG_LOGGER(dbase->logger, "over recv header %s:%d via %d", ip, port, dbase->fd);
        if(dbase_res_resize(dbase, head.length) != 0)
        {
            ERROR_LOGGER(dbase->logger, "renew(%d) data %s:%d via %d failed, %s", head.length, ip, port, dbase->fd, strerror(errno));
            dbase_reset(dbase);
            ret = -5;goto err;
        }
        DEBUG_LOGGER(dbase->logger, "over recv data %s:%d via %d", ip, port, dbase->fd);
        if(head.length > 0 && (n = recv(dbase->fd, dbase->res.data, head.length,
                        MSG_WAITALL)) != head.length)
        {
            ERROR_LOGGER(dbase->logger, "recv data %s:%d via %d failed, %s", ip, port, dbase->fd, strerror(errno));
            dbase_reset(dbase);
            ret = -6;goto err;
        }
        if(head.status == DBASE_STATUS_ERR) 
        {
            ERROR_LOGGER(dbase->logger, "err_status:%d id:%lld %s:%d via %d failed, %s", head.status, (long long)head.id, ip, port, dbase->fd, strerror(errno));
            ret = -7;goto err;
        }
        dbase->nread++;
        ret = dbase->res.ndata = head.length;
        record = (BREC *)&(dbase->res);
err:    
        TIMER_SAMPLE(dbase->timer);
        dbase->time_read += PT_LU_USEC(dbase->timer);
    }
    return record;
}

/* del records  data*/
int dbase_del_records(DBASE *dbase, BJSON *request)
{
    struct sockaddr_in lsa = {0};
    socklen_t lsa_len = sizeof(lsa);
    DBHEAD head = {0}, *xhead = NULL;
    int ret = -1, port = 0;
    char *ip = NULL;

    if(dbase && request  && (xhead = (DBHEAD *)(request->block)))
    {
        TIMER_RESET(dbase->timer);
        if(dbase->fd <= 0 && dbase_connect(dbase) < 0)
        {
            ret = -2;goto err;
        }
        getsockname(dbase->fd, (struct sockaddr *)&lsa, &lsa_len);
        ip    = inet_ntoa(lsa.sin_addr);
        port  = ntohs(lsa.sin_port);
        xhead->length = request->current;
        xhead->cmd = DBASE_REQ_DELETE;
        DEBUG_LOGGER(dbase->logger, "Ready write request data %s:%d via %d", ip, port, dbase->fd)
        if(write(dbase->fd, request->block, xhead->length + sizeof(DBHEAD)) <= 0)
        {
            ERROR_LOGGER(dbase->logger, "write request %s:%d via %d failed, %s", ip, port, dbase->fd, strerror(errno));
            dbase_reset(dbase);
            ret = -3;goto err;
        }
        DEBUG_LOGGER(dbase->logger, "over write request data %s:%d via %d", ip, port, dbase->fd);
        if(recv(dbase->fd, &head, sizeof(DBHEAD), MSG_WAITALL) != sizeof(DBHEAD))
        {
            ERROR_LOGGER(dbase->logger, "recv header %s:%d via %d failed, %s", ip, port, dbase->fd, strerror(errno));
            dbase_reset(dbase);
            ret = -4;goto err;
        }
        if(head.status == DBASE_STATUS_ERR) 
        {
            ERROR_LOGGER(dbase->logger, "err_status:%d id:%lld %s:%d via %d failed, %s", head.status, (long long)head.id, ip, port, dbase->fd, strerror(errno));
            ret = -5;goto err;
        }
        else ret = 0;
err:
        TIMER_SAMPLE(dbase->timer);
        dbase->time_read += PT_LU_USEC(dbase->timer);
    }
    return ret;
}

/* get records  data*/
BRES *dbase_get_records(DBASE *dbase, BJSON *request)
{
    int ret = -1, n = 0, port = 0;
    struct sockaddr_in lsa = {0};
    socklen_t lsa_len = sizeof(lsa);
    DBHEAD head = {0}, *xhead = NULL;
    BRES *res = NULL;
    char *ip = NULL;

    if(dbase && request  && (xhead = (DBHEAD *)(request->block)))
    {
        TIMER_RESET(dbase->timer);
        if(dbase->fd <= 0 && dbase_connect(dbase) < 0)
        {
            ret = -2;goto err;
        }
        getsockname(dbase->fd, (struct sockaddr *)&lsa, &lsa_len);
        ip    = inet_ntoa(lsa.sin_addr);
        port  = ntohs(lsa.sin_port);
        xhead->length = request->current;
        xhead->cmd = DBASE_REQ_GET;
        DEBUG_LOGGER(dbase->logger, "Ready write request data %s:%d via %d", ip, port, dbase->fd)
        if(write(dbase->fd, request->block, xhead->length + sizeof(DBHEAD)) <= 0)
        {
            ERROR_LOGGER(dbase->logger, "write request %s:%d via %d failed, %s", ip, port, dbase->fd, strerror(errno));
            dbase_reset(dbase);
            ret = -3;goto err;
        }
        DEBUG_LOGGER(dbase->logger, "over write request data %s:%d via %d", ip, port, dbase->fd);
        if(recv(dbase->fd, &head, sizeof(DBHEAD), MSG_WAITALL) != sizeof(DBHEAD))
        {
            ERROR_LOGGER(dbase->logger, "recv header %s:%d via %d failed, %s", ip, port, dbase->fd, strerror(errno));
            dbase_reset(dbase);
            ret = -4;goto err;
        }
        DEBUG_LOGGER(dbase->logger, "over recv header %s:%d via %d", ip, port, dbase->fd);
        if(dbase_res_resize(dbase, head.length) != 0)
        {
            ERROR_LOGGER(dbase->logger, "renew(%d) data %s:%d via %d failed, %s", head.length, ip, port, dbase->fd, strerror(errno));
            dbase_reset(dbase);
            ret = -5;goto err;
        }
        DEBUG_LOGGER(dbase->logger, "over recv data %s:%d via %d", ip, port, dbase->fd);
        if(head.length > 0 && (n = recv(dbase->fd, dbase->res.data, head.length,
                        MSG_WAITALL)) != head.length)
        {
            ERROR_LOGGER(dbase->logger, "recv data %s:%d via %d failed, %s", ip, port, dbase->fd, strerror(errno));
            dbase_reset(dbase);
            ret = -6;goto err;
        }
        if(head.status == DBASE_STATUS_ERR) 
        {
            ERROR_LOGGER(dbase->logger, "err_status:%d id:%lld %s:%d via %d failed, %s", head.status, (long long)head.id, ip, port, dbase->fd, strerror(errno));
            ret = -7;goto err;
        }
        dbase->nread++;
        ret = dbase->res.ndata = head.length;
        res = &(dbase->res);

err:
        TIMER_SAMPLE(dbase->timer);
        dbase->time_read += PT_LU_USEC(dbase->timer);
    }
    return res;
}

/* update data
 * return value
 * -1 dbase is NULL
 * -2 connection is bad
 * -3 write() header failed
 * -4 write() data failed
 * -5 recv() header failed
 * -6 malloc() for packet failed
 * -7 recv() data failed
 * 
 */
int dbase_update(DBASE *dbase, int64_t id, const char *data, int ndata)
{
    int ret = 0, n = 0, port = 0;
    struct sockaddr_in lsa = {0};
    socklen_t lsa_len = sizeof(lsa);
    char *ip = NULL, *p = NULL;
    DBHEAD head = {0}, *xhead = NULL;

    if(dbase && data && ndata > 0)
    {
        if(dbase->fd <= 0 && dbase_connect(dbase) < 0)
        {
            return -2;
        }
        getsockname(dbase->fd, (struct sockaddr *)&lsa, &lsa_len);
        ip    = inet_ntoa(lsa.sin_addr);
        port  = ntohs(lsa.sin_port);
        n = sizeof(DBHEAD) + ndata;
        if((p = xmm_new(n)))
        {
            xhead = (DBHEAD *)p; 
            xhead->id = id;
            xhead->length = ndata;
            xhead->cmd = DBASE_REQ_UPDATE;
            memcpy(p + sizeof(DBHEAD), data, ndata);
            if(write(dbase->fd, p, n) < 0)
            {
                ERROR_LOGGER(dbase->logger, "write data %s:%d via %d failed, %s", ip, port, dbase->fd, strerror(errno));
                ret = -3;
            }
            xmm_free(p, n);
        }
        else
        {
            ERROR_LOGGER(dbase->logger, "malloc data[%d] %s:%d via %d failed, %s", n, ip, port, dbase->fd, strerror(errno));
            ret = -4;
        }
        if(ret < 0) return ret; 
        DEBUG_LOGGER(dbase->logger, "over write data %s:%d via %d", ip, port, dbase->fd);
        if(recv(dbase->fd, &head, sizeof(DBHEAD), MSG_WAITALL) != sizeof(DBHEAD))
        {
            ERROR_LOGGER(dbase->logger, "recv header %s:%d via %d failed, %s", ip, port, dbase->fd, strerror(errno));
            dbase_reset(dbase);
            return -5;
        }
        DEBUG_LOGGER(dbase->logger, "over recv header %s:%d via %d", ip, port, dbase->fd);
        if(dbase_res_resize(dbase, head.length) != 0)
        {
            ERROR_LOGGER(dbase->logger, "renew(%d) data %s:%d via %d failed, %s", head.length, ip, port, dbase->fd, strerror(errno));
            dbase_reset(dbase);
            return -6;
        }
        DEBUG_LOGGER(dbase->logger, "over recv data %s:%d via %d", ip, port, dbase->fd);
        if(head.length > 0 && (n = recv(dbase->fd, dbase->res.data, head.length,
                        MSG_WAITALL)) != head.length)
        {
            ERROR_LOGGER(dbase->logger, "recv data %s:%d via %d failed, %s", ip, port, dbase->fd, strerror(errno));
            dbase_reset(dbase);
            return -7;
        }
        if(head.status == DBASE_STATUS_ERR) 
        {
            ERROR_LOGGER(dbase->logger, "err_status:%d id:%lld %s:%d via %d failed, %s", head.status, (long long)head.id, ip, port, dbase->fd, strerror(errno));
            return -8;
        }
    }
    return -1;
}

/* get data
 * return value
 * -1 dbase is NULL
 * -2 connection is bad
 * -3 write() request failed
 * -4 xmm_new/malloc()  failed
 * -5 recv header failed
 * -6 malloc() data failed
 * -7 recv() data failed
 * 
 */
int dbase_get(DBASE *dbase, int64_t *idlist, int count)
{
    DBHEAD *xhead = NULL, head = {0};
    struct sockaddr_in lsa = {0};
    socklen_t lsa_len = sizeof(lsa);
    int ret = 0, n = 0, port = 0;
    char *p = NULL, *ip = NULL;

    if(dbase && idlist && count > 0)
    {
        if(dbase->fd <= 0 && dbase_connect(dbase) <= 0) 
            return -2;
        getsockname(dbase->fd, (struct sockaddr *)&lsa, &lsa_len);
        ip    = inet_ntoa(lsa.sin_addr);
        port  = ntohs(lsa.sin_port);
        n = sizeof(DBHEAD) + sizeof(int64_t) * count;
        if((p = xmm_new(n)))
        {
            xhead = (DBHEAD *)p; 
            xhead->id = 0;
            xhead->length = sizeof(int64_t) * count;
            xhead->cmd = DBASE_REQ_GET;
            memcpy(p + sizeof(DBHEAD), (char *)idlist, xhead->length);
            if(write(dbase->fd, p, n) < 0)
            {
                ERROR_LOGGER(dbase->logger, "write data %s:%d via %d failed, %s", ip, port, dbase->fd, strerror(errno));
                ret = -3;
            }
            xmm_free(p, n);
        }
        else
        {
            ERROR_LOGGER(dbase->logger, "malloc data[%d] %s:%d via %d failed, %s", n, ip, port, dbase->fd, strerror(errno));
            ret = -4;
        }
        if(ret < 0) return ret; 
        /* recv header */
        if(recv(dbase->fd, &head, sizeof(DBHEAD), MSG_WAITALL) != sizeof(DBHEAD))
        {
            ERROR_LOGGER(dbase->logger, "recv response header %s:%d via %d failed, %s", ip, port, dbase->fd, strerror(errno));
            dbase_reset(dbase);
            return -5;
        }
        /* malloc for packet */
        if(dbase_res_resize(dbase, head.length) != 0)
        {
            ERROR_LOGGER(dbase->logger, "malloc() data:%d %s:%d via %d failed, %s", head.length, ip, port, dbase->fd, strerror(errno));
            dbase_reset(dbase);
            return -6;
        }
        /* read packet */
        if(head.length > 0 && (n = recv(dbase->fd, dbase->res.data, head.length,
                        MSG_WAITALL)) != head.length)
        {
            ERROR_LOGGER(dbase->logger, "recv data:%d %s:%d via %d failed, %s", head.length, ip, port, dbase->fd, strerror(errno));
            dbase_reset(dbase);
            return -7;
        }
        dbase->res.ndata = head.length;
        ret = 0;
    }
    return -1;
}
/* dbase close */
int dbase_close(DBASE *dbase)
{
    if(dbase->fd > 0)
    {
        shutdown(dbase->fd, SHUT_RD|SHUT_WR);
        close(dbase->fd);
        dbase->fd = -1;
        if(dbase->res.data && dbase->res.size)
            xmm_free(dbase->res.data, dbase->res.size);
        TIMER_CLEAN(dbase->timer);
        return 0;
    }
    return -1;
}

/* record resize */
int bjson_resize(BJSON *record, int incre)
{
    int ret = -1, n = 0, len = 0, nhead = 0;
    char *block = NULL;

    if(record)
    {
        if((record->ndata - record->current) < incre)
        {
            nhead = (record->data - record->block);
            n = nhead + record->current + incre;
            if(n > (record->nblock * 2)) len = ((n/BJSON_BASE)+1) * BJSON_BASE;
            else len = record->nblock * 2;
            if((block = xmm_resize(record->block, record->nblock, len)))
            {
                record->block = block;
                record->nblock = len;
                record->data = record->block + nhead;
                record->ndata = record->nblock - nhead;
                ret = 0;
            }
        }
        else ret = 0;
    }
    return ret;
}

/* check add root */
int bjson_start(BJSON *record)
{
    BELEMENT *element = NULL;
    char *p = NULL;

    if(record && record->current == 0) 
    {
        if(bjson_resize(record, sizeof(BELEMENT) + 1) == 0 && record->data)
        {
            p = record->data;
            element = (BELEMENT *)p;
            element->nchilds = 0;
            element->length  = 0;
            element->flag = BJSON_TYPE_OBJECT;
            p += sizeof(BELEMENT);
            *p++ = '\0';
            record->current = p - record->data;
        }
    }
    return 0;
}

/* record */
int bjson_reset(BJSON *record)
{
    int ret = -1;

    if(record)
    {
        if(record->block == NULL)
        {
            record->block = xmm_new(BJSON_BASE);
            record->nblock = BJSON_BASE;
        }
        record->data = record->block;
        record->ndata = record->nblock;
        record->deep = 0;
        record->current = 0;
    }
    return ret;
}

/* bjson new object */
int bjson_new_object(BJSON *record, const char *name)
{
    BELEMENT *element = NULL, *parent = NULL;
    int ret = -1, n = 0;
    char *p = NULL;

    if(record)
    {
        if(name) n = strlen(name);
        if(bjson_resize(record, sizeof(BELEMENT) + n + 1) == 0 && record->data)
        {
            parent = (BELEMENT *)(record->data + record->stacks[record->deep]);
            parent->nchilds++;
            record->stacks[++(record->deep)] = record->current;
            p = record->data + record->current;
            element = (BELEMENT *)p;
            element->nchilds = 0;
            element->length  = 0;
            element->flag = BJSON_TYPE_OBJECT;
            p += sizeof(BELEMENT);
            if(name){p += sprintf(p, "%s", name);}
            *p++ = '\0';
            record->current = p - record->data;
            ret = 0;
        }
    }
    return ret;
}

/* over object */
int bjson_finish_object(BJSON *record)
{
    BELEMENT *element = NULL;
    int ret = -1;

    if(record)
    {
        if(record->deep > 0)
        {
            element = (BELEMENT *)(record->data + record->stacks[record->deep]);
            element->length = record->current - record->stacks[record->deep];
            --(record->deep);
            //fprintf(stdout, "%s::%d deep:%d\n", __FILE__, __LINE__, record->deep);
            ret = 0;
        }
    }
    return ret;
}

/* BJSON To BREC */
void  bjson_to_record(BJSON *bjson, BREC *record)
{
    if(bjson && record)
    {
        record->data = bjson->data;
        record->ndata = bjson->current;
    }
    return ;
}
/* initialize request  */
int brequest_reset(BJSON *request)
{
    int ret = -1;

    if(request)
    {
        if(request->block == NULL)
        {
            request->block = xmm_new(BJSON_BASE);
            request->nblock = BJSON_BASE;
        }
        request->data = request->block + sizeof(DBHEAD);
        request->ndata = request->nblock - sizeof(DBHEAD);
        request->current = 0;
        request->deep = 0;
    }
    return ret;
}

/* bjson finish */
int brequest_finish(BJSON *request)
{
    DBHEAD *head = NULL;

    if(request && (head = (DBHEAD *)request->block))
    {
        head->length = request->current;
        return 0;
    }
    return -1;
}

/* request clean */
void brequest_clean(BJSON *request)
{
    if(request)
    {
        if(request->block && request->nblock > 0)
        {
            xmm_free(request->block, request->nblock);
            memset(request, 0, sizeof(BJSON));
        }
    }
    return ;
}

/* append to request key */
int brequest_append_key(BJSON *request, int64_t key)
{
    int64_t *x = NULL;

    if(request)
    {
        if(bjson_resize(request, sizeof(int64_t)) == 0 && request->data)
        {
            x = (int64_t *)(request->data + request->current);
            *x = key;
            request->current += sizeof(int64_t);
            return 0;
        }
    }
    return -1;
}

/* append to keys */
int brequest_append_keys(BJSON *request, int64_t *keys, int count)
{
    int64_t *x = NULL;
    int i = 0, ret = -1;

    if(request && keys && count > 0)
    {
        for(i = 0; i < count; i++)
        {
            if(bjson_resize(request, sizeof(int64_t)) == 0 && request->data)
            {
                x = (int64_t *)(request->data + request->current);
                *x = keys[i];
                request->current += sizeof(int64_t);
                ret = 0;
            }
            else break;
        }
    }
    return ret;
}


/* append int element */
int bjson_append_int(BJSON *record, const char *name, int v)
{
    BELEMENT *element = NULL, *parent = NULL;
    int ret = -1, n = 0;
    char *p = NULL, *base = NULL;

    if(record)
    {
        if(name) n = strlen(name); 
        if(bjson_resize(record, sizeof(BELEMENT) + n + 1 + sizeof(int)) == 0
                && record->data)
        {
            parent = (BELEMENT *)(record->data + record->stacks[record->deep]);
            parent->nchilds++;
            base = p = record->data + record->current;
            element = (BELEMENT *)p; 
            element->nchilds = 0;
            element->flag = BJSON_TYPE_INT;
            p += sizeof(BELEMENT); 
            if(name) strcpy(p, name);
            p += n;*p++ = '\0';
            *((int *)p) = v;
            p += sizeof(int);
            element->length = p - (char *)element; 
            ret = record->current = p - record->data;
        }
    }
    return ret;
}

/* append long element */
int bjson_append_long(BJSON *record, const char *name, int64_t v)
{
    BELEMENT *element = NULL, *parent = NULL;
    int ret = -1, n = 0;
    char *p = NULL;

    if(record)
    {
        if(name) n = strlen(name); 
        if(bjson_resize(record, sizeof(BELEMENT) + n + 1 + sizeof(int64_t)) == 0
                && record->data)
        {
            parent = (BELEMENT *)(record->data + record->stacks[record->deep]);
            parent->nchilds++;
            p = record->data + record->current;
            element = (BELEMENT *)p; 
            element->nchilds = 0;
            element->flag = BJSON_TYPE_LONG;
            p += sizeof(BELEMENT); 
            if(name) strcpy(p, name);
            p += n;*p++ = '\0';
            *((int64_t *)p) = v;
            p += sizeof(int64_t);
            element->length = p - (char *)element; 
            ret = record->current = p - record->data;
        }
    }
    return ret;
}

/* append double element */
int bjson_append_double(BJSON *record, const char *name, double v)
{
    BELEMENT *element = NULL, *parent = NULL;
    int ret = -1, n = 0;
    char *p = NULL;

    if(record)
    {
        if(name) n = strlen(name); 
        if(bjson_resize(record, sizeof(BELEMENT) + n + 1 + sizeof(double)) == 0
                && record->data)
        {
            parent = (BELEMENT *)(record->data + record->stacks[record->deep]);
            parent->nchilds++;
            p = record->data + record->current;
            element = (BELEMENT *)p; 
            element->nchilds = 0;
            element->flag = BJSON_TYPE_DOUBLE;
            p += sizeof(BELEMENT); 
            if(name) strcpy(p, name);
            p += n;*p++ = '\0';
            *((double *)p) = v;
            p += sizeof(double);
            element->length = p - (char *)element; 
            ret = record->current = p - record->data;
        }
    }
    return ret;
}

/* append string element */
int bjson_append_string(BJSON *record, const char *name, const char *v)
{
    BELEMENT *element = NULL, *parent = NULL;
    int ret = -1, n = 0, ndata = 0;
    char *p = NULL;

    if(record)
    {
        if(name) n = strlen(name); 
        if(v) ndata = strlen(v);
        if(bjson_resize(record, sizeof(BELEMENT) + n + 1 + ndata + 1) == 0
                && record->data)
        {
            parent = (BELEMENT *)(record->data + record->stacks[record->deep]);
            parent->nchilds++;
            p = record->data + record->current;
            element = (BELEMENT *)p; 
            element->nchilds = 0;
            element->flag = BJSON_TYPE_STRING;
            p += sizeof(BELEMENT); 
            if(name) strcpy(p, name);
            p += n;*p++ = '\0';
            if(v) strcpy(p, v);
            p += ndata;*p++ = '\0';
            element->length = p - (char *)element; 
            ret = record->current = p - record->data;
        }
    }
    return ret;
}

/* append blob element */
int bjson_append_blob(BJSON *record, const char *name, void *data, int ndata)
{
    BELEMENT *element = NULL, *parent = NULL;
    int ret = -1, n = 0;
    char *p = NULL;

    if(record)
    {
        if(name) n = strlen(name); 
        if(bjson_resize(record, sizeof(BELEMENT) + n + 1 + ndata + 1) == 0
                && record->data)
        {
            parent = (BELEMENT *)(record->data + record->stacks[record->deep]);
            parent->nchilds++;
            p = record->data + record->current;
            element = (BELEMENT *)p; 
            element->nchilds = 0;
            element->flag = BJSON_TYPE_BLOB;
            p += sizeof(BELEMENT); 
            if(name) strcpy(p, name);
            p += n;*p++ = '\0';
            if(data && ndata > 0) memcpy(p, data, ndata);
            p += ndata;
            element->length = p - (char *)element; 
            ret = record->current = p - record->data;
        }
    }
    return ret;
}

/* append element */
int bjson_append_element(BJSON *record, BELEMENT *e)
{
    BELEMENT *e1 = NULL, *parent = NULL;
    char *p = NULL;
    int ret = -1;

    if(record && e && e->length > 0)
    {
        if(bjson_resize(record,  e->length) == 0 && record->data)
        {
            parent = (BELEMENT *)(record->data + record->stacks[record->deep]);
            parent->nchilds++;
            p = record->data + record->current;
            e1 = (BELEMENT *)p;
            memcpy(p, e, e->length);
            p += e->length;
            e1->flag &= BJSON_TYPE_ALL;
            ret = record->current = p - record->data;
        }
    }
    return ret;
}

/* bjson finish */
int bjson_finish(BJSON *record)
{
    BELEMENT *element = NULL;

    if(record && (element = (BELEMENT *)record->data))
    {
        element->length = record->current;
        return 0;
    }
    return -1;
}

/* bjson json element */
int bjson_json_element(char *base, int nchilds, char *out)
{
    char *pp = NULL, *p = NULL, *s = NULL, *es = NULL;
    BELEMENT *element = NULL;
    int i = 0, n = 0;

    if((pp = base) && (p = out) && nchilds > 0)
    {
        while(i < nchilds)
        {
            element = (BELEMENT *)pp;
            pp += sizeof(BELEMENT);
            if(*pp != 0)
            {
                *p++ = '"';
                while(*pp != 0)
                {
                    if(*pp == '\r' || *pp == '\n' || *pp == '\0' || *pp == '\t')++pp;
                    else if(*pp == '\''){p += sprintf(p, "&#39;");++pp;}
                    else if(*pp == '"'){p += sprintf(p, "&#34;");++pp;}
                    else *p++ = *pp++;       
                }
                p += sprintf(p, "\":");
            }
            ++pp;
            if(element->nchilds > 0)
            {
                *p++ = '{';
                p += bjson_json_element(pp, element->nchilds, p);
                //fprintf(stdout, "%s::%d nchilds:%d\n", __FILE__, __LINE__, element->nchilds);
                *p++ = '}';
                *p++ = ',';
            }
            else
            {
                if(element->flag & BJSON_TYPE_INT)
                    p += sprintf(p, "\"%d\",", *((int *)pp));
                else if(element->flag & BJSON_TYPE_LONG)
                    p += sprintf(p, "\"%lld\",", *((long long *)pp));
                else if(element->flag & BJSON_TYPE_DOUBLE)
                    p += sprintf(p, "\"%f\",", *((double *)pp));
                else if(element->flag & BJSON_TYPE_STRING)
                {
                    p += sprintf(p, "\"%s\",", pp);
                }
                else if(element->flag & BJSON_TYPE_BLOB)
                {
                    *p++ = '\"';
                    s = pp;es = s + element->length;
                    while(s < es) 
                    {
                        p += sprintf(p, "%02x", *((unsigned char *)s));
                        ++s;
                    }
                    *p++ = '\"';
                    *p++ = ',';
                }
            }
            pp = (char *)element + element->length;
            ++i;
        }
        n = --p - out;
    }
    return n;
}

/* bjson json */
int bjson_json(BREC *record, const char *out)
{
    char *p = NULL, *pp = NULL;
    BELEMENT *element = NULL;
    int n = 0;

    if(record && (p = (char *)out))
    {
        p += sprintf(p, "({");
        element = (BELEMENT *)(record->data); 
        pp = record->data + sizeof(BELEMENT) + 1;
        p += bjson_json_element(pp, element->nchilds, p);
        *p++ = '}';
        *p++ = ')';
        *p = '\0';
         n = p - out;
    }
    return n;
}

/* bjson to json */
int bjson_to_json(BREC *record, const char *out)
{
    char *p = NULL, *pp = NULL;
    BELEMENT *element = NULL;
    int n = 0;

    if(record && (p = (char *)out))
    {
        p += sprintf(p, "{");
        element = (BELEMENT *)(record->data); 
        pp = record->data + sizeof(BELEMENT) + 1;
        p += bjson_json_element(pp, element->nchilds, p);
        *p++ = '}';
        *p = '\0';
         n = p - out;
    }
    return n;
}



/* rec to element */
BELEMENT *belement(BREC *record)
{
    BELEMENT *e = NULL;

    if(record && record->ndata > sizeof(BELEMENT))
    {
        e = (BELEMENT *)(record->data);
    }
    return e;
}

/* BJSON to element */
BELEMENT *bjson_root(BJSON *record)
{
    BELEMENT *e = NULL;

    if(record && record->ndata > sizeof(BELEMENT))
    {
        e = (BELEMENT *)(record->data);
    }
    return e;
}

/* get next record */
BELEMENT *dbase_next_record(BRES *res, BELEMENT *cur, int64_t *key)
{
    BELEMENT *e = NULL;
    DBMETA *m = NULL;
    char *end = NULL;

    if(res && res->data && res->ndata > 0 && (end = (res->data + res->ndata)))
    {
        if(cur && (char *)cur >= res->data && (char *)cur < end) 
            m = (DBMETA *)((char *)cur + cur->length);  
        else 
            m = (DBMETA *)(res->data);

        if(m && (char *)m >= res->data && (char *)m < end)
        {
            if(key) *key = m->id;
            if(m->length >= sizeof(BELEMENT))
                e = (BELEMENT *)((char *)m + sizeof(DBMETA));
        }
    }
    return e;
}

/* get next meta */
DBMETA *dbase_next_meta(BRES *res, DBMETA *meta)
{
    DBMETA *m = NULL, *new = NULL;
    char *end = NULL;

    if(res && res->data && res->ndata > 0 && (end = (res->data + res->ndata)))
    {
        if(meta && (char *)meta >= res->data && (char *)meta < end) 
            m = (DBMETA *)((char *)meta + sizeof(DBMETA) + meta->length);  
        else 
            m = (DBMETA *)(res->data);

        if(m && (char *)m >= res->data && (char *)m < end)
        {
            new = m;
        }
    }
    return new;
}

/* return meta record */
BELEMENT *dbase_meta_element(DBMETA *meta)
{
    BELEMENT *e = NULL;

    if(meta && meta->length > sizeof(BELEMENT))
    {
        e = (BELEMENT *)((char *)meta + sizeof(DBMETA));
    }
    return e;
}

/* return childs element */
BELEMENT *belement_childs(BELEMENT *e)
{
    char *p = NULL, *end = NULL;
    BELEMENT *childs = NULL;

    if(e && e->nchilds > 0 && (p = (char *)e + sizeof(BELEMENT))
            && (end = ((char *)e + e->length)) && p < end)
    {
       while(p < end && *p != '\0')++p;
       if(*p == '\0')
       {
           ++p;
           childs = (BELEMENT *)p;
       }
    }
    return childs;
}

/* return next element */
BELEMENT *belement_next(BELEMENT *parent, BELEMENT *e)
{
    BELEMENT *next = NULL;
    char *p = NULL;

    if(parent && e &&  (p  = ((char *)e + e->length)) 
            && (p < ((char *)parent + parent->length)))
    {
        next = (BELEMENT *)p;
    }
    return next;
}

/* return element name */
char *belement_key(BELEMENT *e)
{
    char *key = NULL;

    if(e)
    {
        key = (char *)e + sizeof(BELEMENT);
    }
    return key;    
}

/* find element with name */
BELEMENT *belement_find(BELEMENT *e, const char *key)
{
    BELEMENT *x = NULL, *k = NULL;
    char *p = NULL, *end = NULL;

    if(e && key && strlen(key) > 0 && (p = (char *)e + sizeof(BELEMENT))
            && (end = (char *)e + e->length) && p < end)
    {
        while(p < end && *p != '\0')++p;
        if(*p == '\0')
        {
            ++p;
            while(p < end && (k = (BELEMENT *)p))
            {
                p += sizeof(BELEMENT);
                if(p < end && strcasecmp(key, p) == 0){ x = k;break;}
                p = (char *)k + k->length;
            }
        }
    }
    return x;
}

/* get element with NO */
BELEMENT *belement_get(BELEMENT *e, int no)
{
    BELEMENT *x = NULL, *k = NULL;
    char *p = NULL, *end = NULL;
    int i = 0;

    if(e && no >= 0 && no < e->nchilds && (p = (char *)e + sizeof(BELEMENT))
            && (end =  (char *)e + e->length) && p < end)
    {
        while(p < end && *p != '\0')++p;
        if(*p == '\0')
        {
            ++p;
            while(p < end && i <= no && (k = (BELEMENT *)p)) 
            {
                p += k->length; 
                ++i;
            }
            if(i == no) x = k;
        }
    }
    return x;
}

/* get value  */
int belement_v(BELEMENT *e, void **v)
{
    char *p = NULL, *end = NULL;
    int ret = -1;

    if(e && v && (p = ((char *)e + sizeof(BELEMENT))) 
        && (end = ((char *)e + e->length)) && p < end)
    {
        while(p < end && *p != '\0')++p;
        if(*p == '\0')
        {
            ++p;
            *v = p;
            ret = end - p;
        }
    }
    return ret;
}

/* get type  */
int belement_type(BELEMENT *e)
{
    int ret = 0;

    if(e)
    {
        ret = (e->flag & (BJSON_TYPE_INT|BJSON_TYPE_LONG|BJSON_TYPE_DOUBLE
                    |BJSON_TYPE_STRING | BJSON_TYPE_BLOB | BJSON_TYPE_OBJECT));
    }
    return ret;
}

/* get value int */
int belement_v_int(BELEMENT *e, int *v)
{
    char *p = NULL, *end = NULL;
    int ret = -1, n = 0;

    if(e && v && (p = ((char *)e + sizeof(BELEMENT))) 
        && (end = ((char *)e + e->length)) && p < end)
    {
        while(p < end && *p != '\0')++p;
        if(*p == '\0')
        {
            ++p;
            n = end - p;
            if((e->flag & BJSON_TYPE_INT) && (n == sizeof(int)))
            {
                *v = *((int *)p);
                ret = 0;
            }
        }
    }
    return ret;
}

/* get value long */
int belement_v_long(BELEMENT *e, int64_t *v)
{
    char *p = NULL, *end = NULL;
    int ret = -1, n = 0;

    if(e && v && (p = ((char *)e + sizeof(BELEMENT))) 
        && (end = ((char *)e + e->length)) && p < end)
    {
        while(p < end && *p != '\0')++p;
        if(*p == '\0')
        {
            ++p;
            n = end - p;
            if((e->flag & BJSON_TYPE_LONG) && (n == sizeof(int64_t)))
            {
                *v = *((int64_t *)p);
                ret = 0;
            }
        }
    }
    return ret;
}

/* get value double */
int belement_v_double(BELEMENT *e, double *v)
{
    char *p = NULL, *end = NULL;
    int ret = -1, n = 0;

    if(e && v && (p = ((char *)e + sizeof(BELEMENT))) 
        && (end = ((char *)e + e->length)) && p < end)
    {
        while(p < end && *p != '\0')++p;
        if(*p == '\0')
        {
            ++p;
            n = end - p;
            if((e->flag & BJSON_TYPE_DOUBLE) && (n == sizeof(double)))
            {
                *v = *((double *)p);
                ret = 0;
            }
        }
    }
    return ret;
}

/* get value string */
char *belement_v_string(BELEMENT *e)
{
    char *p = NULL, *end = NULL, *v = NULL;
    int n = 0;

    if(e && (p = ((char *)e + sizeof(BELEMENT)))
        && (end = ((char *)e + e->length)) && p < end)
    {
        while(p < end && *p != '\0')++p;
        if(*p == '\0')
        {
            ++p;
            n = end - p;
            if((e->flag & BJSON_TYPE_STRING) && n > 0)
            {
                v = p;
            }
        }
    }
    return v;
}

/* get value blob */
int belement_v_blob(BELEMENT *e, char **v)
{
    char *p = NULL, *end = NULL;
    int n = 0, ret = -1;

    if(e && v && (p = ((char *)e + sizeof(BELEMENT))) 
        && (end = ((char *)e + e->length)) && p < end)
    {
        while(p < end && *p != '\0')++p;
        if(*p == '\0')
        {
            ++p;
            n = end - p;
            if((e->flag & BJSON_TYPE_BLOB) && n > 0)
            {
                *v = p;
                ret = n;
            }
        }
    }
    return ret;
}

int bjson_merge_element(BJSON *one, BELEMENT *old, BELEMENT *batch)
{
    BELEMENT *e = NULL, *e1 = NULL, *e2 = NULL;

    if(one && old)
    {
        if(old->nchilds > 0 && (e1 = belement_childs(old)))
        {
            bjson_new_object(one, belement_key(old)); 
            do
            {
                e2 = belement_find(batch, belement_key(e1));
                /* merge object */
                if(e1->nchilds > 0)
                {
                    if(e2 && e2->nchilds > 0) 
                    {
                        e2->flag |= BJSON_PASS;
                        bjson_merge_element(one, e1, e2);
                    }
                    else
                    {
                       bjson_append_element(one, e1); 
                    }
                }
                else
                {
                    e = NULL;
                    if(e2 && ((e1->flag & e2->flag) & BJSON_TYPE_ALL))
                    {
                        e2->flag |= BJSON_PASS;
                        e = e2;
                    }
                    else 
                        e = e1;
                    bjson_append_element(one, e);
                }
            }while((e1 = belement_next(old, e1)));
            /* append batch left */
            if((e = belement_childs(batch)))
            {
                do
                {
                    if(!(e->flag & BJSON_PASS)) 
                        bjson_append_element(one, e);
                }while((e = belement_next(batch, e)));
            }
            bjson_finish_object(one);
        }
        else
        {
            bjson_append_element(one, old);
        }
        return 0;
    }
    return -1;
}

/* clean bjson */
void bjson_clean(BJSON *record)
{
    if(record)
    {
        if(record->block && record->nblock > 0)
        {
            xmm_free(record->block, record->nblock);
            memset(record, 0, sizeof(BJSON));
        }
    }
    return ;
}

/* dbio */
int dbio_set(DBIO *dbio, char *multicast_network, int port)
{
    socklen_t lsa_len = sizeof(struct sockaddr);
    unsigned char ttl = DBASE_TTL_MAX;
    int i = 0, fd = 0, opt = 1;
    struct timeval tv = {0,0};
    struct sockaddr_in lsa;
    char ip[DBASE_IP_MAX];

    if(dbio && multicast_network && port > 0)
    {
        memset(dbio, 0, sizeof(DBIO));
        strcpy(dbio->multicast_network, multicast_network);
        tv.tv_sec = 0;tv.tv_usec = DBASE_TIMEOUT;
        for(i = 0; i < DBASE_MASK; i++)
        {
            if((fd = dbio->casts[i].rfd = socket(AF_INET, SOCK_DGRAM, 0)) > 0)
            {
                setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char *)&opt, (socklen_t) sizeof(int));
                setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (char *)&tv, (socklen_t)sizeof(tv));
#ifdef SO_REUSEPORT
                setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, (char *)&opt, (socklen_t) sizeof(int));
#endif
                memset(&lsa, 0, sizeof(struct sockaddr));
                lsa.sin_family = AF_INET;
                if(bind(fd, (struct sockaddr *)&lsa, sizeof(struct sockaddr)) != 0)
                {
                    FATAL_LOGGER(dbio->logger, "Bind fd:%d on local[%s:%d] failed, %s", fd, inet_ntoa(lsa.sin_addr), ntohs(lsa.sin_port), strerror(errno));
                    goto err;
                }
                getsockname(fd, (struct sockaddr *)&lsa, &lsa_len);
            }
            else goto err;
            if((fd = dbio->casts[i].fd = socket(AF_INET, SOCK_DGRAM, 0)) > 0)
            {
                setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char *)&opt, (socklen_t) sizeof(int));
#ifdef SO_REUSEPORT
                setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, (char *)&opt, (socklen_t) sizeof(int));
#endif
                sprintf(ip, "%s.%d", multicast_network, i);
                memset(&(dbio->casts[i].mask_addr), 0, sizeof(struct sockaddr));
                dbio->casts[i].mask_addr.sin_family = AF_INET;
                dbio->casts[i].mask_addr.sin_addr.s_addr = inet_addr(ip);
                dbio->casts[i].mask_addr.sin_port =  htons(port);
                if(bind(fd, (struct sockaddr *)&lsa, sizeof(struct sockaddr)) != 0)
                {
                    FATAL_LOGGER(dbio->logger, "Bind fd:%d on local[%s:%d] failed, %s", fd, inet_ntoa(lsa.sin_addr), ntohs(lsa.sin_port), strerror(errno));
                    goto err;
                }
                if(connect(fd, (struct sockaddr *)&(dbio->casts[i].mask_addr), sizeof(struct sockaddr)) != 0)
                {
                    FATAL_LOGGER(dbio->logger, "Connect to %s:%d  local[%s:%d] failed, %s", ip, port, inet_ntoa(lsa.sin_addr), ntohs(lsa.sin_port), strerror(errno));
                    goto err;
                }
                getsockname(fd, (struct sockaddr *)&(dbio->casts[i].local_addr), &lsa_len);
                setsockopt(fd, IPPROTO_IP, IP_MULTICAST_IF, (struct sockaddr *)&(dbio->casts[i].local_addr), sizeof(struct sockaddr));
                setsockopt(fd, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl));
                //fprintf(stdout, "Connect to %s:%d  local[%s:%d]\n", ip, port, inet_ntoa(lsa.sin_addr), ntohs(lsa.sin_port));
            }
            else goto err;
        }
        return 0;
    }
err:
    return -1;
}

/* check connection  */
int dbio_is_connected(DBIO *dbio, int no)
{
    struct sockaddr_in rsa = {0};
    unsigned char *ch = NULL;
    struct timeval tv = {0,0};
    int ret = -1;

    if(dbio && no > 0 && no < DBASE_NODES_MAX && dbio->nodes[no].ip 
            && (ret = dbio->nodes[no].fd) <= 0)
    {
        ch = (unsigned char *)&(dbio->nodes[no].ip);
        if((dbio->nodes[no].fd = socket(AF_INET, SOCK_STREAM, 0)) <= 0)
        {
            FATAL_LOGGER(dbio->logger, "create socket() failed, %s", strerror(errno));
            return -2;
        }
        tv.tv_sec = 0;tv.tv_usec = DBASE_TIMEOUT;
        rsa.sin_family = AF_INET;
        rsa.sin_addr.s_addr = dbio->nodes[no].ip;
        rsa.sin_port = htons(no);
        //setsockopt(dbio->nodes[no].fd, SOL_SOCKET, SO_RCVTIMEO, (char *)&tv, (socklen_t)sizeof(tv));
        if(connect(dbio->nodes[no].fd, (struct sockaddr *)&rsa, sizeof(struct sockaddr)) != 0)
        {
            FATAL_LOGGER(dbio->logger, "connect() to  nodes[%d.%d.%d.%d:%d] failed, %s", ch[0], ch[1], ch[2], ch[3], no, strerror(errno));
            close(dbio->nodes[no].fd);
            dbio->nodes[no].fd = 0;
            return -3;
        }
        ret = dbio->nodes[no].fd;
    }
    return ret;
}

/* resize dbae->res */
int dbio_res_resize(DBIO *dbio, int size)
{
    int ret = -1, len = 0;

    if(dbio)
    {
        if(dbio->res.size < size) len = ((size/DBASE_MM_BASE)+1) * DBASE_MM_BASE;
        if(len > 0) 
        {
            if((dbio->res.data = xmm_renew(dbio->res.data, dbio->res.size, len)))
            {
                dbio->res.size = len;
                ret = 0;
            }
        }
        else ret = 0;
    }
    return ret;
}

/* DBIO reset node */
int dbio_reset_node(DBIO *dbio, int no)
{
    if(dbio && no > 0 && no < DBASE_NODES_MAX)
    {
        if(dbio->nodes[no].fd > 0)
        {
            shutdown(dbio->nodes[no].fd, SHUT_RDWR);
            close(dbio->nodes[no].fd);
        }
        dbio->nodes[no].fd = 0;
    }
    return 0;
}

/* send data */
int dbio_send(DBIO *dbio, int no, int64_t id, int cmd, char *data, int ndata)
{
    unsigned char *ch = NULL;
    int ret = -1, port = 0, n = 0;
    char ip[DBASE_IP_MAX];
    DBHEAD head = {0}, *xhead = NULL;

    if(dbio && no > 0 && no < DBASE_NODES_MAX && id && cmd 
            && data && ndata > 0 && dbio->nodes[no].ip)
    {
        ch = (unsigned char *)&(dbio->nodes[no].ip);
        sprintf(ip, "%d.%d.%d.%d", ch[0], ch[1], ch[2], ch[3]);  
        port = no;
        n = sizeof(DBHEAD) + ndata;
        if(dbio_res_resize(dbio, n) != 0)
        {
            ERROR_LOGGER(dbio->logger, "resize buffer to %s:%d via %d failed, %s", ip, port, dbio->nodes[no].fd, strerror(errno));
            return ret = -2;

        }
        xhead = (DBHEAD *)dbio->res.data;
        xhead->id = id;
        xhead->length = ndata;
        xhead->cmd = cmd;
        memcpy(dbio->res.data + sizeof(DBHEAD), data, ndata);
        if(write(dbio->nodes[no].fd, dbio->res.data, n) != n)
        {
            ERROR_LOGGER(dbio->logger, "write request:%d data %s:%d via %d failed, %s", cmd, ip, port, dbio->nodes[no].fd, strerror(errno));
            dbio_reset_node(dbio, no);
            return ret = -3; 
        }
        if(recv(dbio->nodes[no].fd, &head, sizeof(DBHEAD), MSG_WAITALL) != sizeof(DBHEAD))
        {
            ERROR_LOGGER(dbio->logger, "recv header %s:%d via %d failed, %s", ip, port, dbio->nodes[no].fd, strerror(errno));
            dbio_reset_node(dbio, no);
            return ret = -4;
        }
        if(head.status == DBASE_STATUS_ERR) 
        {
            ERROR_LOGGER(dbio->logger, "err_status:%d id:%lld %s:%d via %d failed, %s", head.status, (long long)head.id, ip, port, dbio->nodes[no].fd, strerror(errno));
            return ret = -5;
        }
        ret = 0;
    }
    return ret;
}

/* dbio receive */
int dbio_recv(DBIO *dbio, int no, int64_t id)
{
    unsigned char *ch = NULL;
    int ret = -1, port = 0;
    char ip[DBASE_IP_MAX];
    DBHEAD head = {0};

    if(dbio && no > 0 && no < DBASE_NODES_MAX && id && dbio->nodes[no].ip)
    {
        head.id = id;
        head.length = 0;
        head.cmd = DBASE_REQ_GET;
        ch = (unsigned char *)&(dbio->nodes[no].ip);
        sprintf(ip, "%d.%d.%d.%d", ch[0], ch[1], ch[2], ch[3]);  
        port = no;
        /* send request header */
        if(write(dbio->nodes[no].fd, &head, sizeof(DBHEAD)) <= 0)
        {
            ERROR_LOGGER(dbio->logger, "send request header to %s:%d via %d failed, %s", ip, port, dbio->nodes[no].fd, strerror(errno));
            dbio_reset_node(dbio, no);
            return ret = -2;
        }
        /* recv response header */
        if(recv(dbio->nodes[no].fd, &head, sizeof(DBHEAD), MSG_WAITALL) != sizeof(DBHEAD))
        {
            ERROR_LOGGER(dbio->logger, "recv response header %s:%d via %d failed, %s", ip, port, dbio->nodes[no].fd, strerror(errno));
            dbio_reset_node(dbio, no);
            return ret = -3;
        }
        if(dbio_res_resize(dbio, head.length) != 0)
        {
            ERROR_LOGGER(dbio->logger, "renew(%d) data %s:%d via %d failed, %s", head.length, ip, port, dbio->nodes[no].fd, strerror(errno));
            dbio_reset_node(dbio, no);
            return ret = -4;
        }
        //DEBUG_LOGGER(dbio->logger, "over recv data %s:%d via %d", ip, port, dbio->fd);
        if(head.length > 0 && (recv(dbio->nodes[no].fd, dbio->res.data, head.length,
                        MSG_WAITALL)) != head.length)
        {
            ERROR_LOGGER(dbio->logger, "recv data %s:%d via %d failed, %s", ip, port, dbio->nodes[no].fd, strerror(errno));
            dbio_reset_node(dbio, no);
            return ret = -5;
        }
        if(head.status == DBASE_STATUS_ERR) 
        {
            ERROR_LOGGER(dbio->logger, "err_status:%d id:%lld %s:%d via %d failed, %s", head.status, (long long)head.id, ip, port, dbio->nodes[no].fd, strerror(errno));
            return ret = -6;
        }
        ret = dbio->res.ndata = head.length;
    }
    return ret;
}

/* read data */
int dbio_read(DBIO *dbio, int64_t id)
{
    socklen_t rsa_len = sizeof(struct sockaddr);
    int mask = id % DBASE_MASK, ret = -1, no = 0;
    struct sockaddr_in rsa;
    DBHEAD head; DBRESP resp;

    if(dbio && dbio->casts[mask].fd > 0)
    {
        memset(&head, 0, sizeof(DBHEAD)); 
        head.cmd = DBASE_REQ_FIND;
        head.id = id;
        if(sendto(dbio->casts[mask].fd, &head, sizeof(DBHEAD), 0, 
                    (struct sockaddr *)&(dbio->casts[mask].mask_addr), sizeof(struct sockaddr)) < 0)
        {
            FATAL_LOGGER(dbio->logger, "send FIND multicast failed, %s", strerror(errno));
            ret = -2;goto end;
        }
        do
        {
            if(recvfrom(dbio->casts[mask].rfd, &resp, sizeof(DBRESP), MSG_WAITALL, (struct sockaddr *)&rsa, &rsa_len) <= 0)
            {
                FATAL_LOGGER(dbio->logger, "recv FIND id:%lld  response local[%s:%d] failed, %s", LL(id), inet_ntoa(dbio->casts[mask].local_addr.sin_addr), ntohs(dbio->casts[mask].local_addr.sin_port), strerror(errno));
                ret = -3;goto end;
            }
            if(resp.cmd == DBASE_RESP_FIND && resp.id == id)
            {
                if((no = resp.port) > 0 && (dbio->nodes[no].ip = (int)rsa.sin_addr.s_addr) 
                        && dbio_is_connected(dbio, no) >  0
                        && (ret = dbio_recv(dbio, no, resp.id)) > 0)
                {
                    ACCESS_LOGGER(dbio->logger, "read data id:%lld length:%d from node[%s:%d]", LL(id), resp.size, inet_ntoa(rsa.sin_addr), resp.port);
                }
                else
                {
                    FATAL_LOGGER(dbio->logger, "read data id:%lld length:%d from node[%s:%d] failed, %s", LL(id), resp.size, inet_ntoa(rsa.sin_addr), resp.port, strerror(errno));
                }
                break;
            }
        }while(1);
    }
end:
    return ret;
}

/* dbio push  */
int dbio_write(DBIO *dbio, int64_t id, char *data, int ndata)
{
    socklen_t rsa_len = sizeof(struct sockaddr);
    int mask = DBKMASK(id), ret = -1, no = 0;
    struct sockaddr_in rsa;
    DBHEAD head = {0}; DBRESP resp;

    if(dbio && dbio->casts[mask].fd > 0)
    {
        memset(&head, 0, sizeof(DBHEAD)); 
        head.cmd = DBASE_REQ_REQUIRE;
        head.id = id;
        if(sendto(dbio->casts[mask].fd, &head, sizeof(DBHEAD), 0, 
                    (struct sockaddr *)&(dbio->casts[mask].mask_addr), sizeof(struct sockaddr)) < 0)
        {
            ERROR_LOGGER(dbio->logger, "send REQUIRE multicast failed, %s", strerror(errno));
            ret = -2;goto end;
        }
        do
        {
            if(recvfrom(dbio->casts[mask].rfd, &resp, sizeof(DBRESP), MSG_WAITALL, (struct sockaddr *)&rsa, &rsa_len) <= 0)
            {
                ERROR_LOGGER(dbio->logger, "recv REQUIRE id:%lld  response local[%s:%d] failed, %s\n", LL(id), inet_ntoa(dbio->casts[mask].local_addr.sin_addr), ntohs(dbio->casts[mask].local_addr.sin_port), strerror(errno));
                ret = -3;goto end;
            }
            if(resp.cmd == DBASE_RESP_REQUIRE && resp.id == id)
            {
                //unsigned char *ch = (unsigned char *)&(rsa.sin_addr.s_addr);
                //fprintf(stdout, "recv REQUIRE response on local[%s:%d] from %s:%d cmd:%06x id:%lld length:%d host[%d.%d.%d.%d:%d]\n", inet_ntoa(dbio->casts[mask].local_addr.sin_addr), ntohs(dbio->casts[mask].local_addr.sin_port), inet_ntoa(rsa.sin_addr), ntohs(rsa.sin_port), resp.cmd, LL(resp.id), resp.size, ch[0], ch[1], ch[2], ch[3], resp.port);
                if((no = resp.port) > 0 && (dbio->nodes[no].ip = (int)(rsa.sin_addr.s_addr)) 
                        && dbio_is_connected(dbio, resp.port) > 0
                        && (ret = dbio_send(dbio, no, id, DBASE_REQ_SET, data, ndata)) >= 0)
                {
                    ACCESS_LOGGER(dbio->logger, "write data id:%lld length:%d to node[%s:%d]", LL(id), ndata, inet_ntoa(rsa.sin_addr), resp.port);
                } 
                else
                {
                    FATAL_LOGGER(dbio->logger, "write data id:%lld length:%d to node[%s:%d] failed, %s", LL(id), ndata, inet_ntoa(rsa.sin_addr), resp.port, strerror(errno));

                }
                break;
            }
        }while(1);
    }
end:
    return ret;
}

/* close dbio */
int dbio_close(DBIO *dbio)
{
    int i = 0;

    if(dbio)
    {
        for(i = 0; i < DBASE_MASK; i++)
        {
            if(dbio->casts[i].fd > 0) 
            {
                shutdown(dbio->casts[i].fd, SHUT_RDWR);
                close(dbio->casts[i].fd);
                dbio->casts[i].fd = 0;
            }
            if(dbio->casts[i].rfd > 0) 
            {
                shutdown(dbio->casts[i].rfd, SHUT_RDWR);
                close(dbio->casts[i].rfd);
                dbio->casts[i].rfd = 0;
            }
        }
        for(i = 0; i < DBASE_NODES_MAX; i++)
        {
            if(dbio->nodes[i].fd > 0)
            {
                shutdown(dbio->nodes[i].fd, SHUT_RDWR);
                close(dbio->nodes[i].fd);
                dbio->nodes[i].fd = 0;
            }
        }
        if(dbio->res.data && dbio->res.size) xmm_free(dbio->res.data, dbio->res.size);
        return 0;
    }
    return -1;
}

/* genarate 64 bits key */
int64_t dbase_kid(char *str, int len)
{
    int64_t kid = 0;
    unsigned char digest[MD5_LEN];
    md5((unsigned char *)str, len, digest);
    memcpy(&kid, digest, sizeof(int64_t));
    return kid;
}

#ifdef _DEBUG_DBIO
int main(int argc, char **argv)
{
    DBIO dbio;
    int i = 0, n = 0, from = 0, to = 0, count = 0;
    char s[1024];

    if(argc < 3)
    {
        fprintf(stderr, "Usage:%s from count\n", argv[0]);
        _exit(-1);
    }
    from = atoi(argv[1]);
    count = atoi(argv[2]);
    to =  from + count;
    if(dbio_set(&dbio, "234.8.8", 2345) == 0)
    {
        LOGGER_INIT(dbio.logger, "/tmp/dbase.log");
        LOGGER_SET_LEVEL(dbio.logger, 0);
        for(i = from; i < to; i++)
        {
             n = sprintf(s, "sdfakhfklsdkfjlisajddddddddddkkkkkkkkkkkkkkkdmbfnjskandbfmnsadmf,nsdamfnmsdfnmsdanfmds,fnmsd,fnmsd,fnmsdfnsdm,fnsdm,fnm,sadnfmsdfnsam,dfnmasdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddfdsfa,mndfm,ndsam,fnm,adsnfmds,anffsmd,f.ssssssssssam,,,,,,,,,,,,,,,,,,,,,,,,fasdmfnsam,,,,,,,,,,,,,fksahfioewyiroweairjksfdjkdnfkjdnfjknjkdfnjksdnfjkdnfjnjfnakjnakjnajknsjkannnnnnnnnnnnnnnnnw878392789sfahfkjhdfkdsjafjkasfbsdajfbmsnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnddjfklasdfkljsadklfjkladsjflkasdjflksjdflkjsdlfkjalkdfjkldjfkldjf:%d\r\n", i);
            dbio_write(&dbio, i, s, n);
        }
        dbio_close(&dbio);
    }
    return -1;
}
#endif
//gcc -o dbio dbase.c utils/logger.* utils/xmm.c utils/md5.c -Iutils -DHAVE_MMAP -D_DEBUG_DBIO  
#ifdef _DEBUG_DBASE
#define T_URL_MAX   1024
#define T_PACKET_MAX   1024
#define T_COMMENT_MAX   1024
#include "md5.h"
#include "timer.h"
int main(int argc, char **argv)
{
    char *ip = NULL, ch = 0, out[BJSON_BASE * 2], comment[T_COMMENT_MAX], 
         url[T_URL_MAX], *s = NULL;
    int isdaemon = 0, from = 0, to = 0, total = 0, wanto = 0, port = 0, i = 0, 
        isout = 0, x = 0, count = 0, ret = -1, n = 0, vint = 0;
    int64_t key = 0, rand = 0, idlist[T_PACKET_MAX], vlong = 0;
    BELEMENT *root = NULL, *sub = NULL, *e = NULL, *k = NULL;
    BJSON record = {0}, request = {0};
    unsigned char digest[MD5_LEN];
    double vdouble = 0.0;
    void *timer = NULL;
    DBASE dbase = {0};
    BRES *res = NULL;
    pid_t pid = 0;

    /* get configure file */
    while((ch = getopt(argc, argv, "h:p:f:n:do")) != -1)
    {
        switch(ch)
        {
            case 'h':
                ip = optarg;
                break;
            case 'p':
                port = atoi(optarg);
                break;
            case 'd':
                isdaemon = 1;
                break;
            case 'o':
                isout = 1;
                break;
            case 'f':
                from = atoi(optarg);
                break;
            case 'n':
                wanto = atoi(optarg);
                break;
            default:
                break;
        }
    }
    if(ip == NULL || port <= 0 && wanto <= 0)
    {
        fprintf(stderr, "Usage:%s -h server_ip -p server_port -n db_insert_count (-d daemon -o output)\n", argv[0]);
        _exit(-1);
    }
    /* daemon */
    if(isdaemon)
    {
        pid = fork();
        switch (pid) {
            case -1:
                perror("fork()");
                exit(EXIT_FAILURE);
                break;
            case 0: /* child process */
                if(setsid() == -1)
                    exit(EXIT_FAILURE);
                break;
            default:/* parent */
                _exit(EXIT_SUCCESS);
                break;
        }
    }
    if(dbase_set(&dbase, ip, port) == 0)
    {
        LOGGER_INIT(dbase.logger, "/tmp/dbase.log");
        LOGGER_SET_LEVEL(dbase.logger, 0);
        TIMER_INIT(timer);
        n = from;to = from + wanto;
        do
        {
            brequest_reset(&record);
            sprintf(url, "http://demo.com/%d.html", ++n);
            md5((unsigned char *)url, strlen(url), digest);
            key = *((int64_t *)digest);
            rand = (int64_t)random();
            bjson_start(&record);
            bjson_append_long(&record, "id", key);
            bjson_append_int(&record, "int", rand/n);
            bjson_append_long(&record, "long", rand);
            bjson_append_double(&record, "avg", (double)rand/(double)n);
            bjson_append_string(&record, "url", url);
            bjson_append_blob(&record, "md5", digest, 16);
            bjson_new_object(&record, "items");
            count = random()%16;
            for(i = 1; i <= count; i++)
            {
                rand = (int64_t)random();
                bjson_new_object(&record, "");
                bjson_append_int(&record, "id", rand/i);
                bjson_append_long(&record, "score", rand/n);
                sprintf(comment, "comment:%d rand:%lld float:%f", i, (long long)rand, (double)rand/(double)33);
                bjson_append_string(&record, "comment", comment);
                bjson_finish_object(&record);
            }
            bjson_finish_object(&record);
            bjson_finish(&record);
            if((ret = dbase_set_record(&dbase, key, &record)) < 0)
            {
                fprintf(stderr, "set_data(%lld) ndata:%d failed, ret:%d\n", (long long)key, record.current, ret);
                _exit(-1);
            }
        }while(n <= to);
        TIMER_SAMPLE(timer);
        if(dbase.nwrite > 0)
        {
            fprintf(stdout, "set_record_total:%lld db_time:%lld(usec) avg:%lld(usec)\n", 
                    LL(dbase.nwrite), LL(dbase.time_write), LL(dbase.time_write/dbase.nwrite));
        }
        else
        {
            fprintf(stderr, "set_records() error");
        }
        fprintf(stdout, "gen_and_update_records:%d from:%d time:%lld\n", wanto, n, PT_LU_USEC(timer));
        n = from;to = from + wanto;
        do
        {
            sprintf(url, "http://demo.com/%d.html", ++n);
            md5((unsigned char *)url, strlen(url), digest);
            key = *((int64_t *)digest);
            if((res = dbase_get_record(&dbase, key)) == NULL)
            {
                fprintf(stderr, "get_record(%lld) failed, ret:%d\n", LL(key), ret);
            }
        }while(n <= to);
        TIMER_SAMPLE(timer);
        if(dbase.nread > 0)
        {
            fprintf(stdout, "get_record_total:%lld db_time:%lld(usec) avg:%lld(usec)\n", 
                    LL(dbase.nread), LL(dbase.time_read), LL(dbase.time_read/dbase.nread));
        }
        else
        {
            fprintf(stderr, "get_records() error");
        }
        fprintf(stdout, "read_records:%d from:%d time:%lld\n", wanto, n, PT_LU_USEC(timer));
        /* get records */
        /*
        if((res = dbase_get_records(&dbase, &request)) == NULL)
        {
            fprintf(stderr, "read_data() ndata:%d failed, ret:%d\n", dbase.ndata, ret);
        }
        */
        //fprintf(stdout, "read ndata:%d db_time:%lld\n", dbase.ndata, PT_LU_USEC(timer));
        if(res && bjson_json(res, out) > 0)
        {
            fprintf(stdout, "key:%lld json:%s\n", LL(key), out);
        }
        if((root = belement(res)) && (e = belement_childs(root)))
        {
            fprintf(stdout, "%s::%d OK\n", __FILE__, __LINE__);
            do
            {
                fprintf(stdout, "key:%s type:%d length:%d\n", belement_key(e), belement_type(e), e->length);
            }while((e = belement_next(root, e)));
            if((e = belement_find(root, "int")) && belement_v_int(e, &vint) == 0) fprintf(stdout, "int:%d\n", vint);
            if((e = belement_find(root, "long")) && belement_v_long(e, &vlong) == 0) fprintf(stdout, "long:%lld\n", LL(vlong));
            if((e = belement_find(root, "avg")) && belement_v_double(e, &vdouble) == 0) fprintf(stdout, "double:%f\n", vdouble);
            if((e = belement_find(root, "url")) && (s = belement_v_string(e))) fprintf(stdout, "url:%s\n", s);
            if((e = belement_find(root, "md5")) && (n = belement_v_blob(e, &s)) > 0) fprintf(stdout, "md5:%p len:%d\n", s, n);
            if((sub = belement_find(root, "items")) && (e = belement_childs(sub)))
            {
                fprintf(stdout, "item->nchilds:%d length:%d\n", sub->nchilds, sub->length);
                i = 0;
                do
                {
                    fprintf(stdout, "i:%d type:%d length:%d\n", i, belement_type(e), e->length);
                    if((k = belement_childs(e)))
                    {
                        do
                        {
                            fprintf(stdout, "key:%s type:%d length:%d\n", belement_key(k), belement_type(k), k->length);
                        }while((k = belement_next(e, k)));
                    }
                    ++i;
                }while((e = belement_next(sub, e)));
            }
        }
        TIMER_CLEAN(timer);
        bjson_clean(&record);
        bjson_clean(&request);
        dbase_close(&dbase);
        return 0;
    }
    return -1;
}
//gcc -o dbase dbase.c utils/xmm.c utils/md5.c -Iutils -DHAVE_MMAP -D_DEBUG_DBASE && ./dbase -h 127.0.0.1 -p2481 -n 1000000
#endif

#ifdef _DEBUG_BJSON
int main()
{
    BJSON old = {0}, batch = {0}, one = {0};
    BREC record = {0};
    char out[65536];
    int nout = 0;

    BJSON_INIT(old);     
    BJSON_INIT(batch);     
    BJSON_INIT(one);     
   
    /* old json */
    bjson_start(&old);
    bjson_append_int(&old, "id", 10000);
    bjson_new_object(&old, "items"); 
    bjson_new_object(&old, "");
    bjson_append_int(&old, "userid", 1000);
    bjson_append_string(&old, "username", "name");
    bjson_finish_object(&old);
    bjson_finish_object(&old);
    bjson_finish(&old);
    if((nout = bjson_json((BREC *)&old, (const char *)out)) > 0)
    {
        fprintf(stdout, "%s\n", out);
    }
    /* batch json */
    bjson_start(&batch);
    bjson_append_int(&batch, "id", 10001);
    bjson_new_object(&batch, "items"); 
    bjson_new_object(&batch, "");
    bjson_append_int(&batch, "userid", 1001);
    bjson_append_string(&batch, "username", "name1");
    bjson_finish_object(&batch);
    bjson_finish_object(&batch);
    bjson_append_double(&batch, "score", 0.111);
    bjson_finish(&batch);
    if((nout = bjson_json((BREC *)&batch, (const char *)out)) > 0)
    {
        fprintf(stdout, "%s\n", out);
    }
    /* merge */
    bjson_merge_element(&one, bjson_root(&old), bjson_root(&batch));
    if((nout = bjson_json((BREC *)&one, (const char *)out)) > 0)
    {
        fprintf(stdout, "%s\n", out);
    }
    bjson_clean(&old);
    bjson_clean(&batch);
    bjson_clean(&one);
    return -1;
}
#endif
//gcc -o dbase dbase.c utils/logger.c utils/xmm.c utils/md5.c -Iutils -DHAVE_MMAP -D_DEBUG_BJSON && ./dbase
