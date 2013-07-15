#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include <sys/resource.h>
#include <locale.h>
#include <sbase.h>
#include <arpa/inet.h>
#include "dbase.h"
#include "stime.h"
#include "base64.h"
#include "base64trackerdhtml.h"
#include "http.h"
#include "iniparser.h"
#include "logger.h"
#include "mtrie.h"
#include "xmap.h"
#ifndef HTTP_LINE_SIZE
#define HTTP_LINE_SIZE          4096
#endif
#ifndef HTTP_BLOCK_MAX 
#define HTTP_BLOCK_MAX          262144
#endif
#ifndef HTTP_QUERY_MAX
#define HTTP_QUERY_MAX          1024
#endif
#define HTTP_LINE_MAX           65536
#define HTTP_RESP_OK            "HTTP/1.1 200 OK\r\n\r\n"
#define HTTP_RESP_CREATED       "HTTP/1.1 201 Created\r\n\r\n"
#define HTTP_RESP_CONTINUE      "HTTP/1.1 100 Continue\r\n\r\n"
#define HTTP_BAD_REQUEST        "HTTP/1.1 400 Bad Request\r\n\r\n"
#define HTTP_NOT_FOUND          "HTTP/1.1 404 Not Found\r\n\r\n" 
#define HTTP_NOT_MODIFIED       "HTTP/1.1 304 Not Modified\r\n\r\n"
#define HTTP_NO_CONTENT         "HTTP/1.1 204 No Content\r\n\r\n"
#ifndef LL
#define LL(x) ((long long int)x)
#endif
#define E_OP_LIST        0x01
#define E_OP_SET_MODE    0x02
#define E_OP_ADD_MASK    0x03
#define E_OP_DROP_MASK   0x04
#define E_OP_GET         0x05
static char *e_argvs[] =
{
        "op",
#define E_ARGV_OP        0
        "id",
#define E_ARGV_ID        1
        "diskid",
#define E_ARGV_DISKID    2
        "path",
#define E_ARGV_PATH      3
        "port",
#define E_ARGV_PORT      4
        "limit",
#define E_ARGV_LIMIT     5
        "mode",
#define E_ARGV_MODE      6
        "url",
#define E_ARGV_URL       7
        "keys",
#define E_ARGV_KEYS      8
        "mask"
#define E_ARGV_MASK      9
};
#define  E_ARGV_NUM      10
static void *argvmap = NULL;
typedef struct _MGROUP
{
    int bits;
    int groupid;
}MGROUP;
static char *http_default_charset = "UTF-8";
static char *httpd_home = NULL;
static int is_inside_html = 1;
static unsigned char *httpd_index_html_code = NULL;
static int  nhttpd_index_html_code = 0;
static SBASE *sbase = NULL;
static SERVICE *httpd = NULL;
static SERVICE *trackerd = NULL;
static SERVICE *traced = NULL;
static MGROUP multicasts[DBASE_MASK_MAX];
static char *multicast_network = "234.8.8";
static int  multicast_port = 2345;
static int  multicast_limit = 8;
static SERVICE *multicastd = NULL;
static dictionary *dict = NULL;
static void *http_headers_map = NULL;
static void *logger = NULL;
static XMAP *xmap = NULL;
static int query_wait_time = 200000;
static int query_timeout = 10000000;
static char *dbprefix = "/mdb/";
static int ndbprefix = 5;
static int group_conns_limit = 16;
//static int chunk_io_timeout = 60000000;
static char *public_multicast = "233.8.8.8";
static int public_multicast_limit = 8;
static int public_groupid = 0;
int traced_local_handler(CONN *conn, DBHEAD *xhead);
int traced_try_request(CONN *conn, DBHEAD *xhead, XMSETS *xsets, int num);
int traced_req_handler(CONN *conn);
/* httpd packet reader */
int httpd_packet_reader(CONN *conn, CB_DATA *buffer)
{
    if(conn)
    {
        return 0;
    }
    return -1;
}

/* converts hex char (0-9, A-Z, a-z) to decimal */
unsigned char hex2chr(unsigned char c)
{
    c = c - '0';
    if (c > 9) {
        c = (c + '0' - 1) | 0x20;
        c = c - 'a' + 11;
    }
    if (c > 15)
        c = 0xFF;
    return c;
}

#define URLDECODE(p, end, ps, high, low)                                                \
do                                                                                      \
{                                                                                       \
    while(*p != '\0' && *p != '&')                                                      \
    {                                                                                   \
        if(ps >= end) break;                                                            \
        else if(*p == '+') {*ps++ = 0x20;++p;continue;}                                 \
        else if(*p == '%')                                                              \
        {                                                                               \
            high = hex2chr(*(p + 1));                                                   \
            if (high != 0xFF)                                                           \
            {                                                                           \
                low = hex2chr(*(p + 2));                                                \
                if (low != 0xFF)                                                        \
                {                                                                       \
                    high = (high << 4) | low;                                           \
                    if (high < 32 || high == 127) high = '_';                           \
                    *ps++ = high;                                                       \
                    p += 3;                                                             \
                    continue;                                                           \
                }                                                                       \
            }                                                                           \
        }                                                                               \
        *ps++ = *p++;                                                                   \
    }                                                                                   \
    *ps = '\0';                                                                         \
}while(0)

int hex2long(char *hex, int64_t *key)
{
    unsigned char *s = (unsigned char *)((void *)key);
    unsigned char *p = (unsigned char *)hex, high = 0, low = 0;
    int i = 0;
    if(p)
    {
        while(*p && i < 16)
        {
            high = hex2chr(*p++);
            low = hex2chr(*p++);
            *s++ = (unsigned char)((high << 4) | low);
            ++i;
        }
    }
    return 0;
}

/*request handler */
int httpd_request_handler(CONN *conn, HTTP_REQ *http_req, char *data, int ndata)
{
    int ret = -1, k = 0, gid = 0, n = 0;
    char *s = NULL, *p = NULL;
    DBHEAD xhead = {0};

    if(conn && http_req)
    {
        conn->over_estate(conn);//disable drop cache 
        p = s = http_req->path + ndbprefix;
        while(*p && *p != '?')p++;
        if(*p == '?') *p = '\0';
        xhead.id = 0;
        hex2long(s, &(xhead.id)); 
        k = DBKMASK(xhead.id);
        gid = multicasts[k].groupid;
        if(http_req->reqid == HTTP_PUT)
        {
            xhead.cmd = DBASE_REQ_SET;
        }
        else
        {
            xhead.cmd = DBASE_REQ_GET;
        }
        xhead.cid = conn->xids[2];
        xhead.index = conn->index;
        conn->xids64[0] = xhead.id;
        conn->xids[0] = http_req->reqid;//reqid
        conn->xids[1] = 0;//keepalive
        xhead.ssize = 0;
        if((n = http_req->headers[HEAD_GEN_CONNECTION]) > 0
            && strcasestr(http_req->hlines + n, "keep-alive"))
        {
            conn->xids[1] = 1;
        }
        if(data && ndata > 0)
        {
            xhead.ssize = ndata;
            xhead.size = ndata;
        }
        ACCESS_LOGGER(logger, "HTTP_REQ{%s %s key:%llu length:%d from %s:%d via %d", http_methods[http_req->reqid].e, http_req->path, (uint64_t)(xhead.id), xhead.ssize, conn->remote_ip, conn->remote_port, conn->fd);
        ret = traced_local_handler(conn, &xhead);
    }
    return ret;
}

/* over handler */
void httpd_over_handler(int index, int64_t key)
{
    CONN *conn  = NULL;

    if(index && (conn = httpd->findconn(httpd, index)) && key == conn->xids64[0])
    {
        ACCESS_LOGGER(logger, "over key:%llu", LLU(conn->xids64[0]));
        conn->reset_chunk2(conn);
        conn->push_chunk(conn, HTTP_RESP_CONTINUE, strlen(HTTP_RESP_CONTINUE));
        if(!conn->xids[1])conn->over(conn);
    }
    return ;
}

/* fail handler */
void httpd_fail_handler(int index, int64_t key)
{
    CONN *conn  = NULL;

    if(index && (conn = httpd->findconn(httpd, index)) && key == conn->xids64[0])
    {
        ACCESS_LOGGER(logger, "fail_req{key:%llu}", LLU(conn->xids64[0]));
        conn->push_chunk(conn, HTTP_BAD_REQUEST, strlen(HTTP_BAD_REQUEST));
        conn->over(conn);
    }
    return ;
}

/* out data */
int httpd_out_handler(DBHEAD *resp)
{
    char buf[HTTP_BUF_SIZE], *block = NULL;
    CONN *conn  = NULL;
    int n = 0, ret = -1;

    if(resp->index && (conn = httpd->findconn(httpd, resp->index))
        && xmap_cache_info(xmap, resp->cid, &block) > 0 && block)
    {
        n = sprintf(buf, "HTTP/1.0 200 OK\r\nContent-Length: %d\r\n\r\n", resp->length);
        conn->xids[2] = resp->cid;
        conn->wait_estate(conn);
        conn->push_chunk(conn, buf, n);
        ret = conn->relay_chunk(conn, block, resp->length);
        if(!conn->xids[1])conn->over(conn);
        ret = 0;
    }
    return ret;
}

/* packet handler */
int httpd_packet_handler(CONN *conn, CB_DATA *packet)
{
    char buf[HTTP_BUF_SIZE], file[HTTP_PATH_MAX], *block = NULL, *p = NULL, *end = NULL;
    int ret = -1, n = 0, dbreq = 0;
    HTTP_REQ http_req = {0};
    struct stat st = {0};
    
    if(conn)
    {
        //TIMER_INIT(timer);
        p = packet->data;
        end = packet->data + packet->ndata;
        //WARN_LOGGER(logger, "%s", p);
        if(http_request_parse(p, end, &http_req, http_headers_map) == -1) goto err_end;
        if(strncasecmp(http_req.path, dbprefix, ndbprefix) == 0) dbreq = 1; 
        if(dbreq && (n = http_req.headers[HEAD_ENT_CONTENT_LENGTH]) > 0
                && (n = atol(http_req.hlines + n)) > 0)
        {
            if((conn->xids[2] = xmap_truncate_block(xmap, n, &block)) > 0 && block)
            {
                ACCESS_LOGGER(logger, "truncate block cid[%d] length:%d", conn->xids[2], n);
                conn->wait_estate(conn);
                conn->save_cache(conn, &http_req, sizeof(HTTP_REQ));
                return conn->store_chunk(conn, block, n);
            }
            else 
            {
                conn->over(conn);
                return 0;
            }
        }
        if(dbreq) return httpd_request_handler(conn, &http_req, NULL, 0);
        if(http_req.reqid == HTTP_GET)
        {
            if(is_inside_html && httpd_index_html_code && nhttpd_index_html_code > 0)
            {
                p = buf;
                p += sprintf(p, "HTTP/1.0 200 OK\r\nContent-Length: %d\r\n"
                        "Content-Type: text/html;charset=%s\r\n",
                        nhttpd_index_html_code, http_default_charset);
                if((n = http_req.headers[HEAD_GEN_CONNECTION]) > 0)
                    p += sprintf(p, "Connection: %s\r\n", (http_req.hlines + n));
                p += sprintf(p, "\r\n");
                conn->push_chunk(conn, buf, (p - buf));
                return conn->push_chunk(conn, httpd_index_html_code, nhttpd_index_html_code);
            }
            else if(httpd_home)
            {
                p = file;
                if(http_req.path[0] != '/')
                    p += sprintf(p, "%s/%s", httpd_home, http_req.path);
                else
                    p += sprintf(p, "%s%s", httpd_home, http_req.path);
                if(http_req.path[1] == '\0')
                    p += sprintf(p, "%s", "index.html");
                if((n = (p - file)) > 0 && stat(file, &st) == 0)
                {
                    if(st.st_size == 0)
                    {
                        return conn->push_chunk(conn, HTTP_NO_CONTENT, strlen(HTTP_NO_CONTENT));
                    }
                    else if((n = http_req.headers[HEAD_REQ_IF_MODIFIED_SINCE]) > 0
                            && str2time(http_req.hlines + n) == st.st_mtime)
                    {
                        return conn->push_chunk(conn, HTTP_NOT_MODIFIED, strlen(HTTP_NOT_MODIFIED));
                    }
                    else
                    {
                        p = buf;
                        p += sprintf(p, "HTTP/1.0 200 OK\r\nContent-Length:%lld\r\n"
                                "Content-Type: text/html;charset=%s\r\n",
                                (long long int)(st.st_size), http_default_charset);
                        if((n = http_req.headers[HEAD_GEN_CONNECTION]) > 0)
                            p += sprintf(p, "Connection: %s\r\n", http_req.hlines + n);
                        p += sprintf(p, "Last-Modified:");
                        p += GMTstrdate(st.st_mtime, p);
                        p += sprintf(p, "%s", "\r\n");//date end
                        p += sprintf(p, "%s", "\r\n");
                        conn->push_chunk(conn, buf, (p - buf));
                        return conn->push_file(conn, file, 0, st.st_size);
                    }
                }
                else
                {
                    return conn->push_chunk(conn, HTTP_NOT_FOUND, strlen(HTTP_NOT_FOUND));
                }
            }
            else
            {
                return conn->push_chunk(conn, HTTP_BAD_REQUEST, strlen(HTTP_BAD_REQUEST));
            }
        }
        else if(http_req.reqid == HTTP_POST)
        {
            if((n = http_req.headers[HEAD_ENT_CONTENT_LENGTH]) > 0
                    && (n = atol(http_req.hlines + n)) > 0)
            {
                conn->save_cache(conn, &http_req, sizeof(HTTP_REQ));
                return conn->recv_chunk(conn, n);
            }
        }
err_end:
        ret = conn->push_chunk(conn, HTTP_BAD_REQUEST, strlen(HTTP_BAD_REQUEST));
    }
    return ret;
}

/*  data handler */
int httpd_data_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    char *p = NULL, *pp = NULL, *end = NULL, *path = NULL, *url = NULL, *keys = NULL,
        *key = NULL, header[HTTP_LINE_SIZE], ip[HTTP_IP_MAX];
    int ret = -1, id = 0, op = 0, n = 0, i = 0, diskid = -1, nout = 0,
        port = 0, mode = -1, mask = 0, num = 0;
    HTTP_REQ httpRQ = {0}, *http_req = NULL;
    CB_DATA *block = NULL;
    CONN *xconn = NULL;
    DBHEAD xhead = {0};
    off_t limit = -1;
    SESSION sess = {0};

    if(conn && packet && cache && chunk && chunk->ndata > 0)
    {
        conn->over_estate(conn);
        if((http_req = (HTTP_REQ *)cache->data))
        {
            if(http_req->reqid == HTTP_PUT)
            {
                return httpd_request_handler(conn, http_req, chunk->data, chunk->ndata);
            }
            if(http_req->reqid == HTTP_POST)
            {
                p = chunk->data;
                end = chunk->data + chunk->ndata;
                if(http_argv_parse(p, end, &httpRQ) == -1)goto err_end;
                for(i = 0; i < httpRQ.nargvs; i++)
                {
                    if(httpRQ.argvs[i].nk > 0 && (n = httpRQ.argvs[i].k) > 0
                            && (p = (httpRQ.line + n)))
                    {
                        if((id = (mtrie_get(argvmap, p, httpRQ.argvs[i].nk) - 1)) >= 0
                                && httpRQ.argvs[i].nv > 0
                                && (n = httpRQ.argvs[i].v) > 0
                                && (p = (httpRQ.line + n)))
                        {
                            switch(id)
                            {
                                case E_ARGV_OP :
                                    op = atoi(p);
                                    break;
                                case E_ARGV_ID :
                                    key = p;
                                    break;
                                case E_ARGV_KEYS:
                                    keys = p;
                                    break;
                                case E_ARGV_DISKID:
                                    diskid = atoi(p);
                                    break;
                                case E_ARGV_PORT:
                                    port = atoi(p);
                                    break;
                                case E_ARGV_MODE:
                                    mode = atoi(p);
                                    break;
                                case E_ARGV_LIMIT:
                                    limit = (off_t) atoll(p);
                                    break;
                                case E_ARGV_PATH:
                                    path = p;
                                    break;
                                case E_ARGV_URL:
                                    url = p;
                                    break;
                                case E_ARGV_MASK:
                                    if((pp = strchr(p, '/')))
                                    {
                                        *pp++ = 0;
                                        mask = (int)inet_addr(p);
                                        num = atoi(pp);
                                    }
                                    break;
                            }
                        }
                    }
                }
                if(diskid > 0 && xmap_get_disk_host(xmap, diskid, ip, &port) == 0)
                {
                    switch(op)
                    {
                        case E_OP_DROP_MASK:
                            xhead.cmd = DBASE_REQ_DROP_MASK;
                        break;
                        case E_OP_SET_MODE:
                            if(mode >= 0)
                            {
                                xhead.cmd = DBASE_REQ_SET_MODE;
                                xhead.ssize = mode;
                            }
                        break;
                        case E_OP_ADD_MASK:
                            if(mask && num > 0)
                            {
                                xhead.cmd = DBASE_REQ_ADD_MASK;
                                xhead.ip = mask;
                                xhead.ssize = num;
                            }
                        break;
                    }
                    if(xhead.cmd && (xconn = traced->newconn(traced, -1, -1, ip, port, &sess)))
                    {
                        //fprintf(stdout, "%s::%d mask:%d num:%d remote[%s:%d]\n", __FILE__, __LINE__, mask, num, ip, port);
                        if(op == E_OP_DROP_MASK) xmap_reset_masks(xmap, diskid);
                        xconn->push_chunk(xconn, &xhead, sizeof(DBHEAD));
                        xconn->over(xconn);
                        goto disklists;
                    }
                }
                if(op == E_OP_LIST){goto disklists;}
            }
        }
        return conn->over(conn);
disklists:
        if((block = conn->newchunk(conn, HTTP_BLOCK_MAX)))
        {
            if((nout = xmap_list_disks(xmap, block->data)) > 0)
            {
                n = sprintf(header, "HTTP/1.0 200 OK\r\nContent-Length: %d\r\nContent-Type: text/html;charset=%s\r\nConnection: close\r\n\r\n", ret, http_default_charset);
                //WARN_LOGGER(logger, "remote[%s:%d]", conn->remote_ip, conn->remote_port);
                conn->push_chunk(conn, header, n);
                if(conn->send_chunk(conn, block, nout) != 0)
                    conn->freechunk(conn, block);
                return conn->over(conn);
            }
        }
        return conn->close(conn);
err_end: 
        ret = conn->push_chunk(conn, HTTP_BAD_REQUEST, strlen(HTTP_BAD_REQUEST));
    }
    return ret;
}

/* evtimeout handler */
int httpd_evtimeout_handler(CONN *conn)
{
    int ret = -1, n = 0, status = -1;
    DBHEAD *xhead = NULL;
    XMSETS xsets;

    if(conn && (xhead = (DBHEAD *)(conn->header.data)))
    {
        conn->over_evstate(conn);
        if((n = xmap_check_meta(xmap, xhead->qid, &status, &xsets)) > 0
                && status == XM_STATUS_FREE)
        {
            if(xhead->cmd == DBASE_REQ_SET || xhead->cmd == DBASE_REQ_GET)
            {
                ret = traced_try_request(conn, xhead, &xsets, n);
            }
        }
    }
    return ret;
}

/* httpd timeout handler */
int httpd_timeout_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    if(conn)
    {
        conn->over_cstate(conn);
        return conn->over(conn);
    }
    return -1;
}

/* error handler */
int httpd_error_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    int ret = -1;

    if(conn && conn->xids[2] > 0)
    {
        xmap_drop_cache(xmap, conn->xids[2]);
        conn->xids[2] = 0;
    }
    return ret;
}

/* data sendover handler */
int httpd_sendover_handler(CONN *conn)
{
    int ret = -1;

    if(conn && conn->xids[2] > 0)
    {
        xmap_drop_cache(xmap, conn->xids[2]);
        conn->over_estate(conn);
        conn->xids[2] = 0;
    }
    return ret;
}

/* OOB data handler for httpd */
int httpd_oob_handler(CONN *conn, CB_DATA *oob)
{
    if(conn)
    {
        return 0;
    }
    return -1;
}

/* check host groupid */
void traced_update_metainfo(DBHEAD *xhead, char *ip, int port, 
        int *gid, int *total, int *status)
{
    SESSION session = {0};
    int diskid = 0;

    if((diskid = xmap_diskid(xmap, ip, port, gid)) > 0)
    {
        ACCESS_LOGGER(logger, "disks[%d][%s:%d]  group[%d]", diskid, ip, port, *gid);
        if(*gid == XM_NO_GROUPID)
        {
            memcpy(&session, &(traced->session), sizeof(SESSION));
            session.flags |= SB_NONBLOCK;
            *gid = traced->addgroup(traced, ip, port, group_conns_limit, &session);
            xmap_set_groupid(xmap, diskid, *gid);
        }
        *total = xmap_over_meta(xmap, xhead->qid, diskid, status);
    }
    return ;
}

/* send request */
int traced_try_request(CONN *conn, DBHEAD *xhead, XMSETS *xsets, int num)
{
    int ret = -1, i = 0, gid = 0, port = 0;
    unsigned char *p = NULL;
    CONN *xconn = NULL;
    SESSION sess = {0};
    char ip[16];

    if(conn && xhead && xsets && num > 0 
        && (xhead->cmd == DBASE_REQ_SET || xhead->cmd == DBASE_REQ_GET))
    {
        memcpy(&sess, &(traced->session), sizeof(SESSION));
        sess.ok_handler = traced_req_handler;
        for(i = 0; i < num; i++)
        {
            gid = xsets->lists[i].gid;
            port = xsets->lists[i].port;
            p = (unsigned char *)&(xsets->lists[i].ip);
            sprintf(ip, "%u.%u.%u.%u", p[0], p[1], p[2], p[3]);
            if((xconn = traced->getconn(traced, gid)))
            {
                ACCESS_LOGGER(logger, "TASK{cmd:%d qid:%d key:%llu gid:%d size:%d} on remote[%s:%d] local[%s:%d] via %d", xhead->cmd, xhead->qid, (uint64_t)xhead->id, gid, xhead->ssize, xconn->remote_ip, xconn->remote_port, xconn->local_ip, xconn->local_port, xconn->fd);
                xconn->wait_estate(xconn);
                xconn->save_header(xconn, xhead, sizeof(DBHEAD));
                ret = traced_req_handler(xconn);
            }
            else
            {
                sess.groupid = gid;
                xconn = traced->newconn(traced, -1, -1, ip, port, &sess);
                if(xconn)
                {
                    ACCESS_LOGGER(logger, "TRANS{cmd:%d qid:%d key:%llu gid:%d size:%d} on remote[%s:%d] local[%s:%d] via %d", xhead->cmd, xhead->qid, (uint64_t)xhead->id, gid, xhead->ssize, xconn->remote_ip, xconn->remote_port, xconn->local_ip, xconn->local_port, xconn->fd);
                    xconn->wait_estate(xconn);
                    xconn->save_header(xconn, xhead, sizeof(DBHEAD));
                    ret = traced->newtransaction(traced, xconn, xhead->index);
                }
                else
                {
                    ACCESS_LOGGER(logger, "NO_FREE_CONN[%s:%d]{key:%llu qid:%d}",
                            ip, port, (uint64_t)xhead->id, xhead->qid);
                    httpd_fail_handler(xhead->index, xhead->id);
                }
            }
            if(xhead->cmd == DBASE_REQ_GET) break;
        }
    }
    return ret;
}

/* handling request */
int traced_local_handler(CONN *conn, DBHEAD *xhead)
{
    int ret = -1, status = -1, k = 0, gid = 0, qid = 0, n = 0;
    CONN *xconn = NULL;
    XMSETS xsets;

    if(conn && xhead && (qid = xmap_qid(xmap, xhead->id, &status, &xsets, &n))> 0)
    {
        xhead->qid = qid;
        xhead->index = conn->index;
        conn->save_header(conn, xhead, sizeof(DBHEAD));
        if(status == XM_STATUS_FREE)
        {
            if(n != XM_HOST_MAX)
            {
                k = DBKMASK(xhead->id);
                gid = multicasts[k].groupid;
                if((xconn = multicastd->getconn(multicastd, gid))) 
                {
                    ACCESS_LOGGER(logger, "SEND_CASTS{qid:%d key:%llu length:%d multicasts[%d][%s:%d fd:%d]} from %s:%d ", qid, (uint64_t)(xhead->id), xhead->ssize, k, xconn->remote_ip, xconn->remote_port, xconn->fd, conn->remote_ip, conn->remote_port);
                    xconn->push_chunk(xconn, xhead, sizeof(DBHEAD));
                    xconn->over(xconn);
                    ret = 0;
                }
                else
                {
                    ACCESS_LOGGER(logger, "NO_CONN{qid:%d key:%llu length:%d multicasts[%d]} from %s:%d", qid, (uint64_t)(xhead->id), xhead->size, k, conn->remote_ip, conn->remote_port);
                    xmap_reset_meta(xmap, qid);
                    return conn->close(conn);
                }
            }
            else
            {
                if(xhead->cmd == DBASE_REQ_GET || xhead->cmd == DBASE_REQ_SET)
                    ret = traced_try_request(conn, xhead, &xsets, n);
                else
                {
                    xhead->cmd |= DBASE_CMD_BASE;
                    xhead->length = sizeof(XMSETS);
                    conn->push_chunk(conn, xhead, sizeof(DBHEAD));
                    conn->push_chunk(conn, &xsets, xhead->length);
                    ret = 0;
                }
            }
            conn->set_timeout(conn, query_timeout);
        }
        else
        {
            //timeout wait
            ACCESS_LOGGER(logger, "WAIT_TASK{qid:%d key:%llu length:%d } from %s:%d ", xhead->qid, (uint64_t)(xhead->id), xhead->ssize, conn->remote_ip, conn->remote_port);
            conn->wait_evtimeout(conn, query_wait_time);
        }
    }
    return ret;
}

/* trackerd packet handler */
int trackerd_packet_handler(CONN *conn, CB_DATA *packet)
{
    DBHEAD *xhead = NULL;
    int ret = -1;

    if(conn && packet && (xhead = (DBHEAD *)packet->data))
    {
        if(xhead->length > 0) 
        {
            conn->recv_chunk(conn, xhead->length);
        }
        else
        {
            ret = traced_local_handler(conn, xhead);
        }
    }
    return ret;
}

/* data handler */
int trackerd_data_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    DBHEAD *xhead = NULL;
    int ret = -1;

    if(conn && packet && (xhead = (DBHEAD *)(packet->data)))
    {
        xhead->ssize = chunk->ndata;
        ret = traced_local_handler(conn, xhead);
    }
    return ret;
}

/* error handler */
int trackerd_error_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    int ret = -1;

    if(conn && packet)
    {
        
    }
    return ret;
}


/* httpd timeout handler */
int trackerd_timeout_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    if(conn)
    {
        conn->over_cstate(conn);
        return conn->close(conn);
    }
    return -1;
}

int trackerd_evtimeout_handler(CONN *conn)
{
    DBHEAD *xhead = NULL;
    int status = 0;
    XMSETS xsets;

    if(conn && (xhead = (DBHEAD *)(conn->cache.data)))
    {
        if(xmap_check_meta(xmap, xhead->qid, &status, &xsets) > 0)
        {
            if(status == XM_STATUS_FREE)
            {
                xhead->cmd |= DBASE_CMD_BASE;
                xhead->length = sizeof(XMSETS);
                conn->push_chunk(conn, xhead, sizeof(DBHEAD));
                conn->push_chunk(conn, &xsets, xhead->length);
            }
            else
            {
                conn->wait_evtimeout(conn, query_wait_time);
            }
            return 0;
        }
        else
        {
            conn->close(conn);
        }
    }
    return -1;
}

/* OOB data handler for httpd */
int trackerd_oob_handler(CONN *conn, CB_DATA *oob)
{
    if(conn)
    {
        return 0;
    }
    return -1;
}

/* req handler */
int traced_req_handler(CONN *conn)
{
    DBHEAD *xhead = NULL;
    char *block = NULL;
    int ret = -1;

    if(conn && conn->header.ndata > 0 && (xhead = (DBHEAD *)(conn->header.data)) && xhead->cmd)
    {
        if(xhead->ssize > 0 && xmap_cache_info(xmap, xhead->cid, &block) > 0 && block)
        { 
            xhead->length = xhead->ssize; 
            conn->push_chunk(conn, xhead, sizeof(DBHEAD));
            ret = conn->relay_chunk(conn, block, xhead->ssize);
            ACCESS_LOGGER(logger, "REQ{cmd:%d key:%llu length:%d} on %s:%d via %d", xhead->cmd, (uint64_t)xhead->id, xhead->ssize, conn->remote_ip, conn->remote_port, conn->fd);
        }
        else
        {
            ACCESS_LOGGER(logger, "REQ{cmd:%d key:%llu length:%d} on %s:%d via %d", xhead->cmd, (uint64_t)xhead->id, xhead->ssize, conn->remote_ip, conn->remote_port, conn->fd);
            ret = conn->push_chunk(conn, xhead, sizeof(DBHEAD));
        }
        xhead->cmd = 0;/* disable retry quest */
    }
    return ret;
}

/* trans handler */
int traced_trans_handler(CONN *conn, int tid)
{
    DBHEAD *xhead = NULL;
    int ret = -1;

    if(conn && tid > 0)
    {
        if(conn->status == CONN_STATUS_FREE)
        {
            if((xhead = (DBHEAD *)(conn->header.data)) && xhead->cmd)
            {
                if(xhead->ssize > 0)
                {
                    ACCESS_LOGGER(logger, "READY_PUT{cmd:%d qid:%d key:%llu ssize:%d} on %s:%d via %d", xhead->cmd, xhead->qid, (uint64_t)xhead->id, xhead->ssize, conn->remote_ip, conn->remote_port, conn->fd);
                }
                else
                {
                    ACCESS_LOGGER(logger, "READY_GET{cmd:%d qid:%d key:%llu size:%d} on %s:%d via %d", xhead->cmd, xhead->qid, (uint64_t)xhead->id, xhead->size, conn->remote_ip, conn->remote_port, conn->fd);
                }
                ret = traced_req_handler(conn);
            }
        }
    }
    return ret;
}

/* traced packet handler */
int traced_packet_handler(CONN *conn, CB_DATA *packet)
{
    DBHEAD *xhead = NULL;
    int ret = -1;

    if(conn && packet && (xhead = (DBHEAD *)packet->data))
    {
        conn->over_estate(conn);
        ACCESS_LOGGER(logger, "RESP{key:%llu} on %s:%d via %d", (uint64_t)xhead->id, conn->remote_ip, conn->remote_port, conn->fd);
        if(xhead->cmd == DBASE_RESP_SET && xmap_over_task(xmap, xhead->cid) == 0)
        {
            httpd_over_handler(xhead->index, xhead->id);
        }
        if(conn->groupid) conn->over_session(conn);
        else conn->close(conn);
    }
    return ret;
}

/* data handler */
int traced_data_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    DBHEAD *xhead = NULL;
    int ret = -1;

    if(conn && packet && (xhead = (DBHEAD *)packet->data))
    {
        ACCESS_LOGGER(logger, "RESP_DATA{qid:%d key:%llu} on %s:%d", xhead->qid, (uint64_t)xhead->id, conn->remote_ip, conn->remote_port);
        conn->over_estate(conn);
        if(xhead->cmd == DBASE_RESP_GET)
        {
            if(httpd_out_handler(xhead) != 0)
            {
                xmap_drop_cache(xmap, xhead->cid);
            }
            else ret = 0;
        }
        if(conn->groupid) conn->over_session(conn);
        else conn->close(conn);
    }
    return ret;
}

/* error handler */
int traced_error_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    DBHEAD *xhead = NULL;
    int ret = -1;

    if(conn && (xhead = (DBHEAD *)conn->header.data)
        && (xhead->cmd == DBASE_REQ_GET || xhead->cmd == DBASE_REQ_SET))
    {
        ACCESS_LOGGER(logger, "ERR_REQ{qid:%d key:%llu} on remote[%s:%d] local[%s:%d] via %d", xhead->qid, (uint64_t)xhead->id, conn->remote_ip, conn->remote_port, conn->local_ip, conn->local_port, conn->fd);
        xmap_over_task(xmap, xhead->cid);
        httpd_fail_handler(xhead->index, xhead->id);
        ret = 0;
    }
    return ret;
}

/* quick handler */
int traced_quick_handler(CONN *conn, CB_DATA *packet)
{
    DBHEAD *head = NULL;
    char *block = NULL;
    int ret = -1;

    if(conn && packet && (head = (DBHEAD *)packet->data))
    {
        ACCESS_LOGGER(logger, "QUICK{cmd:%d qid:%d key:%llu} on %s:%d", head->cmd, head->qid, (uint64_t)head->id, conn->remote_ip, conn->remote_port);
        if((head->cid = xmap_truncate_block(xmap, head->length, &block)) > 0 && block)
        {
            conn->wait_estate(conn);
            conn->store_chunk(conn, block, head->length);
        }
        ret = head->length;
    }
    return ret;
}

/* httpd timeout handler */
int traced_timeout_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    if(conn)
    {
        conn->over_cstate(conn);
        return conn->close(conn);
    }
    return -1;
}

/* OOB data handler for httpd */
int traced_oob_handler(CONN *conn, CB_DATA *oob)
{
    if(conn)
    {
        return 0;
    }
    return -1;
}


/* multicastd packet handler */
int multicastd_packet_handler(CONN *conn, CB_DATA *packet)
{
    int ret = -1, gid = -6, total = 0, status = -1;
    DBHEAD *resp = NULL, xhead = {0};
    SESSION sess = {0};
    CONN *xconn = NULL;

    if(conn && packet && (resp = (DBHEAD *)(packet->data)) 
            && resp->status == DBASE_STATUS_OK)
    {
        traced_update_metainfo(resp, conn->remote_ip, resp->port, &gid, &total, &status);
        ACCESS_LOGGER(logger, "Found{cmd:%d qid:%d key:%llu gid:%d size:%d} on host[%s:%d]", resp->cmd, resp->qid, (uint64_t)resp->id, gid, resp->size, conn->remote_ip, resp->port);
        //check task over
        memcpy(&xhead, resp, sizeof(DBHEAD));
        xhead.cmd = 0;
        switch(resp->cmd)
        {
            case DBASE_RESP_GET:
                if(resp->size > 0)
                {
                    xhead.cmd = DBASE_REQ_GET;
                    xhead.length = 0;
                    xhead.size = 0;
                }
                break;
            case DBASE_RESP_SET:
                xhead.cmd = DBASE_REQ_SET;
                xhead.size = 0;
                break;
        }
        if((xhead.cmd == DBASE_REQ_GET && total == 1)//check first request 
            || xhead.cmd == DBASE_REQ_SET)
        {
            if((xconn = traced->getconn(traced, gid)))
            {
                ACCESS_LOGGER(logger, "TASK{cmd:%d qid:%d key:%llu gid:%d size:%d} on remote[%s:%d] local[%s:%d] via %d", xhead.cmd, xhead.qid, (uint64_t)xhead.id, gid, xhead.ssize, xconn->remote_ip, xconn->remote_port, xconn->local_ip, xconn->local_port, xconn->fd);
                xconn->wait_estate(xconn);
                xconn->save_header(xconn, &xhead, sizeof(DBHEAD));
                ret = traced_req_handler(xconn);
            }
            else
            {        
                memcpy(&sess, &(traced->session), sizeof(SESSION));
                sess.ok_handler = traced_req_handler;
                sess.groupid = gid;
                xconn = traced->newconn(traced, -1, -1, conn->remote_ip, resp->port, &sess);
                if(xconn)
                {
                    ACCESS_LOGGER(logger, "TRANS{cmd:%d qid:%d key:%llu gid:%d size:%d} on remote[%s:%d] local[%s:%d] via %d", xhead.cmd, xhead.qid, (uint64_t)xhead.id, gid, xhead.ssize, xconn->remote_ip, xconn->remote_port, xconn->local_ip, xconn->local_port, xconn->fd);
                    xconn->wait_estate(xconn);
                    xconn->save_header(xconn, &xhead, sizeof(DBHEAD));
                    ret = traced->newtransaction(traced, xconn, xhead.index);
                }
                else
                {
                    ACCESS_LOGGER(logger, "NO_FREE_CONN[%s:%d]{key:%llu qid:%d}", 
                            conn->remote_ip, resp->port, (uint64_t)resp->id, resp->qid);

                    httpd_fail_handler(xhead.index, xhead.id);
                }
            }
        }
        conn->over_session(conn);
    }
    return ret;
}

/* quick handler */
int multicastd_quick_handler(CONN *conn, CB_DATA *packet)
{
    DBHEAD *head = NULL;
    int ret = -1;

    if(conn && packet && (head = (DBHEAD *)packet->data))
    {
        ret = head->length;
    }
    return ret;
}


/* data handler */
int multicastd_data_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    int ret = -1, i = 0, ndisks = 0;
    DBHEAD *head = NULL;
    MDISK *disks = NULL;

    if(conn && chunk && packet && (head = (DBHEAD *)packet->data))
    {
        if(head->cmd == DBASE_RESP_REPORT
            && (disks = (MDISK *)chunk->data) 
            && (ndisks = (chunk->ndata/sizeof(MDISK))) > 0)
        {
            for(i = 0; i < ndisks; i++)
            {
                disks[i].ip = (int)inet_addr(conn->remote_ip);
                xmap_set_disk(xmap, &(disks[i]));
            }
        }
        ret = conn->over_session(conn);
    }
    return ret;
}

/* error handler */
int multicastd_error_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    int ret = -1;

    if(conn)
    {
        ret = 0;
    }
    return ret;
}

/* httpd timeout handler */
int multicastd_timeout_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{

    if(conn)
    {
        conn->over_cstate(conn);
        return conn->over(conn);
    }
    return -1;
}

/* OOB data handler for httpd */
int multicastd_oob_handler(CONN *conn, CB_DATA *oob)
{
    if(conn)
    {
        return 0;
    }
    return -1;
}

/* running callback */
void multicastd_onrunning(SERVICE *service)
{
    char ip[SB_IP_MAX];
    int i = 0;

    if(service == multicastd && multicast_network && multicast_port > 0)
    {
        if(public_groupid < 1) 
        {
            public_groupid = multicastd->addgroup(multicastd, public_multicast, multicast_port,
                    public_multicast_limit, &(multicastd->session));
        }
        for(i = 0; i < DBASE_MASK_MAX; i++)
        {
            if(multicasts[i].groupid < 1)
            {
                sprintf(ip, "%s.%d", multicast_network, i);
                multicasts[i].groupid = multicastd->addgroup(multicastd, ip, multicast_port,
                        multicast_limit, &(multicastd->session));
            }
        }
    }
    return ;
}

/* heartbeat */
void multicastd_heartbeat_handler(void *arg)
{
    DBHEAD head = {0};
    CONN *conn = NULL;
    
    if(public_groupid > 0 && (conn = multicastd->getconn(multicastd, public_groupid)))
    {
        head.cmd = DBASE_REQ_REPORT;
        conn->push_chunk(conn, &head, sizeof(DBHEAD));
        conn->over(conn);
    }
    return ;
}

/* Initialize from ini file */
int sbase_initialize(SBASE *sbase, char *conf)
{
    char *s = NULL, *p = NULL;
    int i = 0, n = 0;

    if((dict = iniparser_new(conf)) == NULL)
    {
        fprintf(stderr, "Initializing conf:%s failed, %s\n", conf, strerror(errno));
        _exit(-1);
    }
    if((p = iniparser_getstr(dict, "HTTPD:access_log")))
    {
        LOGGER_INIT(logger, p);
        LOGGER_SET_LEVEL(logger, iniparser_getint(dict, "HTTPD:access_log_level", 0));
    }
    /* http headers map */
    if((http_headers_map = http_headers_map_init()) == NULL)
    {
        fprintf(stderr, "Initialize http_headers_map failed,%s", strerror(errno));
        _exit(-1);
    }
    /* argvmap */
    if((argvmap = mtrie_init()) == NULL)_exit(-1);
    else
    {
        for(i = 0; i < E_ARGV_NUM; i++)
        {
            mtrie_add(argvmap, e_argvs[i], strlen(e_argvs[i]), i+1);
        }
    }
    /* initialize xmap */
    if((p = iniparser_getstr(dict, "XMAP:basedir")))
        xmap = xmap_init(p);
    else
    {
        FATAL_LOGGER(logger, "initialize xmap failed, %s", strerror(errno));
    }
    if((p = iniparser_getstr(dict, "XMAP:dbprefix")))
    {
        dbprefix = p;
        ndbprefix = strlen(p);
    }
    /* SBASE */
    sbase->nchilds = iniparser_getint(dict, "SBASE:nchilds", 0);
    sbase->connections_limit = iniparser_getint(dict, "SBASE:connections_limit", SB_CONN_MAX);
    setrlimiter("RLIMIT_NOFILE", RLIMIT_NOFILE, sbase->connections_limit);
    sbase->usec_sleep = iniparser_getint(dict, "SBASE:usec_sleep", SB_USEC_SLEEP);
    sbase->set_log(sbase, iniparser_getstr(dict, "SBASE:logfile"));
    sbase->set_evlog(sbase, iniparser_getstr(dict, "SBASE:evlogfile"));
    /* httpd */
    is_inside_html = iniparser_getint(dict, "HTTPD:is_inside_html", 1);
    httpd_home = iniparser_getstr(dict, "HTTPD:httpd_home");
    http_default_charset = iniparser_getstr(dict, "HTTPD:httpd_charset");
    /* decode html base64 */
    if(html_code_base64 && (n = strlen(html_code_base64)) > 0
            && (httpd_index_html_code = (unsigned char *)calloc(1, n + 1)))
    {
        nhttpd_index_html_code = base64_decode(httpd_index_html_code,
                (char *)html_code_base64, n);
    }
    if((httpd = service_init()) == NULL)
    {
        fprintf(stderr, "Initialize service failed, %s", strerror(errno));
        _exit(-1);
    }
    httpd->family = iniparser_getint(dict, "HTTPD:inet_family", AF_INET);
    httpd->sock_type = iniparser_getint(dict, "HTTPD:socket_type", SOCK_STREAM);
    httpd->ip = iniparser_getstr(dict, "HTTPD:service_ip");
    httpd->port = iniparser_getint(dict, "HTTPD:service_port", 2080);
    httpd->working_mode = iniparser_getint(dict, "HTTPD:working_mode", WORKING_PROC);
    httpd->service_type = iniparser_getint(dict, "HTTPD:service_type", S_SERVICE);
    httpd->service_name = iniparser_getstr(dict, "HTTPD:service_name");
    httpd->nprocthreads = iniparser_getint(dict, "HTTPD:nprocthreads", 1);
    httpd->ndaemons = iniparser_getint(dict, "HTTPD:ndaemons", 0);
    httpd->niodaemons = iniparser_getint(dict, "HTTPD:niodaemons", 1);
    httpd->use_cond_wait = iniparser_getint(dict, "HTTPD:use_cond_wait", 0);
    if(iniparser_getint(dict, "HTTPD:use_cpu_set", 0) > 0) httpd->flag |= SB_CPU_SET;
    if(iniparser_getint(dict, "HTTPD:event_lock", 0) > 0) httpd->flag |= SB_EVENT_LOCK;
    if(iniparser_getint(dict, "HTTPD:newconn_delay", 0) > 0) httpd->flag |= SB_NEWCONN_DELAY;
    if(iniparser_getint(dict, "HTTPD:tcp_nodelay", 0) > 0) httpd->flag |= SB_TCP_NODELAY;
    if(iniparser_getint(dict, "HTTPD:socket_linger", 0) > 0) httpd->flag |= SB_SO_LINGER;
    if(iniparser_getint(dict, "HTTPD:while_send", 0) > 0) httpd->flag |= SB_WHILE_SEND;
    if(iniparser_getint(dict, "HTTPD:log_thread", 0) > 0) httpd->flag |= SB_LOG_THREAD;
    if(iniparser_getint(dict, "HTTPD:use_outdaemon", 0) > 0) httpd->flag |= SB_USE_OUTDAEMON;
    if(iniparser_getint(dict, "HTTPD:use_evsig", 0) > 0) httpd->flag |= SB_USE_EVSIG;
    if(iniparser_getint(dict, "HTTPD:use_cond", 0) > 0) httpd->flag |= SB_USE_COND;
    if((n = iniparser_getint(dict, "HTTPD:sched_realtime", 0)) > 0) httpd->flag |= (n & (SB_SCHED_RR|SB_SCHED_FIFO));
    if((n = iniparser_getint(dict, "HTTPD:io_sleep", 0)) > 0) httpd->flag |= ((SB_IO_NANOSLEEP|SB_IO_USLEEP|SB_IO_SELECT) & n);
    httpd->nworking_tosleep = iniparser_getint(dict, "HTTPD:nworking_tosleep", SB_NWORKING_TOSLEEP);
    httpd->set_log(httpd, iniparser_getstr(dict, "HTTPD:logfile"));
    httpd->set_log_level(httpd, iniparser_getint(dict, "HTTPD:log_level", 0));
    httpd->session.flags = SB_NONBLOCK;
    httpd->session.packet_type = iniparser_getint(dict, "HTTPD:packet_type",PACKET_DELIMITER);
    httpd->session.packet_delimiter = iniparser_getstr(dict, "HTTPD:packet_delimiter");
    p = s = httpd->session.packet_delimiter;
    while(*p != 0 )
    {
        if(*p == '\\' && *(p+1) == 'n')
        {
            *s++ = '\n';
            p += 2;
        }
        else if (*p == '\\' && *(p+1) == 'r')
        {
            *s++ = '\r';
            p += 2;
        }
        else
            *s++ = *p++;
    }
    *s++ = 0;
    httpd->session.packet_delimiter_length = strlen(httpd->session.packet_delimiter);
    httpd->session.buffer_size = iniparser_getint(dict, "HTTPD:buffer_size", SB_BUF_SIZE);
    httpd->session.packet_reader = &httpd_packet_reader;
    httpd->session.packet_handler = &httpd_packet_handler;
    httpd->session.data_handler = &httpd_data_handler;
    httpd->session.timeout_handler = &httpd_timeout_handler;
    httpd->session.oob_handler = &httpd_oob_handler;
    httpd->session.error_handler = &httpd_error_handler;
    httpd->session.sendover_handler = &httpd_sendover_handler;
    /* trackerd */
    if((trackerd = service_init()) == NULL)
    {
        fprintf(stderr, "Initialize service failed, %s", strerror(errno));
        _exit(-1);
    }
    trackerd->family = iniparser_getint(dict, "TRACKERD:inet_family", AF_INET);
    trackerd->sock_type = iniparser_getint(dict, "TRACKERD:socket_type", SOCK_STREAM);
    trackerd->ip = iniparser_getstr(dict, "TRACKERD:service_ip");
    trackerd->port = iniparser_getint(dict, "TRACKERD:service_port", 2070);
    trackerd->working_mode = iniparser_getint(dict, "TRACKERD:working_mode", WORKING_PROC);
    trackerd->service_type = iniparser_getint(dict, "TRACKERD:service_type", S_SERVICE);
    trackerd->service_name = iniparser_getstr(dict, "TRACKERD:service_name");
    trackerd->nprocthreads = iniparser_getint(dict, "TRACKERD:nprocthreads", 8);
    trackerd->ndaemons = iniparser_getint(dict, "TRACKERD:ndaemons", 0);
    trackerd->niodaemons = iniparser_getint(dict, "TRACKERD:niodaemons", 1);
    trackerd->use_cond_wait = iniparser_getint(dict, "TRACKERD:use_cond_wait", 0);
    if(iniparser_getint(dict, "TRACKERD:use_cpu_set", 0) > 0) trackerd->flag |= SB_CPU_SET;
    if(iniparser_getint(dict, "TRACKERD:event_lock", 0) > 0) trackerd->flag |= SB_EVENT_LOCK;
    if(iniparser_getint(dict, "TRACKERD:newconn_delay", 0) > 0) trackerd->flag |= SB_NEWCONN_DELAY;
    if(iniparser_getint(dict, "TRACKERD:tcp_nodelay", 0) > 0) trackerd->flag |= SB_TCP_NODELAY;
    if(iniparser_getint(dict, "TRACKERD:socket_linger", 0) > 0) trackerd->flag |= SB_SO_LINGER;
    if(iniparser_getint(dict, "TRACKERD:while_send", 0) > 0) trackerd->flag |= SB_WHILE_SEND;
    if(iniparser_getint(dict, "TRACKERD:log_thread", 0) > 0) trackerd->flag |= SB_LOG_THREAD;
    if(iniparser_getint(dict, "TRACKERD:use_outdaemon", 0) > 0) trackerd->flag |= SB_USE_OUTDAEMON;
    if(iniparser_getint(dict, "TRACKERD:use_evsig", 0) > 0) trackerd->flag |= SB_USE_EVSIG;
    if(iniparser_getint(dict, "TRACKERD:use_cond", 0) > 0) trackerd->flag |= SB_USE_COND;
    if((n = iniparser_getint(dict, "TRACKERD:sched_realtime", 0)) > 0) trackerd->flag |= (n & (SB_SCHED_RR|SB_SCHED_FIFO));
    if((n = iniparser_getint(dict, "TRACKERD:io_sleep", 0)) > 0) trackerd->flag |= ((SB_IO_NANOSLEEP|SB_IO_USLEEP|SB_IO_SELECT) & n);
    trackerd->nworking_tosleep = iniparser_getint(dict, "TRACKERD:nworking_tosleep", SB_NWORKING_TOSLEEP);
    trackerd->set_log(trackerd, iniparser_getstr(dict, "TRACKERD:logfile"));
    trackerd->set_log_level(trackerd, iniparser_getint(dict, "TRACKERD:log_level", 0));
    trackerd->session.flags = SB_NONBLOCK;
    trackerd->session.packet_type = PACKET_CERTAIN_LENGTH;
    trackerd->session.packet_length = sizeof(DBHEAD);
    trackerd->session.buffer_size = iniparser_getint(dict, "TRACKERD:buffer_size", SB_BUF_SIZE);
    trackerd->session.packet_handler = &trackerd_packet_handler;
    trackerd->session.data_handler = &trackerd_data_handler;
    trackerd->session.timeout_handler = &trackerd_timeout_handler;
    trackerd->session.evtimeout_handler = &trackerd_evtimeout_handler;
    /* traced */
    if((traced = service_init()) == NULL)
    {
        fprintf(stderr, "Initialize service failed, %s", strerror(errno));
        _exit(-1);
    }
    traced->family = iniparser_getint(dict, "TRACED:inet_family", AF_INET);
    traced->sock_type = iniparser_getint(dict, "TRACED:socket_type", SOCK_STREAM);
    //traced->ip = iniparser_getstr(dict, "TRACED:service_ip");
    //traced->port = iniparser_getint(dict, "TRACED:service_port", 2060);
    traced->working_mode = iniparser_getint(dict, "TRACED:working_mode", WORKING_PROC);
    traced->service_type = iniparser_getint(dict, "TRACED:service_type", C_SERVICE);
    traced->service_name = iniparser_getstr(dict, "TRACED:service_name");
    traced->nprocthreads = iniparser_getint(dict, "TRACED:nprocthreads", 1);
    traced->ndaemons = iniparser_getint(dict, "TRACED:ndaemons", 0);
    traced->niodaemons = iniparser_getint(dict, "TRACED:niodaemons", 1);
    traced->use_cond_wait = iniparser_getint(dict, "TRACED:use_cond_wait", 0);
    if(iniparser_getint(dict, "TRACED:use_cpu_set", 0) > 0) traced->flag |= SB_CPU_SET;
    if(iniparser_getint(dict, "TRACED:event_lock", 0) > 0) traced->flag |= SB_EVENT_LOCK;
    if(iniparser_getint(dict, "TRACED:newconn_delay", 0) > 0) traced->flag |= SB_NEWCONN_DELAY;
    if(iniparser_getint(dict, "TRACED:tcp_nodelay", 0) > 0) traced->flag |= SB_TCP_NODELAY;
    if(iniparser_getint(dict, "TRACED:socket_linger", 0) > 0) traced->flag |= SB_SO_LINGER;
    if(iniparser_getint(dict, "TRACED:while_send", 0) > 0) traced->flag |= SB_WHILE_SEND;
    if(iniparser_getint(dict, "TRACED:log_thread", 0) > 0) traced->flag |= SB_LOG_THREAD;
    if(iniparser_getint(dict, "TRACED:use_outdaemon", 0) > 0) traced->flag |= SB_USE_OUTDAEMON;
    if(iniparser_getint(dict, "TRACED:use_evsig", 0) > 0) traced->flag |= SB_USE_EVSIG;
    if(iniparser_getint(dict, "TRACED:use_cond", 0) > 0) traced->flag |= SB_USE_COND;
    if((n = iniparser_getint(dict, "TRACED:sched_realtime", 0)) > 0) traced->flag |= (n & (SB_SCHED_RR|SB_SCHED_FIFO));
    if((n = iniparser_getint(dict, "TRACED:io_sleep", 0)) > 0) traced->flag |= ((SB_IO_NANOSLEEP|SB_IO_USLEEP|SB_IO_SELECT) & n);
    traced->nworking_tosleep = iniparser_getint(dict, "TRACED:nworking_tosleep", SB_NWORKING_TOSLEEP);
    traced->set_log(traced, iniparser_getstr(dict, "TRACED:logfile"));
    traced->set_log_level(traced, iniparser_getint(dict, "TRACED:log_level", 0));
    //traced->session.flags = SB_NONBLOCK;
    traced->session.packet_type = PACKET_CERTAIN_LENGTH;
    traced->session.packet_length = sizeof(DBHEAD);
    traced->session.buffer_size = iniparser_getint(dict, "TRACED:buffer_size", SB_BUF_SIZE);
    traced->session.packet_handler = &traced_packet_handler;
    traced->session.transaction_handler = &traced_trans_handler;
    traced->session.quick_handler = &traced_quick_handler;
    traced->session.data_handler = &traced_data_handler;
    traced->session.timeout_handler = &traced_timeout_handler;
    traced->session.error_handler = &traced_error_handler;
    group_conns_limit = iniparser_getint(dict, "TRACED:group_conns_limit", 32);
    /* multicastd */
    if((multicastd = service_init()) == NULL)
    {
        fprintf(stderr, "Initialize service multicast failed, %s", strerror(errno));
        _exit(-1);
    }
    multicastd->family = iniparser_getint(dict, "MULTICASTD:inet_family", AF_INET);
    multicastd->sock_type = iniparser_getint(dict, "MULTICASTD:socket_type", SOCK_DGRAM);
    multicastd->ip = iniparser_getstr(dict, "MULTICASTD:service_ip");
    multicastd->port = iniparser_getint(dict, "MULTICASTD:service_port", 2344);
    multicastd->heartbeat_interval = iniparser_getint(dict, "MULTICASTD:heartbeat", 1000000);
    multicastd->working_mode = iniparser_getint(dict, "MULTICASTD:working_mode", WORKING_THREAD);
    multicastd->service_type = iniparser_getint(dict, "MULTICASTD:service_type", S_SERVICE);
    multicastd->service_name = iniparser_getstr(dict, "MULTICASTD:service_name");
    multicastd->nprocthreads = iniparser_getint(dict, "MULTICASTD:nprocthreads", 1);
    multicastd->conns_limit = iniparser_getint(dict, "MULTICASTD:conns_limit", SB_CONNS_LIMIT);
    multicastd->ndaemons = iniparser_getint(dict, "MULTICASTD:ndaemons", 0);
    multicastd->niodaemons = iniparser_getint(dict, "MULTICASTD:niodaemons", 1);
    multicastd->use_cond_wait = iniparser_getint(dict, "MULTICASTD:use_cond_wait", 0);
    if(iniparser_getint(dict, "MULTICASTD:use_cpu_set", 0) > 0) multicastd->flag |= SB_CPU_SET;
    if(iniparser_getint(dict, "MULTICASTD:event_lock", 0) > 0) multicastd->flag |= SB_EVENT_LOCK;
    if(iniparser_getint(dict, "MULTICASTD:newconn_delay", 0) > 0) multicastd->flag |= SB_NEWCONN_DELAY;
    if(iniparser_getint(dict, "MULTICASTD:tcp_nodelay", 0) > 0) multicastd->flag |= SB_TCP_NODELAY;
    if(iniparser_getint(dict, "MULTICASTD:socket_linger", 0) > 0) multicastd->flag |= SB_SO_LINGER;
    if(iniparser_getint(dict, "MULTICASTD:while_send", 0) > 0) multicastd->flag |= SB_WHILE_SEND;
    if(iniparser_getint(dict, "MULTICASTD:log_thread", 0) > 0) multicastd->flag |= SB_LOG_THREAD;
    if(iniparser_getint(dict, "MULTICASTD:use_outdaemon", 0) > 0) multicastd->flag |= SB_USE_OUTDAEMON;
    if(iniparser_getint(dict, "MULTICASTD:use_evsig", 0) > 0) multicastd->flag |= SB_USE_EVSIG;
    if(iniparser_getint(dict, "MULTICASTD:use_cond", 0) > 0) multicastd->flag |= SB_USE_COND;
    if((n = iniparser_getint(dict, "MULTICASTD:sched_realtime", 0)) > 0) multicastd->flag |= (n & (SB_SCHED_RR|SB_SCHED_FIFO));
    if((n = iniparser_getint(dict, "MULTICASTD:io_sleep", 0)) > 0) multicastd->flag |= ((SB_IO_NANOSLEEP|SB_IO_USLEEP|SB_IO_SELECT) & n);
    multicastd->nworking_tosleep = iniparser_getint(dict, "MULTICASTD:nworking_tosleep", SB_NWORKING_TOSLEEP);
    multicastd->set_log(multicastd, iniparser_getstr(dict, "MULTICASTD:logfile"));
    multicastd->set_log_level(multicastd, iniparser_getint(dict, "MULTICASTD:log_level", 0));
    multicastd->session.flags = SB_MULTICAST;
    multicastd->session.packet_type = PACKET_CERTAIN_LENGTH;
    multicastd->session.packet_length = sizeof(DBHEAD);
    multicastd->session.buffer_size = iniparser_getint(dict, "MULTICASTD:buffer_size", SB_BUF_SIZE);
    multicastd->session.packet_handler = &multicastd_packet_handler;
    multicastd->session.data_handler = &multicastd_data_handler;
    multicastd->session.quick_handler = &multicastd_quick_handler;
    multicastd->session.timeout_handler = &multicastd_timeout_handler;
    multicastd->session.error_handler = &multicastd_error_handler;
    multicastd->onrunning = &multicastd_onrunning;
    multicastd->set_heartbeat(multicastd, multicastd->heartbeat_interval, 
            &multicastd_heartbeat_handler, multicastd);
    public_multicast = iniparser_getstr(dict, "MULTICASTD:public_multicast");
    public_multicast_limit = iniparser_getint(dict, "MULTICASTD:public_multicast_limit", 24);
    multicast_network = iniparser_getstr(dict, "MULTICASTD:multicast_network");
    multicast_port = iniparser_getint(dict, "MULTICASTD:multicast_port", 2345);
    multicast_limit = iniparser_getint(dict, "MULTICASTD:multicast_limit", 64);
    multicastd->session.multicast_ttl = iniparser_getint(dict, "MULTICASTD:multicast_ttl", 4);
    memset(multicasts, 0, sizeof(MGROUP) * DBASE_MASK_MAX);
    return (sbase->add_service(sbase, httpd) | sbase->add_service(sbase, trackerd)
            | sbase->add_service(sbase, traced) | sbase->add_service(sbase, multicastd));
}

static void hitrackerd_stop(int sig)
{
    switch (sig)
    {
        case SIGINT:
        case SIGTERM:
            fprintf(stderr, "hitrackerd server is interrupted by user.\n");
            if(sbase)sbase->stop(sbase);
            break;
        default:
            break;
    }
    return ;
}

int main(int argc, char **argv)
{
    char *conf = NULL, ch = 0;
    int is_daemon = 0;
    pid_t pid;

    /* get configure file */
    while((ch = getopt(argc, argv, "c:d")) != (char)-1)
    {
        if(ch == 'c') conf = optarg;
        else if(ch == 'd') is_daemon = 1;
    }
    if(conf == NULL)
    {
        fprintf(stderr, "Usage:%s -d -c config_file\n", argv[0]);
        _exit(-1);
    }
    /* locale */
    setlocale(LC_ALL, "C");
    /* signal */
    signal(SIGTERM, &hitrackerd_stop);
    signal(SIGINT,  &hitrackerd_stop);
    signal(SIGHUP,  &hitrackerd_stop);
    signal(SIGPIPE, SIG_IGN);
    signal(SIGCHLD, SIG_IGN);
    if(is_daemon)
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
    if((sbase = sbase_init()) == NULL)
    {
        exit(EXIT_FAILURE);
        return -1;
    }
    fprintf(stdout, "Initializing from configure file:%s\n", conf);
    /* Initialize sbase */
    if(sbase_initialize(sbase, conf) != 0 )
    {
        fprintf(stderr, "Initialize from configure file failed\n");
        return -1;
    }
    fprintf(stdout, "Initialized successed\n");
    sbase->running(sbase, 0);
    //sbase->running(sbase, 3600);
    //sbase->running(sbase, 60000000);
    //sbase->stop(sbase);
    sbase->clean(sbase);
    if(http_headers_map) http_headers_map_clean(http_headers_map);
    if(dict)iniparser_free(dict);
    if(argvmap)mtrie_clean(argvmap);
    if(logger){LOGGER_CLEAN(logger);}
    if(httpd_index_html_code) free(httpd_index_html_code);
    if(xmap) xmap_clean(xmap);
    return 0;
}
