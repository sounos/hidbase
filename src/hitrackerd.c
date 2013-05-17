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
#ifndef HTTP_BUF_SIZE
#define HTTP_BUF_SIZE           131072
#endif
#ifndef HTTP_LINE_SIZE
#define HTTP_LINE_SIZE          4096
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
static MGROUP multicasts[DBASE_MASK];
static char *multicast_network = "234.8.8";
static int  multicast_port = 2345;
static int  multicast_limit = 8;
static SERVICE *multicastd = NULL;
static dictionary *dict = NULL;
static void *http_headers_map = NULL;
static void *logger = NULL;
static XMAP *xmap = NULL;
static int query_wait_time = 100000;
static char *dbprefix = "/mdb/";
static int ndbprefix = 5;
static int group_conns_limit = 16;
//static int chunk_io_timeout = 60000000;
static char *public_multicast = "233.8.8.8";
static int public_multicast_limit = 8;
static int public_groupid = 0;
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
    CONN *xconn = NULL;
    DBHEAD xhead = {0};
    char *s = NULL, *p = NULL;
    int ret = -1, k = 0, gid = 0;

    if(conn && http_req)
    {
        p = s = http_req->path + ndbprefix;
        while(*p && *p != '?')p++;
        if(*p == '?') *p = '\0';
        xhead.id = 0;
        hex2long(s, &(xhead.id)); 
        k = DBKMASK(xhead.id);
        gid = multicasts[k].groupid;
        if(data && ndata > 0)
        {
            //fprintf(stdout, "%s::%d ndata:%d\r\n", __FILE__, __LINE__, ndata);
            xhead.cid = xmap_cache(xmap, data, ndata);
            //fprintf(stdout, "%s::%d ndata:%d\r\n", __FILE__, __LINE__, ndata);
            xhead.size = ndata;
            ACCESS_LOGGER(logger, "CACHE{key:%llu cid:%d length:%d} group[%d] from %s:%d ", (uint64_t)(xhead.id), xhead.cid, xhead.size, k, conn->remote_ip, conn->remote_port);
        }
        ACCESS_LOGGER(logger, "READY_REQUIRE{%s %s key:%llu length:%d cid:%d group[%d]} from %s:%d ", http_methods[http_req->reqid].e, http_req->path, (uint64_t)(xhead.id), xhead.size, xhead.cid, k, conn->remote_ip, conn->remote_port);
        if((xconn = multicastd->getconn(multicastd, gid)))
        {
            conn->xids64[0] = xhead.id;
            conn->xids[0] = http_req->reqid;
            ACCESS_LOGGER(logger, "SEND_REQUIRE{%s %s key:%llu length:%d group[%d][%s:%d fd:%d]} from %s:%d ", http_methods[http_req->reqid].e, http_req->path, (uint64_t)(xhead.id), xhead.size, k, xconn->remote_ip, xconn->remote_port, xconn->fd, conn->remote_ip, conn->remote_port);
            if(http_req->reqid == HTTP_PUT)
                xhead.cmd = DBASE_REQ_REQUIRE;
            else
                xhead.cmd = DBASE_REQ_FIND;
            xhead.index = conn->index;
            xhead.length = 0;
            xconn->push_chunk(xconn, &xhead, sizeof(DBHEAD));
            xconn->over(xconn);
        }
        else
        {
            ACCESS_LOGGER(logger, "NO_CONN{%s %s key:%llu length:%d group[%d] from %s:%d}", http_methods[http_req->reqid].e, http_req->path, (uint64_t)(xhead.id), xhead.size, k, conn->remote_ip, conn->remote_port);
        }
    }
    return ret;
}

/* failed handler */
void httpd_fail_handler(int index)
{
    //char buf[HTTP_BUF_SIZE];
    CONN *conn  = NULL;

    if(index && (conn = httpd->findconn(httpd, index)))
    {
        conn->push_chunk(conn, HTTP_BAD_REQUEST, strlen(HTTP_BAD_REQUEST));
        conn->over(conn);
    }
    return ;
}
/* over handler */
void httpd_over_handler(int index, int64_t key)
{
    //char buf[HTTP_BUF_SIZE];
    CONN *conn  = NULL;

    if(index && (conn = httpd->findconn(httpd, index)) && key == conn->xids64[0])
    {
        ACCESS_LOGGER(logger, "over key:%llu", LLU(conn->xids64[0]));
        conn->push_chunk(conn, HTTP_NO_CONTENT, strlen(HTTP_NO_CONTENT));
        //conn->over_session(conn);
    }
    return ;
}

/* out data */
void httpd_out_handler(int index, char *data, int ndata)
{
    char buf[HTTP_BUF_SIZE];
    CONN *conn  = NULL;
    int n = 0;

    if(index && (conn = httpd->findconn(httpd, index)))
    {
        n = sprintf(buf, "HTTP/1.0 200 OK\r\nContent-Length: %d\r\n\r\n", ndata);
        conn->push_chunk(conn, buf, n);
        conn->push_chunk(conn, data, ndata);
        conn->over(conn);
    }
    return ;
}

/* packet handler */
int httpd_packet_handler(CONN *conn, CB_DATA *packet)
{
    char buf[HTTP_BUF_SIZE], file[HTTP_PATH_MAX], *p = NULL, *end = NULL;
    HTTP_REQ http_req = {0};
    int ret = -1, n = 0;
    struct stat st = {0};
    
    if(conn)
    {
        //TIMER_INIT(timer);
        p = packet->data;
        end = packet->data + packet->ndata;
        if(http_request_parse(p, end, &http_req, http_headers_map) == -1) goto err_end;
        if((n = http_req.headers[HEAD_ENT_CONTENT_LENGTH]) > 0
                && (n = atol(http_req.hlines + n)) > 0)
        {
            conn->save_cache(conn, &http_req, sizeof(HTTP_REQ));
            return conn->recv_chunk(conn, n);
        }
        //fprintf(stdout, "%s:%d headers:%s\r\n", __FILE__, __LINE__, p);
        if(strncasecmp(http_req.path, dbprefix, ndbprefix) == 0) 
            return httpd_request_handler(conn, &http_req, NULL, 0);
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
                p += sprintf(p, "Connection:Keep-Alive\r\n");
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
    HTTP_REQ httpRQ = {0}, *http_req = NULL;
    char *p = NULL, *end = NULL;
    int ret = -1;

    if(conn && packet && cache && chunk && chunk->ndata > 0)
    {
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
                //fprintf(stdout, "%s::%d request:%s\n", __FILE__, __LINE__, p);
                if(http_argv_parse(p, end, &httpRQ) == -1)goto err_end;
                //fprintf(stdout, "%s::%d OK\n", __FILE__, __LINE__);
            }
        }
err_end: 
        ret = conn->push_chunk(conn, HTTP_BAD_REQUEST, strlen(HTTP_BAD_REQUEST));
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
int traced_check_disk_groupid(char *ip, int port)
{
    int gid = -1, diskid = 0;
    SESSION session = {0};

    if((diskid = xmap_diskid(xmap, ip, port, &gid)) > 0 && gid == XM_NO_GROUPID)
    {
        memcpy(&session, &(traced->session), sizeof(SESSION));
        session.flags |= SB_NONBLOCK;
        gid = traced->addgroup(traced, ip, port, group_conns_limit, &session);
        xmap_set_groupid(xmap, diskid, gid);
    }
    return gid;
}

/* trackerd packet handler */
int trackerd_packet_handler(CONN *conn, CB_DATA *packet)
{
    int ret = -1, status = 0, qid = 0, gid = 0;
    DBHEAD *xhead = NULL;
    CONN *xconn = NULL;
    XMHOST xhost = {0};

    if(conn && packet && (xhead = (DBHEAD *)packet->data))
    {
        if(xhead->length > 0) conn->save_cache(conn, xhead, sizeof(DBHEAD));
        else
        {
            gid = multicasts[DBKMASK(xhead->id)].groupid;
            switch(xhead->cmd)
            {
                case DBASE_REQ_FIND:
                    if((qid = xmap_qid(xmap, xhead->id, &status, &xhost)) > 0)
                    {
                        xhead->qid = qid;
                        if(xhost.ip)
                        {
                           xhead->ip = xhost.ip;
                           xhead->port = xhost.port;
                           xhead->cmd |= DBASE_CMD_BASE;
                           conn->push_chunk(conn, xhead, sizeof(DBHEAD));
                        }
                        else if(status == XM_STATUS_WAIT)
                        {
                            conn->wait_evtimeout(conn, query_wait_time);
                        }
                        else
                        {
                            if((xconn = multicastd->getconn(multicastd, gid))) 
                            {
                                xhead->index = conn->index;
                                xconn->push_chunk(xconn, xhead, sizeof(DBHEAD));
                                xconn->over(xconn);
                            }
                        }
                        ret = 0;
                    }
                    break;
                case DBASE_REQ_REQUIRE:
                    if((xconn = multicastd->getconn(multicastd, gid))) 
                    {
                        xhead->index = conn->index;
                        xconn->push_chunk(xconn, xhead, sizeof(DBHEAD));
                        xconn->over(xconn);
                        ret = 0;
                    }
                    break;
            }
        }
    }
    return ret;
}

/* data handler */
int trackerd_data_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    int ret = -1;

    if(conn && packet)
    {

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
    XMHOST xhost = {0};

    if(conn && (xhead = (DBHEAD *)(conn->packet.data)))
    {
        if(xmap_check(xmap, xhead->qid, &xhost) > 0)
        {
            xhead->ip = xhost.ip;
            xhead->port = xhost.port;
            xhead->cmd |= DBASE_CMD_BASE;
            return conn->push_chunk(conn, xhead, sizeof(DBHEAD));
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

/* trasaction handler */
int traced_trans_handler(CONN *conn, int id)
{
    DBHEAD *xhead = NULL, *req = NULL;
    CB_DATA *chunk = NULL;

    if(conn && conn->cache.ndata > 0 && (xhead = (DBHEAD *)(conn->cache.data)))
    {
        if(xhead->cid > 0 && (xhead->length = xmap_cache_len(xmap, xhead->cid)) > 0
                && (chunk = conn->newchunk(conn, xhead->length+sizeof(DBHEAD))))
        { 
            if((req = (DBHEAD *)(chunk->data)) && (xhead->length = xmap_read_cache(xmap, 
                        xhead->cid, chunk->data+sizeof(DBHEAD))) > 0)
            {
                memcpy(req, xhead, sizeof(DBHEAD));
                //conn->push_chunk(conn, &xhead, sizeof(DBHEAD));
                if(conn->send_chunk(conn, chunk, xhead->length+sizeof(DBHEAD)) == 0)
                {
                    ACCESS_LOGGER(logger, "REQ{key:%llu length:%d} on %s:%d", (uint64_t)xhead->id, xhead->length, conn->remote_ip, conn->remote_port);
                    chunk = NULL;
                }
            }
            else
            {
                ACCESS_LOGGER(logger, "READ_CHUNK_FAIL{key:%llu cid:%d length:%d chunk:%p} on %s:%d", (uint64_t)xhead->id,  xhead->cid, xhead->length, chunk->data, conn->remote_ip, conn->remote_port);
            }
            if(chunk)conn->freechunk(conn, chunk);
        }
        else
        {
            ACCESS_LOGGER(logger, "NEW_CHUNK_FAIL{key:%llu cid:%d length:%d/%d} on %s:%d", (uint64_t)xhead->id,  xhead->cid, xhead->length, xmap_cache_len(xmap, xhead->cid), conn->remote_ip, conn->remote_port);
        }
    }
    return -1;
}

/* traced packet handler */
int traced_packet_handler(CONN *conn, CB_DATA *packet)
{
    DBHEAD *xhead = NULL;
    int ret = -1;

    if(conn && packet && (xhead = (DBHEAD *)packet->data))
    {
        ACCESS_LOGGER(logger, "RESP{key:%llu} on %s:%d", (uint64_t)xhead->id, conn->remote_ip, conn->remote_port);
        switch(xhead->cmd)
        {
            case DBASE_RESP_SET:
                xmap_drop_cache(xmap, xhead->cid);
                httpd_over_handler(xhead->index, xhead->id);
                ret = 0;
                break;
            default:
                break;
        }
        if(!conn->groupid) conn->over_session(conn);
    }
    return ret;
}

/* data handler */
int traced_data_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    DBHEAD *xhead = NULL;
    int ret = -1;
    //fprintf(stdout, "%s:%d OK\r\n", __FILE__, __LINE__);

    if(conn && packet && (xhead = (DBHEAD *)packet->data))
    {
        //fprintf(stdout, "%s::%d cmd:%d/%d\r\n", __FILE__, __LINE__, xhead->cmd, DBASE_RESP_GET);
        ACCESS_LOGGER(logger, "RESP_DATA{key:%llu} on %s:%d", (uint64_t)xhead->id, conn->remote_ip, conn->remote_port);
        if(xhead->cmd == DBASE_RESP_GET)
        {
            httpd_out_handler(xhead->index, chunk->data, chunk->ndata);
            ret = 0;
        }
        if(!conn->groupid) conn->over_session(conn);
    }
    return ret;
}

/* error handler */
int traced_error_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    int ret = -1;

    if(conn && packet)
    {

    }
    return ret;
}

/* quick handler */
int traced_quick_handler(CONN *conn, CB_DATA *packet)
{
    DBHEAD *head = NULL;
    int ret = -1;

    if(conn && packet && (head = (DBHEAD *)packet->data))
    {
        ACCESS_LOGGER(logger, "QUICK{cmd:%d key:%llu} on %s:%d", head->cmd, (uint64_t)head->id, conn->remote_ip, conn->remote_port);
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
        return conn->over(conn);
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
    DBHEAD *resp = NULL, xhead = {0};
    int ret = -1, gid = 0;
    CONN *xconn = NULL;
    XMHOST xhost = {0};

    if(conn && packet && (resp = (DBHEAD *)packet->data) 
            && resp->status == DBASE_STATUS_OK)
    {
        gid = traced_check_disk_groupid(conn->remote_ip, resp->port);
        ACCESS_LOGGER(logger, "Found{cmd:%d key:%llu cid:%d size:%d} on %s:%d", resp->cmd, (uint64_t)resp->id, resp->cid, resp->size, conn->remote_ip, resp->port);
        memcpy(&xhead, resp, sizeof(DBHEAD));
        xhead.cmd = 0;
        switch(resp->cmd)
        {
            case DBASE_RESP_FIND:
                if(resp->size > 0)
                {
                    xhost.ip = resp->ip;
                    xhost.port = resp->port;
                    //ret = xmap_over(xmap, resp->qid, &xhost);
                    xhead.cmd = DBASE_REQ_GET;
                    xhead.length = 0;
                    xhead.size = 0;
                    xhead.cid = 0;
                }
                break;
            case DBASE_RESP_REQUIRE:
                xhead.cmd = DBASE_REQ_SET;
                //xhead.size = 0;
                break;
        }
        //fprintf(stdout, "%s::%d cmd:%d|%d/%d/%d/%d\r\n", __FILE__, __LINE__, resp->cmd, DBASE_RESP_REQUIRE, xhead.cmd, DBASE_REQ_SET, DBASE_REQ_GET);
        if(xhead.cmd)
        {
            if(!(xconn = traced->getconn(traced, gid)))
                xconn = traced->newconn(traced, -1, -1, conn->remote_ip, resp->port, NULL);
            if(xconn)
            {
                if(resp->cid > 0)
                {
                    xconn->save_cache(xconn, &xhead, sizeof(DBHEAD));
                    traced->newtransaction(traced, xconn, xhead.cmd);
                }
                else
                {
                    ACCESS_LOGGER(logger, "READY_GET{cmd:%d key:%llu cid:%d size:%d} on %s:%d", xhead.cmd, (uint64_t)xhead.id, xhead.cid, xhead.size, conn->remote_ip, resp->port);
                    xconn->push_chunk(conn, &xhead, sizeof(DBHEAD));

                }
                ret = 0;
            }
            else
            {
                ACCESS_LOGGER(logger, "NO_FREE_CONN[%s:%d]{key:%llu cid:%d}", 
                        conn->remote_ip, resp->port, (uint64_t)resp->id, resp->cid);
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
        //conn->over_cstate(conn);
        //return conn->over(conn);
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
        for(i = 0; i < DBASE_MASK; i++)
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
    int n = 0;

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
    traced->session.packet_type = PACKET_CERTAIN_LENGTH;
    traced->session.packet_length = sizeof(DBHEAD);
    traced->session.buffer_size = iniparser_getint(dict, "TRACED:buffer_size", SB_BUF_SIZE);
    traced->session.packet_handler = &traced_packet_handler;
    traced->session.transaction_handler = &traced_trans_handler;
    traced->session.quick_handler = &traced_quick_handler;
    traced->session.data_handler = &traced_data_handler;
    traced->session.timeout_handler = &traced_timeout_handler;
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
    memset(multicasts, 0, sizeof(MGROUP) * DBASE_MASK);
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
    if(logger){LOGGER_CLEAN(logger);}
    if(httpd_index_html_code) free(httpd_index_html_code);
    if(xmap) xmap_clean(xmap);
    return 0;
}
