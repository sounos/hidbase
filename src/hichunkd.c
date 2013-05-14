#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include <sys/resource.h>
#include <locale.h>
#include <sbase.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "stime.h"
#include "base64.h"
#include "base64chunkdhtml.h"
#include "http.h"
#include "iniparser.h"
#include "logger.h"
#include "mtrie.h"
#include "dbase.h"
#include "xdbase.h"
#include "md5.h"
#include "xmm.h"
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
#define HTTP_RESP_OK            "HTTP/1.0 200 OK\r\n\r\n"
#define HTTP_BAD_REQUEST        "HTTP/1.0 400 Bad Request\r\n\r\n"
#define HTTP_NOT_FOUND          "HTTP/1.0 404 Not Found\r\n\r\n" 
#define HTTP_NOT_MODIFIED       "HTTP/1.0 304 Not Modified\r\n\r\n"
#define HTTP_NO_CONTENT         "HTTP/1.0 204 No Content\r\n\r\n"
#ifndef LL
#define LL(x) ((long long int)x)
#endif
typedef struct _DBTASK{int status;}DBTASK;
typedef struct _DBMASK{int diskid;int mask;char ip[SB_IP_MAX];}DBMASK;
static char *http_default_charset = "UTF-8";
static char *httpd_home = NULL;
static int is_inside_html = 1;
static unsigned char *httpd_index_html_code = NULL;
static int  nhttpd_index_html_code = 0;
static SBASE *sbase = NULL;
static SERVICE *httpd = NULL;
static SERVICE *traced = NULL;
static SERVICE *multicastd = NULL;
static dictionary *dict = NULL;
static void *http_headers_map = NULL;
static void *logger = NULL;
static XDBASE *xdb = NULL;
static SERVICE *dbs[DBASE_MASK];
static DBTASK dbtasks[DBASE_MASK];
static DBMASK dbmasks[DBASE_MASK];
static char *public_multicast = "233.8.8.8";
static int public_multicast_added = 0;
static void *argvmap = NULL;
static int ndbs = 0;
#define E_OP_ADD         0x01
#define E_OP_DEL         0x02
#define E_OP_UPDATE      0x03
#define E_OP_LIST        0x04
#define E_OP_GET         0x05
#define E_OP_ADD_MASK    0x06
#define E_OP_DEL_MASK    0x07
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
int chunkd_add_service(int diskid, int port);
int chunkd_add_mask(int diskid, int mask);
int chunkd_drop_mask(int diskid, int mask);
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
char hex2int(unsigned char hex)
{
    hex = hex - '0';
    if (hex > 9) {
        hex = (hex + '0' - 1) | 0x20;
        hex = hex - 'a' + 11;
    }
    if (hex > 15)
        hex = 0xFF;

    return hex;
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
            high = hex2int(*(p + 1));                                                   \
            if (high != 0xFF)                                                           \
            {                                                                           \
                low = hex2int(*(p + 2));                                                \
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
                conn->push_chunk(conn, httpd_index_html_code, nhttpd_index_html_code);
                return conn->over(conn);
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
                        conn->push_chunk(conn, HTTP_NO_CONTENT, strlen(HTTP_NO_CONTENT));
                        return conn->over(conn);
                    }
                    else if((n = http_req.headers[HEAD_REQ_IF_MODIFIED_SINCE]) > 0
                            && str2time(http_req.hlines + n) == st.st_mtime)
                    {
                        conn->push_chunk(conn, HTTP_NOT_MODIFIED, strlen(HTTP_NOT_MODIFIED));
                        return conn->over(conn);
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
                        conn->push_file(conn, file, 0, st.st_size);
                        return conn->over(conn);
                    }
                }
                else
                {
                    conn->push_chunk(conn, HTTP_NOT_FOUND, strlen(HTTP_NOT_FOUND));
                    return conn->over(conn);
                }
            }
            else
            {
                conn->push_chunk(conn, HTTP_BAD_REQUEST, strlen(HTTP_BAD_REQUEST));
                return conn->over(conn);
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
        return conn->over(conn);
    }
    return ret;
}

/*
POST / HTTP/1.0
Content-Length: 37

op=1&diskid=1&id=-1923868782271024499
*/
/*  data handler */
int httpd_data_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    char *p = NULL, *end = NULL, *path = NULL, *url = NULL, *keys = NULL, *key = NULL, 
         buf[HTTP_BUF_SIZE], line[HTTP_LINE_SIZE], uri[HTTP_URL_PATH_MAX];
    int ret = -1, id = 0, op = 0, n = 0, i = 0, j = 0, diskid = -1, nout = 0, 
        port = 0, mode = -1, x = 0, mask = 0;
    HTTP_REQ httpRQ = {0}, *http_req = NULL;
    int64_t kid = 0, tab[XDB_KEYS_MAX];
    unsigned char digest[MD5_LEN];
    CB_DATA *block = NULL;
    BREC record = {0};
    off_t limit = -1;

    if(conn && packet && cache && chunk && chunk->ndata > 0)
    {
        if((http_req = (HTTP_REQ *)cache->data))
        {
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
                                    mask = (int)inet_addr(p);
                                    break;
                            }
                        }
                    }
                }
                if(op == E_OP_ADD)
                {
                    if(path && port > 0 && limit >= 0 && mode >= 0 
                            && (diskid = xdbase_add_disk(xdb, port, limit, mode, path)) >= 0) 
                    {
                        if(dbs[diskid] == NULL) chunkd_add_service(diskid, port);
                        goto disklist;
                    }
                    else goto err_end;
                }
                else if(op == E_OP_DEL)
                {
                    goto err_end;
                    if(diskid >= 0 && xdbase_del_disk(xdb, diskid) >= 0)
                    {
                        if(dbs[diskid]) 
                        {
                            dbs[diskid]->close(dbs[diskid]);
                            for(j = 0; j < DBASE_MASK; j++)
                            {
                                if(dbmasks[j].diskid == diskid)
                                    chunkd_drop_mask(diskid, dbmasks[j].mask);
                            }
                            dbs[diskid] = NULL;
                        }
                        goto disklist;
                    }
                    else goto err_end;
                }
                else if(op == E_OP_UPDATE)
                {
                    if(diskid >= 0 && (limit >= 0 || mode >= 0))
                    {
                        if(limit >= 0) xdbase_set_disk_limit(xdb, diskid, limit);
                        if(mode >= 0) xdbase_set_disk_mode(xdb, diskid, mode);
                        goto disklist;
                    }
                    else goto err_end;
                }
                else if(op == E_OP_ADD_MASK)
                {
                    if(diskid >= 0 && mask && chunkd_add_mask(diskid, mask) == 0
                            && xdbase_add_mask(xdb, diskid, mask) >= 0)
                    {
                        goto disklist;
                    }
                    else goto err_end;
                }
                else if(op == E_OP_DEL_MASK)
                {
                    if(diskid >= 0 && mask && xdbase_del_mask(xdb, diskid, mask) >= 0)
                    {
                        chunkd_drop_mask(diskid, mask);
                        goto disklist;
                    }
                    else goto err_end;
                }
                else if(op == E_OP_LIST) goto disklist;
                else if(op == E_OP_GET)
                {
                    if(url) 
                    {
                        base64_decode((unsigned char *)uri, (const char *)url, strlen(url));
                        md5((unsigned char *)uri, strlen(uri), digest);
                        kid = *((int64_t *)digest);
                    }
                    if(key) kid = atoll(key);
                    if((p = keys))
                    {
                        while(*p != '\0' && x < XDB_KEYS_MAX)
                        {
                            while(*p != '\0' && *p != '-' && (*p < '0' || *p > '9'))++p;
                            if(*p == '-' || (*p >= '0' && *p <= '9')) tab[x++] = atoll(p);
                            while(*p == '-' || (*p >= '0' && *p <= '9'))++p;
                        }
                    }
                    if(diskid >= 0 && diskid <= DBASE_MASK)
                    {
                        if(kid != 0)
                        {
                            memset(&record, 0, sizeof(BREC));
                            if((n = xdbase_bound(xdb, diskid, 1)) > 0 
                                    && (record.ndata = xdbase_get_data(xdb, diskid, kid, 
                                        &(record.data))) > 0 && record.data)
                            {
                                if((block = conn->newchunk(conn, n)))
                                {
                                    nout = bjson_json(&record, block->data);
                                    p = buf;
                                    p += sprintf(p, "HTTP/1.0 200 OK\r\nContent-Length: %d\r\n"
                                            "Content-Type: text/html;charset=%s\r\n",
                                            nout, http_default_charset);
                                    if((n = http_req->headers[HEAD_GEN_CONNECTION]) > 0)
                                        p += sprintf(p, "Connection: %s\r\n", (http_req->hlines + n));
                                    else
                                        p += sprintf(p, "Connection: Keep-Alive\r\n");
                                    p += sprintf(p, "\r\n");
                                    conn->push_chunk(conn, buf, (p - buf));
                                    if(conn->send_chunk(conn, block, nout) != 0)
                                        conn->freechunk(conn, block);
                                    ret = 0;
                                }
                                xdbase_free_data(xdb, diskid, record.data, record.ndata);
                            }
                        }
                        else if(keys && x > 0 && (n = xdbase_bound(xdb, diskid, x)) > 0
                            && (block = conn->newchunk(conn, n)) && (p = block->data))
                        {
                            *p++ = '(';*p++ = '{';
                            for(i = 0; i < x; i++)
                            {
                                memset(&record, 0, sizeof(BREC));
                                if((record.ndata = xdbase_get_data(xdb, diskid, tab[i], 
                                                &(record.data))) > 0 && record.data)
                                {
                                    p += sprintf(p, "\"%lld\":", LL(tab[i]));
                                    p += bjson_to_json(&record, p);
                                    if((i+1) < x) *p++ = ',';
                                    xdbase_free_data(xdb, diskid, record.data, record.ndata);
                                }
                            }
                            *p++ = '}';*p++ = ')';
                            *p = '\0';
                            nout = p - block->data;
                            p = buf;
                            p += sprintf(p, "HTTP/1.0 200 OK\r\nContent-Length: %d\r\n"
                                    "Content-Type: text/html;charset=%s\r\n",
                                    nout, http_default_charset);
                            if((n = http_req->headers[HEAD_GEN_CONNECTION]) > 0)
                                p += sprintf(p, "Connection: %s\r\n", (http_req->hlines + n));
                            else
                                p += sprintf(p, "Connection: Keep-Alive\r\n");
                            p += sprintf(p, "\r\n");
                            conn->push_chunk(conn, buf, (p - buf));
                            if(conn->send_chunk(conn, block, nout) != 0)
                                conn->freechunk(conn, block);
                            ret = 0;
                        }
                    }
                }
                if(ret >= 0) return conn->over(conn);
                else goto err_end;

disklist:
                if((ret = xdbase_list_disks(xdb, line)) > 0)
                {
                    p = buf;
                    p += sprintf(p, "HTTP/1.0 200 OK\r\nContent-Length: %d\r\n"
                            "Content-Type: text/html;charset=%s\r\n",
                            ret, http_default_charset);
                    if((n = http_req->headers[HEAD_GEN_CONNECTION]) > 0)
                        p += sprintf(p, "Connection: %s\r\n", (http_req->hlines + n));
                    p += sprintf(p, "Connection:Keep-Alive\r\n");
                    p += sprintf(p, "\r\n");
                    conn->push_chunk(conn, buf, (p - buf));
                    conn->push_chunk(conn, line, ret);
                    return conn->over(conn);
                }
            }
        }
err_end: 
        ret = conn->push_chunk(conn, HTTP_BAD_REQUEST, strlen(HTTP_BAD_REQUEST));
        return conn->over(conn);
    }
    return ret;
}

/* httpd timeout handler */
int httpd_timeout_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    if(conn)
    {
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

/* chunkd packet handler */
int chunkd_packet_handler(CONN *conn, CB_DATA *packet)
{
    int ret = -1, diskid = -1, n = 0;
    DBHEAD *xhead = NULL, *resp = NULL;
    CB_DATA *chunk = NULL;
    char *p = NULL;

    if(conn && packet && (xhead = (DBHEAD *)packet->data) 
        && (diskid = conn->get_service_id(conn)) >= 0)
    {
        if(xhead->length > 0)
        {
            //conn->save_cache(conn, xhead, sizeof(DBHEAD));
            //fprintf(stdout, "%s::%d cmd:%d id:%lld length:%d\n", __FILE__, __LINE__, xhead->id, xhead->cmd, xhead->length);
            return conn->recv_chunk(conn, xhead->length);
            /*
            if(xhead->cmd == DBASE_REQ_GET)
                return conn->recv_chunk(conn, xhead->length);
            else
                return conn->recv2_chunk(conn, xhead->length, (char *)xhead, sizeof(DBHEAD));
            if(xhead->cmd == DBASE_REQ_GET)
            {
                return conn->recv_chunk(conn, xhead->length);
            }
            else
            {
                return conn->recv2_chunk(conn, xhead->length, (char *)xhead, sizeof(DBHEAD));
            }
            */
        }
        else
        {
            switch(xhead->cmd)
            {
                case DBASE_REQ_DELETE:
                    //ret = xdbase_qwait(xdb, diskid, (char *)xhead, sizeof(DBHEAD));
                    ret = xdbase_del_data(xdb, diskid, xhead->id);
                    goto end;
                    break;
                case DBASE_REQ_GET:
                    if((n = xdbase_bound(xdb, diskid, 1)) > 0
                        && (chunk = conn->newchunk(conn, n + sizeof(DBHEAD))))
                    {
                        resp = (DBHEAD *)chunk->data;
                        memcpy(resp, xhead, sizeof(DBHEAD));
                        p = chunk->data + sizeof(DBHEAD);
                        if((n = xdbase_read_data(xdb, diskid, xhead->id, p)) > 0)
                        {
                            resp->cmd = DBASE_RESP_GET;
                            resp->length = n;
                            return conn->send_chunk(conn, chunk, (sizeof(DBHEAD) + n));
                        }
                        else
                        {
                            conn->freechunk(conn, chunk);
                            goto err;
                        }
                    }
                    else 
                        goto err;
                    break;
                case DBASE_REQ_COPY:
                    break;
                case DBASE_REQ_STATE:
                    break;
            }
end:
            xhead->cmd += DBASE_CMD_BASE;
            xhead->status = DBASE_STATUS_OK;
            xhead->length = 0;
            return conn->push_chunk(conn, xhead, sizeof(DBHEAD));

err:
            xhead->cmd += DBASE_CMD_BASE;
            xhead->status = DBASE_STATUS_ERR;
            xhead->length = 0;
            return conn->push_chunk(conn, xhead, sizeof(DBHEAD));
        }
    }
    return ret;
}

/* data handler */
int chunkd_data_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    int ret = -1, diskid = -1, n = 0, len = 0;
    char *data = NULL, *end = NULL, *p = NULL;
    DBHEAD *xhead = NULL, *resp = NULL;
    CB_DATA *xchunk = NULL;
    DBMETA *meta = NULL;
    int64_t id = 0;

    if(conn && packet && (xhead = (DBHEAD *)(packet->data)) 
            && chunk && chunk->data && chunk->ndata > 0
            && (diskid = conn->get_service_id(conn)) >= 0)
    {
        if(xhead->cmd == DBASE_REQ_GET)
        {
            n = chunk->ndata/sizeof(int64_t);
            if((len = xdbase_bound(xdb, diskid, n)) > 0
                    && (len += (n * sizeof(DBMETA) + sizeof(DBHEAD))) > 0
                    && (xchunk = conn->newchunk(conn, len)))
            {
                resp = (DBHEAD *)xchunk->data;
                data = xchunk->data + sizeof(DBHEAD);
                p = chunk->data;
                end = chunk->data + chunk->ndata;
                while(p < end)
                {
                    id = *((int64_t*)p);
                    meta = (DBMETA *)data;
                    data += sizeof(DBMETA);
                    if((n = xdbase_read_data(xdb, diskid, id, data)) > 0)
                    {
                        meta->id = id;
                        meta->length = n;
                        data += n;
                    }
                    else
                    {
                        //ERROR_LOGGER(logger, "get_data(%lld) failed, %s", LL(id), strerror(errno));
                        conn->freechunk(conn, xchunk);
                        goto end;
                        //data = (char *)meta;
                    }
                    p += sizeof(int64_t);
                }
                len = (data - xchunk->data);
                resp->cmd = DBASE_RESP_GET;
                resp->length = len - sizeof(DBHEAD); 
                resp->status = DBASE_STATUS_OK;
                if(conn->send_chunk(conn, xchunk, len) != 0)
                {
                    conn->freechunk(conn, xchunk);
                    goto end;
                }
                return 0;
            }
        }
        else if(xhead->cmd == DBASE_REQ_UPBATCH)
        {
            data = chunk->data;
            end = chunk->data + chunk->ndata;
            while((data+sizeof(DBMETA)) <= end)
            {
                meta = (DBMETA *)data;
                data += sizeof(DBMETA);
                if((data+meta->length) <= end)
                {
                    if(xhead->cmd == DBASE_REQ_APPEND)
                        ret = xdbase_add_data(xdb, diskid, xhead->id, data, meta->length); 
                    else if(xhead->cmd == DBASE_REQ_UPDATE)
                        ret = xdbase_set_data(xdb, diskid, xhead->id, data, meta->length); 
                }
                else break;
            }
            ret = 0;
        }
        else if(xhead->cmd == DBASE_REQ_SET)
        {
            //fprintf(stdout, "%s::%d diskid:%d id:%lld ndata:%d\n", __FILE__, __LINE__,  diskid, LL(xhead->id), chunk->ndata);
            ret = xdbase_set_data(xdb, diskid, xhead->id, chunk->data, chunk->ndata); 
            //ACCESS_LOGGER(logger, "data_handler() conn:%p packet:%p from [%s:%d] via %d", conn, packet, conn->remote_ip, conn->remote_port, conn->fd);
            ACCESS_LOGGER(logger, "over_set_data(diskid:%d id:%lld ndata:%d):%d", diskid, LL(xhead->id), chunk->ndata, ret);
        }
        else if(xhead->cmd == DBASE_REQ_APPEND)
        {
            ret = xdbase_add_data(xdb, diskid, xhead->id, chunk->data, chunk->ndata); 
        }
        else if(xhead->cmd == DBASE_REQ_UPDATE)
        {
            ret = xdbase_update_data(xdb, diskid, xhead->id, chunk->data, chunk->ndata); 
        }
        /*
        else if(xhead->cmd == DBASE_REQ_SET 
                || xhead->cmd == DBASE_REQ_UPBATCH 
                || xhead->cmd == DBASE_REQ_APPEND 
                || xhead->cmd == DBASE_REQ_UPDATE 
                || xhead->cmd == DBASE_REQ_DELETE)
        {
            ret = xdbase_qwait(xdb, diskid, chunk->data, chunk->ndata); 
        }
        */
end:
        xhead->length = 0;
        xhead->cmd |= DBASE_CMD_BASE;
        xhead->status = DBASE_STATUS_OK;
        if(ret < 0) xhead->status = DBASE_STATUS_ERR;
        return conn->push_chunk(conn, xhead, sizeof(DBHEAD));
    }
    else
    {
        conn->close(conn);
    }
    return ret;
}

/* quick handler */
int chunkd_quick_handler(CONN *conn, CB_DATA *packet)
{
    DBHEAD *head = NULL;
    int ret = -1;

    if(conn && packet && (head = (DBHEAD *)packet->data))
    {
        ret = head->length;
    }
    return ret;
}

/* task handler */
void chunkd_task_handler(void *arg)
{
    SERVICE *s = NULL;
    int diskid = 0;

    if((diskid = ((int)((long)arg) - 1)) >= 0 && diskid < DBASE_MASK)
    {
        while(xdbase_work(xdb, diskid) > 0 && dbtasks[diskid].status > 0
            && (s = dbs[diskid]) && s->lock != 1)
        {
            s->newtask(s, &chunkd_task_handler, (void *)((long)diskid));
        }
        if(dbs[diskid]) dbtasks[diskid].status = 0;
    }
    return ;
}

/* error handler */
int chunkd_error_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    int ret = -1;

    if(conn && packet)
    {
        conn->close(conn);
    }
    return ret;
}


/* httpd timeout handler */
int chunkd_timeout_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *chunk)
{
    if(conn)
    {
        return conn->over(conn);
    }
    return -1;
}

/* OOB data handler for httpd */
int chunkd_oob_handler(CONN *conn, CB_DATA *oob)
{
    if(conn)
    {
        return 0;
    }
    return -1;
}

/* traced packet handler */
int traced_packet_handler(CONN *conn, CB_DATA *packet)
{
    int ret = -1, diskid = -1;
    DBHEAD *xhead = NULL;

    if(conn && packet && (xhead = (DBHEAD *)packet->data))
    {
        if(xhead->length > 0) conn->save_cache(conn, xhead, xhead->length);
        else
        {
            switch(xhead->cmd)
            {
                case DBASE_REQ_APPEND:
                {
                    diskid = dbmasks[DBKMASK(xhead->id)].diskid; 
                    if(diskid >= 0 && diskid < DBASE_MASK && dbs[diskid])
                    {
                        //request new xhead->length space
                    }
                    break;
                }
                case DBASE_REQ_FIND:
                    break;
                case DBASE_REQ_COPY:
                    break;
                case DBASE_REQ_STATE:
                    break;
            }
        }
    }
    return ret;
}

/* data handler */
int traced_data_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *trace)
{
    int ret = -1;

    if(conn && packet)
    {

    }
    return ret;
}

/* error handler */
int traced_error_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *trace)
{
    int ret = -1;

    if(conn && packet)
    {

    }
    return ret;
}


/* httpd timeout handler */
int traced_timeout_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *trace)
{
    if(conn)
    {
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
    int ret = -1, i = 0, mask = 0, diskid = 0;
    DBHEAD *xhead = NULL, resp = {0};
    char buf[HTTP_BUF_SIZE];
    MDISK *disk = NULL;

    if(conn && packet && (xhead = (DBHEAD *)(packet->data)))
    {
        if(xhead->length > 0) 
        {
            return conn->recv_chunk(conn, xhead->length);
        }
        else
        {
            if(xhead->cmd == DBASE_REQ_REPORT)
            {
                if(ndbs > 0)
                {
                    xhead = (DBHEAD *)buf;
                    xhead->cmd = DBASE_RESP_REPORT;
                    disk = (MDISK *)(buf + sizeof(DBHEAD));
                    for(i = 0; i < DBASE_MASK; i++)
                    {
                        if(xdb->state->xdisks[i].status)
                        {
                            disk->port = xdb->state->xdisks[i].port;
                            disk->total = xdb->state->xdisks[i].total;
                            disk->limit = xdb->state->xdisks[i].limit;
                            disk->free = xdb->state->xdisks[i].free;
                            ++disk;
                        }
                    }
                    xhead->length = (char *)disk - buf - sizeof(DBHEAD);
                    conn->push_chunk(conn, buf, (char *)disk - buf);
                }
            }
            else
            {
                ACCESS_LOGGER(logger, "REQ{cmd:%d id:%llu size:%d} via %d", xhead->cmd, (uint64_t)xhead->id, xhead->size, conn->fd);
                mask = DBKMASK(xhead->id);
                diskid = dbmasks[mask].diskid;
                if(diskid >= 0 && dbs[diskid])
                {
                    memcpy(&resp, xhead, sizeof(DBHEAD));
                    resp.size = 0;
                    resp.port =  dbs[diskid]->port;
                    resp.cmd  = xhead->cmd | DBASE_CMD_BASE;
                    resp.status = DBASE_STATUS_ERR;
                    switch(xhead->cmd)
                    {
                        case DBASE_REQ_FIND:
                            if((resp.size = xdbase_get_data_len(xdb, diskid, xhead->id)) > 0)
                            {
                                resp.status = DBASE_STATUS_OK;
                            }
                            break;
                        case DBASE_REQ_REQUIRE:
                            if((resp.size = xdbase_get_data_len(xdb, diskid, xhead->id)) > 0
                                    || xdbase_check_disk(xdb, diskid, xhead->id, xhead->size) == 0)
                            {
                                resp.status = DBASE_STATUS_OK;
                            }
                            break;
                        default:
                            break;
                    }
                    //fprintf(stdout, "%s::%d OK\r\n", __FILE__, __LINE__);
                    ret = conn->push_chunk(conn, &resp, sizeof(DBHEAD));
                    ACCESS_LOGGER(logger, "SEND{cmd:%d id:%llu size:%d} via %d", resp.cmd, (uint64_t)resp.id, resp.size, conn->fd);
                }
            }
            conn->over(conn);
        }
    }
    return ret;
}

/* data handler */
int multicastd_data_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *multicast)
{
    int ret = -1;

    if(conn && packet)
    {

    }
    return ret;
}

/* error handler */
int multicastd_error_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *multicast)
{
    int ret = -1;

    if(conn && packet)
    {
        ret = 0;
    }
    return ret;
}


/* httpd timeout handler */
int multicastd_timeout_handler(CONN *conn, CB_DATA *packet, CB_DATA *cache, CB_DATA *multicast)
{
    if(conn)
    {
        return conn->over(conn);
    }
    return -1;
}

/* OOB data handler for httpd */
int multicastd_oob_handler(CONN *conn, CB_DATA *oob)
{
    return -1;
}

/* add mask */
int chunkd_add_mask(int diskid, int mask)
{
    unsigned char *ch = (unsigned char *)((void *)&(mask));
    int k = DBKMASK(ch[3]);

    if(dbmasks[k].diskid == -1)
    {
        dbmasks[k].diskid = diskid;
        dbmasks[k].mask = mask;
        sprintf(dbmasks[k].ip, "%d.%d.%d.%d", ch[0], ch[1], ch[2], ch[3]);
        multicastd->add_multicast(multicastd, dbmasks[k].ip);
        return 0;
    }
    return -1;
}

/* drop mask */
int chunkd_drop_mask(int diskid, int mask)
{
    unsigned char *ch = (unsigned char *)&(mask);
    int k  = 0;

    if(diskid >= 0 && diskid < DBASE_MASK && mask)
    {
        k = DBKMASK(ch[3]);
        multicastd->drop_multicast(multicastd, dbmasks[k].ip);
        dbmasks[k].diskid = -1;
        dbmasks[k].mask = 0;
    }
    return 0;
}

/* add service */
int chunkd_add_service(int diskid, int port)
{
    char path[HTTP_PATH_MAX];
    SERVICE *chunkd = NULL;
    int n = 0, ret = 0;

    /* chunkd */
    if((chunkd = service_init()) == NULL)
    {
        fprintf(stderr, "Initialize service failed, %s", strerror(errno));
        _exit(-1);
    }
    chunkd->id = diskid;
    chunkd->family = iniparser_getint(dict, "CHUNKD:inet_family", AF_INET);
    chunkd->sock_type = iniparser_getint(dict, "CHUNKD:socket_type", SOCK_STREAM);
    chunkd->ip = iniparser_getstr(dict, "CHUNKD:service_ip");
    chunkd->port = port;
    chunkd->working_mode = iniparser_getint(dict, "CHUNKD:working_mode", WORKING_PROC);
    chunkd->service_type = iniparser_getint(dict, "CHUNKD:service_type", S_SERVICE);
    chunkd->service_name = iniparser_getstr(dict, "CHUNKD:service_name");
    chunkd->nprocthreads = iniparser_getint(dict, "CHUNKD:nprocthreads", 8);
    chunkd->ndaemons = iniparser_getint(dict, "CHUNKD:ndaemons", 0);
    chunkd->niodaemons = iniparser_getint(dict, "CHUNKD:niodaemons", 1);
    chunkd->use_cond_wait = iniparser_getint(dict, "CHUNKD:use_cond_wait", 0);
    if(iniparser_getint(dict, "CHUNKD:use_cpu_set", 0) > 0) chunkd->flag |= SB_CPU_SET;
    if(iniparser_getint(dict, "CHUNKD:event_lock", 0) > 0) chunkd->flag |= SB_EVENT_LOCK;
    if(iniparser_getint(dict, "CHUNKD:newconn_delay", 0) > 0) chunkd->flag |= SB_NEWCONN_DELAY;
    if(iniparser_getint(dict, "CHUNKD:tcp_nodelay", 0) > 0) chunkd->flag |= SB_TCP_NODELAY;
    if(iniparser_getint(dict, "CHUNKD:socket_linger", 0) > 0) chunkd->flag |= SB_SO_LINGER;
    if(iniparser_getint(dict, "CHUNKD:while_send", 0) > 0) chunkd->flag |= SB_WHILE_SEND;
    if(iniparser_getint(dict, "CHUNKD:log_thread", 0) > 0) chunkd->flag |= SB_LOG_THREAD;
    if(iniparser_getint(dict, "CHUNKD:use_outdaemon", 0) > 0) chunkd->flag |= SB_USE_OUTDAEMON;
    if(iniparser_getint(dict, "CHUNKD:use_evsig", 0) > 0) chunkd->flag |= SB_USE_EVSIG;
    if(iniparser_getint(dict, "CHUNKD:use_cond", 0) > 0) chunkd->flag |= SB_USE_COND;
    if((n = iniparser_getint(dict, "CHUNKD:sched_realtime", 0)) > 0) chunkd->flag |= (n & (SB_SCHED_RR|SB_SCHED_FIFO));
    if((n = iniparser_getint(dict, "CHUNKD:io_sleep", 0)) > 0) chunkd->flag |= ((SB_IO_NANOSLEEP|SB_IO_USLEEP|SB_IO_SELECT) & n);
    chunkd->nworking_tosleep = iniparser_getint(dict, "CHUNKD:nworking_tosleep", SB_NWORKING_TOSLEEP);
    sprintf(path, "%s/chunkd_%d.log", iniparser_getstr(dict, "CHUNKD:logdir"), port);
    chunkd->set_log(chunkd, path);
    chunkd->set_log_level(chunkd, iniparser_getint(dict, "CHUNKD:log_level", 0));
    chunkd->session.flags = SB_NONBLOCK;
    chunkd->session.packet_type = PACKET_CERTAIN_LENGTH;
    chunkd->session.packet_length = sizeof(DBHEAD);
    chunkd->session.buffer_size = iniparser_getint(dict, "CHUNKD:buffer_size", SB_BUF_SIZE);
    chunkd->session.packet_handler = &chunkd_packet_handler;
    chunkd->session.data_handler = &chunkd_data_handler;
    chunkd->session.quick_handler = &chunkd_quick_handler;
    chunkd->session.timeout_handler = &chunkd_timeout_handler;
    chunkd->session.error_handler = &chunkd_error_handler;
    dbs[diskid] = chunkd;
    dbtasks[diskid].status = 0;
    ndbs++;
    ret = sbase->add_service(sbase, chunkd);
    if(sbase->running_status > 0) ret = sbase->run_service(sbase, chunkd);
    return ret;
}

/* running */
void multicastd_onrunning(SERVICE *service)
{
    int i = 0, j = 0;

    if(service == multicastd)
    {
        if(public_multicast && !public_multicast_added)
        {
            multicastd->add_multicast(multicastd, public_multicast);
            public_multicast_added = 1;
        }
        /* db service */
        if(xdb->state && xdb->state->nxdisks > 0 && ndbs == 0) 
        {
            for(i = 0; i < DBASE_MASK; i++)
            {
                if(xdb->state->xdisks[i].status && dbs[i] == NULL)
                {
                    chunkd_add_service(i, xdb->state->xdisks[i].port);
                    if(xdb->state->xdisks[i].nmasks > 0)
                    {
                        for(j = 0; j < DBASE_MASK; j++)
                        {
                            if(xdb->state->xdisks[i].masks[j].mask_ip)
                                chunkd_add_mask(i, xdb->state->xdisks[i].masks[j].mask_ip);
                        }
                    }
                }
            }

        }
    }
    return ;
}

/* heartbeat */
void cb_heartbeat_handler(void *arg)
{
    int i = 0;
    for(i = 0; i < DBASE_MASK; i++)
    {
        if(dbs[i] && dbtasks[i].status == 0)
        {
            dbtasks[i].status = 1;
            dbs[i]->newtask(dbs[i], &chunkd_task_handler, (void *)((long)(i+1)));
        }
    }
    return ;
}

/* Initialize from ini file */
int sbase_initialize(SBASE *sbase, char *conf)
{
    int i = 0, n = 0, port = 0, mode = 0, interval = 0, ret = -1;
    char *s = NULL, *p = NULL, *disk = NULL;
    off_t limit = 0;

    if((dict = iniparser_new(conf)) == NULL)
    {
        fprintf(stderr, "Initializing conf:%s failed, %s\n", conf, strerror(errno));
        _exit(-1);
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

    /* SBASE */
    sbase->nchilds = iniparser_getint(dict, "SBASE:nchilds", 0);
    sbase->connections_limit = iniparser_getint(dict, "SBASE:connections_limit", SB_CONN_MAX);
    setrlimiter("RLIMIT_NOFILE", RLIMIT_NOFILE, sbase->connections_limit);
    sbase->usec_sleep = iniparser_getint(dict, "SBASE:usec_sleep", SB_USEC_SLEEP);
    sbase->set_log(sbase, iniparser_getstr(dict, "SBASE:logfile"));
    sbase->set_evlog(sbase, iniparser_getstr(dict, "SBASE:evlogfile"));
    sbase->set_evlog_level(sbase, iniparser_getint(dict, "SBASE:evlog_level", 0));
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
    httpd->port = iniparser_getint(dict, "HTTPD:service_port", 1080);
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
    LOGGER_INIT(logger, iniparser_getstr(dict, "HTTPD:access_log"));
    LOGGER_SET_LEVEL(logger, iniparser_getint(dict, "HTTPD:access_log_level", 0));
    /* multicastd */
    if((multicastd = service_init()) == NULL)
    {
        fprintf(stderr, "Initialize service failed, %s", strerror(errno));
        _exit(-1);
    }
    multicastd->family = iniparser_getint(dict, "MULTICASTD:inet_family", AF_INET);
    multicastd->sock_type = iniparser_getint(dict, "MULTICASTD:socket_type", SOCK_DGRAM);
    multicastd->ip = iniparser_getstr(dict, "MULTICASTD:service_ip");
    multicastd->port = iniparser_getint(dict, "MULTICASTD:service_port", 2345);
    multicastd->working_mode = iniparser_getint(dict, "MULTICASTD:working_mode", WORKING_PROC);
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
    multicastd->session.flags = SB_MULTICAST|SB_NONBLOCK;
    multicastd->session.packet_type = PACKET_CERTAIN_LENGTH;
    multicastd->session.packet_length = sizeof(DBHEAD);
    multicastd->session.buffer_size = iniparser_getint(dict, "MULTICASTD:buffer_size", SB_BUF_SIZE);
    multicastd->session.packet_handler = &multicastd_packet_handler;
    multicastd->onrunning = &multicastd_onrunning;
    interval = iniparser_getint(dict, "MULTICASTD:heartbeat_interval", SB_HEARTBEAT_INTERVAL);
    multicastd->set_heartbeat(multicastd, interval, &cb_heartbeat_handler, multicastd);

    /* traced */
    /*
    if((traced = service_init()) == NULL)
    {
        fprintf(stderr, "Initialize service failed, %s", strerror(errno));
        _exit(-1);
    }
    traced->family = iniparser_getint(dict, "TRACED:inet_family", AF_INET);
    traced->sock_type = iniparser_getint(dict, "TRACED:socket_type", SOCK_STREAM);
    traced->working_mode = iniparser_getint(dict, "TRACED:working_mode", WORKING_PROC);
    traced->service_type = iniparser_getint(dict, "TRACED:service_type", C_SERVICE);
    traced->service_name = iniparser_getstr(dict, "TRACED:service_name");
    traced->ip = iniparser_getstr(dict, "TRACED:service_ip");
    traced->port = iniparser_getint(dict, "TRACED:service_port", 2344);
    traced->conns_limit = iniparser_getint(dict, "TRACED:conns_limit", SB_CONNS_LIMIT);
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
    traced->session.flags = SB_NONBLOCK;
    traced->session.packet_type = PACKET_CERTAIN_LENGTH;
    traced->session.packet_length = sizeof(DBHEAD);
    traced->session.buffer_size = iniparser_getint(dict, "TRACED:buffer_size", SB_BUF_SIZE);
    traced->session.packet_handler = &traced_packet_handler;
    traced->session.data_handler = &traced_data_handler;
    traced->session.timeout_handler = &traced_timeout_handler;
    */
    /* chunkd */
    public_multicast = iniparser_getstr(dict, "DBASE:public_multicast");
    for(i = 0; i < DBASE_MASK; i++) dbmasks[i].diskid = -1;
    memset(dbtasks, 0, DBASE_MASK * sizeof(DBTASK));
    memset(dbs, 0, sizeof(SERVICE *) * DBASE_MASK);
    mode = iniparser_getint(dict, "DBASE:mode", 0);
    if((p = iniparser_getstr(dict, "DBASE:basedir")) && (xdb = xdbase_init(p, mode)))
    {
        /* add disk list */
        if((p = iniparser_getstr(dict, "DBASE:disklist")))
        {
            while(*p != '\0')
            {
                while(*p != '\0' && *p < '0' && *p > '9')++p;
                port = atoi(p);
                while(*p != '\0' && *p >= '0' && *p <= '9')++p;
                while(*p != '\0' && (*p < '0' || *p > '9'))++p;
                limit = (off_t)atoll(p);
                while(*p != '\0' && *p >= '0' && *p <= '9')++p;
                while(*p != '\0' && (*p < '0' || *p > '9'))++p;
                mode = atoi(p);
                while(*p != '\0' && *p != ':')++p;
                if(*p == ':')++p;
                else break;
                /*
                while(*p != '\0' && (*p < '0' || *p > '9'))++p;
                s = p;
                while(*p != '\0' && *p != ':')++p;
                if(*p != ':') break;
                *p++ = '\0';
                mask = (int)inet_addr(s);
                */
                while(*p != '\0' && *p != '/')++p;
                if(*p != '/') break;
                disk = p;
                while(*p != '\0' && *p != ' ')++p;
                *p++ = '\0';
                //xdbase_add_disk(xdb, port, limit, mode, disk, mask);
                xdbase_add_disk(xdb, port, limit, mode, disk);
            }
        }
    }
    else
    {
        fprintf(stderr, "Invalid service[CHUNKD] config options\n");
        _exit(-1);
    }
    ret = (sbase->add_service(sbase, httpd) 
            //| sbase->add_service(sbase, traced) 
            | sbase->add_service(sbase, multicastd));
    return ret;
}

/* iosignal */
/*
static void hichunkd_iosignal(int sig)
{
    if(sig == SIGIO)
    {
        //sbase->iosignal(sbase);
    }
    return ;
}
*/

static void hichunkd_stop(int sig)
{
    switch (sig)
    {
        case SIGINT:
        case SIGTERM:
            fprintf(stderr, "server is interrupted by user.\n");
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
    //signal(SIGIO,   &hichunkd_iosignal);
    signal(SIGTERM, &hichunkd_stop);
    signal(SIGINT,  &hichunkd_stop);
    signal(SIGHUP,  &hichunkd_stop);
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
    if(argvmap)mtrie_clean(argvmap);
    if(dict)iniparser_free(dict);
    if(logger){LOGGER_CLEAN(logger);}
    if(httpd_index_html_code) free(httpd_index_html_code);
    return 0;
}
