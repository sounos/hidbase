#ifndef _DBASE_H_
#define _DBASE_H_
#ifdef __cplusplus
extern "C" {
#endif
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#define DBASE_REQ_SET         0x0001
#define DBASE_RESP_SET        0x1001
#define DBASE_REQ_APPEND      0x0002
#define DBASE_RESP_APPEND     0x1002
#define DBASE_REQ_DELETE      0x0003
#define DBASE_RESP_DELETE     0x1003
#define DBASE_REQ_UPDATE      0x0004
#define DBASE_RESP_UPDATE     0x1004
#define DBASE_REQ_UPBATCH     0x0005
#define DBASE_RESP_UPBATCH    0x1005
#define DBASE_REQ_COPY        0x0006
#define DBASE_RESP_COPY       0x1006
#define DBASE_REQ_STATE       0x0007
#define DBASE_RESP_STATE      0x1007
#define DBASE_REQ_REPORT      0x0008
#define DBASE_RESP_REPORT     0x1008
#define DBASE_REQ_GET         0x0009
#define DBASE_RESP_GET        0x1009
#define DBASE_REQ_FIND        0x000a
#define DBASE_RESP_FIND       0x100a
#define DBASE_REQ_REQUIRE     0x000b
#define DBASE_RESP_REQUIRE    0x100b
#define DBASE_CMD_BASE        0x1000
#define DBASE_PATH_MAX        1024
#define DBASE_META_PATH       0x01
#define DBASE_CHUNK_SIZE      4194304
#define DBASE_HOST_MAX        64
#define DBASE_STATUS_OK       0
#define DBASE_STATUS_ERR      -1
#define DBASE_MM_BASE         4096
#define DBASE_MASK            96
#define DBASE_TIMEOUT         1000000
#define DBASE_LEFT_MAX        2147483648
#define BJSON_BASE            65536
#define BJSON_DEEP            256
#define DBASE_TTL_MAX         32
#define DBASE_LINE_MAX        256
#define DBKMASK(xxx) (((uint64_t)xxx)%((uint64_t)DBASE_MASK))
#ifndef LLU
#define LLU(mmm) ((uint64_t)mmm)
#endif
    /*    0    Restricted to the same host. Won't be output by any interface.
          1    Restricted to the same subnet. Won't be forwarded by a router.
          <32         Restricted to the same site, organization or department.
          <64 Restricted to the same region.
          <128 Restricted to the same continent.
          <255 Unrestricted in scope. Global.
          */
/* dbase request */
typedef struct _DBHEAD
{
    short           status;
    short           cmd;
    ushort          index;
    ushort          port;
    int             cid;
    int             bits;
    int             qid;
    int             ip;
    int             size;
    int             length;
    int64_t         id;
}DBHEAD;
typedef struct _MDISK
{
    int      ip;
    int      port;
    uint32_t modtime;
    uint32_t total;
    uint64_t limit;
    uint64_t free;
}MDISK;
typedef struct _DBMETA
{
    int         cmd;
    int         length;
    int64_t     id;
}DBMETA;
/* db bjson element */
typedef struct _BELEMENT
{
    short nchilds;
    short flag;
    int   length;
}BELEMENT;
#define BJSON_TYPE_INT     0x01
#define BJSON_TYPE_LONG    0x02
#define BJSON_TYPE_DOUBLE  0x04
#define BJSON_TYPE_STRING  0x08
#define BJSON_TYPE_BLOB    0x10
#define BJSON_TYPE_OBJECT  0x20
#define BJSON_TYPE_ALL     0x3f
#define BJSON_PASS         0x100
/* db bjson */
typedef struct _BJSON
{
    int nblock;
    int ndata;
    char *data;
    char *block;
    int deep;
    int current;
    int stacks[BJSON_DEEP];
}BJSON,BRECORD;
#define BJSON_TYPE_REC  0x01
#define BJSON_TYPE_RES  0x02
/* RESULT */
typedef struct _BREC
{
    int  size;
    int  ndata;
    char *data;
}BREC,BROW,BRES;
/* dbase */
typedef struct _DBASE
{
    int fd;
    int status;
    BRES res;
    int64_t nwrite;
    int64_t time_write;
    int64_t nread;
    int64_t time_read;
    void    *logger;
    void    *timer;
    struct  sockaddr_in rsa;
}DBASE;
#define DBASE_IP_MAX 16
typedef struct _DBCAST
{
    int fd;
    int rfd;
    struct sockaddr_in local_addr;
    struct sockaddr_in mask_addr;
}DBCAST;
/* DB NODE */
#define DBASE_NODES_MAX 10240
typedef struct _DBNODE
{
    int ip;
    int fd;
}DBNODE;
/* DB IO */
typedef struct _DBIO
{
    DBCAST casts[DBASE_MASK];
    char multicast_network[DBASE_IP_MAX];
    DBNODE nodes[DBASE_NODES_MAX];
    int status;
    int bits;
    BRES res;
    void *logger;
}DBIO;
/* multicast RESPONSE  */
typedef struct _DBRESP
{
    short cmd;
    short no;
    short port;
    short status;
    int ip;
    int size;
    int64_t id;
}DBRESP;
/* set ip port */
int dbase_set(DBASE *dbase, const char *host, int port);
/* update data
 * return value
 * -1 dbase is NULL
 * -2 connection is bad
 * -3 write() request failed
 * -4 malloc() request failed
 * -5 recv() header failed
 * -6 malloc() data failed
 * -7 recv() data failed
 */
int dbase_update(DBASE *dbase, int64_t id, const char *data, int ndata);
/* get data
 * return value
 * -1 dbase is NULL
 * -2 connection is bad
 * -3 write() request failed
 * -4 malloc() request failed
 * -5 recv() header failed
 * -6 malloc() data failed
 * -7 recv() data failed
 */
int dbase_get(DBASE *dbase, int64_t *idlist, int count);
/* set bjson  data
 * return value
 * -1 dbase is NULL
 * -2 connection is bad
 * -3 write() request failed
 * -4 recv() header failed
 * -5 malloc() for packet failed
 * -6 recv() data failed
 * -7 err_status 
 */
int dbase_set_record(DBASE *dbase, int64_t id, BJSON *request);
/* update bjson  data
 * return value
 * -1 dbase is NULL
 * -2 connection is bad
 * -3 write() request failed
 * -4 recv() header failed
 * -5 malloc() for packet failed
 * -6 recv() data failed
 * -7 err_status 
 */
int dbase_update_record(DBASE *dbase, int64_t id, BJSON *request);
/* get bjson  data
 * return value
 * -1 dbase is NULL
 * -2 connection is bad
 * -3 write() request failed
 * -4 recv() header failed
 * -5 malloc() for packet failed
 * -6 recv() data failed
 * -7 err_status 
 */
BREC *dbase_get_record(DBASE *dbase, int64_t id);
/* get bjsons  
 * return value
 * -1 dbase is NULL
 * -2 connection is bad
 * -3 write() request failed
 * -4 recv() header failed
 * -5 malloc() for packet failed
 * -6 recv() data failed
 * -7 err_status 
 */
BRES *dbase_get_records(DBASE *dbase, BJSON *request);
/* del records 
 * return value
 * -1 dbase is NULL
 * -2 connection is bad
 * -3 write() request failed
 * -4 recv() header failed
 * -5 err_status 
 */
int dbase_del_records(DBASE *dbase, BJSON *request);
/* connect */
int dbase_connect(DBASE *dbase);
/* dbase close */
int dbase_close(DBASE *dbase);
/* bjson */
int bjson_reset(BJSON *bjson);
/* check add root */
int bjson_start(BJSON *record);
/* bjson new object */
int bjson_new_object(BJSON *bjson, const char *name);
/* over object */
int bjson_finish_object(BJSON *bjson);
/* append int element */
int bjson_append_int(BJSON *bjson, const char *name, int v);
/* append long element */
int bjson_append_long(BJSON *bjson, const char *name, int64_t v);
/* append double element */
int bjson_append_double(BJSON *bjson, const char *name, double v);
/* append string element */
int bjson_append_string(BJSON *bjson, const char *name, const char *v);
/* append blob element */
int bjson_append_blob(BJSON *bjson, const char *name, void *data, int ndata);
/* append element */
int bjson_append_element(BJSON *bjson, BELEMENT *element);
/* bjson finish */
int bjson_finish(BJSON *bjson);
/* BJSON to element */
BELEMENT *bjson_root(BJSON *record);
/* BJSON to BREC */
void bjson_to_record(BJSON *bjson, BREC *record);
/* bjson merge */
int bjson_merge_element(BJSON *one, BELEMENT *old, BELEMENT *batch);
/* bjson json */
int bjson_json(BREC *bjson, const char *out);
/* bjson to json */
int bjson_to_json(BREC *bjson, const char *out);
/* clean bjson */
void bjson_clean(BJSON *bjson);
/* initialize request  */
int brequest_reset(BJSON *request);
/* append to request */
int brequest_append_key(BJSON *request, int64_t id);
/* append keys  */
int brequest_append_keys(BJSON *request, int64_t *keys, int count);
/* finish request */
int brequest_finish(BJSON *request);
/* clean request */
void brequest_clean(BJSON *request);
/* get next record */
BELEMENT *dbase_next_record(BRES *res, BELEMENT *cur, int64_t *key);
/* get next meta */
DBMETA *dbase_next_meta(BRES *res, DBMETA *meta);
/* return meta record */
BELEMENT *dbase_meta_element(DBMETA *meta);
/* rec to element */
BELEMENT *belement(BREC *record);
/* return childs element */
BELEMENT *belement_childs(BELEMENT *e);
/* return next element */
BELEMENT *belement_next(BELEMENT *parent, BELEMENT *e);
/* return element name */
char *belement_key(BELEMENT *e);
/* find element with name */
BELEMENT *belement_find(BELEMENT *e, const char *key);
/* get element with NO */
BELEMENT *belement_get(BELEMENT *e, int no);
/* get value  */
int belement_v(BELEMENT *e, void **v);
/* get type  */
int belement_type(BELEMENT *e);
/* get value int */
int belement_v_int(BELEMENT *e, int *v);
/* get value long */
int belement_v_long(BELEMENT *e, int64_t *v);
/* get value double */
int belement_v_double(BELEMENT *e, double *v);
/* get value string */
char *belement_v_string(BELEMENT *e);
/* get value blob */
int belement_v_blob(BELEMENT *e, char **v);
/* genarate 64 bits key */
int64_t dbase_kid(char *str, int len);
#define BJSON_INIT(bjson) memset(&bjson, 0, sizeof(BJSON))
#ifdef __cplusplus
 }
#endif
#endif
