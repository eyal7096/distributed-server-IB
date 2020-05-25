

/*
 * Copyright (c) 2005 Topspin Communications.  All rights reserved.
 * Copyright (c) 2006 Cisco Systems.  All rights reserved.
 *
 * This software is available to you under a choice of one of two
 * licenses.  You may choose to be licensed under the terms of the GNU
 * General Public License (GPL) Version 2, available from the file
 * COPYING in the main directory of this source tree, or the
 * OpenIB.org BSD license below:
 *
 *     Redistribution and use in source and binary forms, with or
 *     without modification, are permitted provided that the following
 *     conditions are met:
 *
 *      - Redistributions of source code must retain the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer.
 *
 *      - Redistributions in binary form must reproduce the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer in the documentation and/or other materials
 *        provided with the distribution.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/param.h>
#include <sys/time.h>
#include <stdlib.h>
#include <getopt.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <time.h>
#include <errno.h>

#include <infiniband/verbs.h>

#include <fcntl.h>
#include <dirent.h>
#include <sys/mman.h>
#include <sys/stat.h>

struct data_node *get_data_node(char *key);

void add_node(char *key, char *data, unsigned size);

void print_node(struct data_node *to_print);

void replace_data(struct data_node *new);

int read_from_test_file(FILE *fp, char *operation_buf, char *key_buf, char *val_buf);

int g_argc;
static int g_port;
char **g_argv;
int indexer_flag = 0;
struct ibv_cq *cq_g = NULL;
struct ibv_context *context_g = NULL;
struct data_node *head;
struct packet *r_pkt;
int gflag = 0;
#define EAGER_PROTOCOL_LIMIT (1 << 12) /* 4KB limit */
#define KEY_SIZE 50
#define MAX_TEST_SIZE (10 * EAGER_PROTOCOL_LIMIT)
#define NUM_OF_SERVERS 2
#define MAX_CLIENTS 2
#define TEST_LOCATION "~/www/"

enum packet_type {
    GET_REQUEST,                //0
    EAGER_GET_RESPONSE,         //1
    EAGER_SET_REQUEST,          //2
    ACK,                        //3     *this is basicly ACK for EAGER SET*/

    RENDEZVOUS_GET_REQUEST,     //4
    RENDEZVOUS_GET_RESPONSE,    //5
    RENDEZVOUS_SET_REQUEST,     //6
    RENDEZVOUS_SET_RESPONSE,    //7
    FIND,                       //8
    LOCATION,                   //9
};
struct data_node {
    struct data_node *next;
    char key[KEY_SIZE];
    char *data;
    struct ibv_mr *mr;
    int gets;
    int sets;
};
struct mr_list {
    struct ibv_mr *mr;
    char key[KEY_SIZE];
    int gets;
    int sets;
};
struct multi_answer {
    int ctx_num;
};

struct packet {
    enum packet_type type; /* What kind of packet/protocol is this */
    union {
        struct {
            char *key[KEY_SIZE];
        } get_request;//1

        struct {
            char key[KEY_SIZE];
            unsigned value_length;
            char value[0];
        } eager_get_response;//2

        /* EAGER PROTOCOL PACKETS */
        struct {
            char *key[KEY_SIZE];
            char value[0]; /* null terminator between key and value */
        } eager_set_request;//3

        struct {
            char *key[KEY_SIZE];
        } ack;//4

        struct {
            uint64_t buf_address;
            uint32_t r_key;
            int value_size;
        } rndv_get_response;//5

        struct {
            char key[KEY_SIZE];
            int value_size;
        } rndv_set_request;//6

        struct {
            uint64_t buf_address;
            uint32_t r_key;
        } rndv_set_response;//7

        /* TODO - maybe there are more packet types? */
        struct {
            unsigned num_of_servers;
            char key[KEY_SIZE];
        } find;//8

        struct {
            unsigned int selected_server;
        } location;//9
    };
};

struct kv_server_address {
    char *servername; /* In the last item of an array this is NULL */
    short port; /* This is useful for multiple servers on a host */
};

enum {
    PINGPONG_RECV_WRID = 1,
    PINGPONG_SEND_WRID = 2,
};

static int page_size;

struct pingpong_context {
    struct ibv_context *context;
    struct ibv_comp_channel *channel;
    struct ibv_pd *pd;
    struct ibv_mr *mr;
    struct ibv_cq *cq;
    struct ibv_qp *qp;
    void *buf;
    int size;
    int rx_depth;
    int routs;
    int pending;
    int ctx_num;
    struct ibv_port_attr portinfo;
};

struct pingpong_dest {
    int lid;
    int qpn;
    int psn;
    union ibv_gid gid;
};

enum ibv_mtu pp_mtu_to_enum(int mtu) {
    switch (mtu) {
        case 256:
            return IBV_MTU_256;
        case 512:
            return IBV_MTU_512;
        case 1024:
            return IBV_MTU_1024;
        case 2048:
            return IBV_MTU_2048;
        case 4096:
            return IBV_MTU_4096;
        default:
            return -1;
    }
}

int hash(const char *s, int num) {

    int h = 0;
    for (int i = 0; s[i] != '\0'; i++) // the loop condition didn't seem good
    {
        // the cast to unsigned char may be needed for system in which the type char is signed
        h = (h + (unsigned char) s[i]) % num;
    }
    return h;
}

uint16_t pp_get_local_lid(struct ibv_context *context, int port) {
    struct ibv_port_attr attr;

    if (ibv_query_port(context, port, &attr))
        return 0;

    return attr.lid;
}

int pp_get_port_info(struct ibv_context *context, int port,
                     struct ibv_port_attr *attr) {
    return ibv_query_port(context, port, attr);
}

void wire_gid_to_gid(const char *wgid, union ibv_gid *gid) {
    char tmp[9];
    uint32_t v32;
    int i;

    for (tmp[8] = 0, i = 0; i < 4; ++i) {
        memcpy(tmp, wgid + i * 8, 8);
        sscanf(tmp, "%x", &v32);
        *(uint32_t *) (&gid->raw[i * 4]) = ntohl(v32);
    }
}

void gid_to_wire_gid(const union ibv_gid *gid, char wgid[]) {
    int i;

    for (i = 0; i < 4; ++i)
        sprintf(&wgid[i * 8], "%08x", htonl(*(uint32_t *) (gid->raw + i * 4)));
}

static int pp_connect_ctx(struct pingpong_context *ctx, int port, int my_psn,
                          enum ibv_mtu mtu, int sl,
                          struct pingpong_dest *dest, int sgid_idx) {
    struct ibv_qp_attr attr = {
            .qp_state        = IBV_QPS_RTR,
            .path_mtu        = mtu,
            .dest_qp_num        = dest->qpn,
            .rq_psn            = dest->psn,
            .max_dest_rd_atomic    = 1,
            .min_rnr_timer        = 12,
            .ah_attr        = {
                    .is_global    = 0,
                    .dlid        = dest->lid,
                    .sl        = sl,
                    .src_path_bits    = 0,
                    .port_num    = port
            }
    };

    if (dest->gid.global.interface_id) {
        attr.ah_attr.is_global = 1;
        attr.ah_attr.grh.hop_limit = 1;
        attr.ah_attr.grh.dgid = dest->gid;
        attr.ah_attr.grh.sgid_index = sgid_idx;
    }
    if (ibv_modify_qp(ctx->qp, &attr,
                      IBV_QP_STATE |
                      IBV_QP_AV |
                      IBV_QP_PATH_MTU |
                      IBV_QP_DEST_QPN |
                      IBV_QP_RQ_PSN |
                      IBV_QP_MAX_DEST_RD_ATOMIC |
                      IBV_QP_MIN_RNR_TIMER)) {
        fprintf(stderr, "Failed to modify QP to RTR\n");
        return 1;
    }

    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.sq_psn = my_psn;
    attr.max_rd_atomic = 1;
    if (ibv_modify_qp(ctx->qp, &attr,
                      IBV_QP_STATE |
                      IBV_QP_TIMEOUT |
                      IBV_QP_RETRY_CNT |
                      IBV_QP_RNR_RETRY |
                      IBV_QP_SQ_PSN |
                      IBV_QP_MAX_QP_RD_ATOMIC)) {
        fprintf(stderr, "Failed to modify QP to RTS\n");
        return 1;
    }

    return 0;
}

static struct pingpong_dest *pp_client_exch_dest(const char *servername, int port,
                                                 const struct pingpong_dest *my_dest) {
    struct addrinfo *res, *t;
    struct addrinfo hints = {
            .ai_family   = AF_INET,
            .ai_socktype = SOCK_STREAM
    };
    char *service;
    char msg[sizeof "0000:000000:000000:00000000000000000000000000000000"];
    int n;
    int sockfd = -1;
    struct pingpong_dest *rem_dest = NULL;
    char gid[33];

    if (asprintf(&service, "%d", port) < 0)
        return NULL;

    n = getaddrinfo(servername, service, &hints, &res);

    if (n < 0) {
        fprintf(stderr, "%s for %s:%d\n", gai_strerror(n), servername, port);
        free(service);
        return NULL;
    }

    for (t = res; t; t = t->ai_next) {

        sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
        if (sockfd >= 0) {
            if (!connect(sockfd, t->ai_addr, t->ai_addrlen)) {
                break;
            }
            perror("fail to connect tcp\n");
            close(sockfd);
            sockfd = -1;
        }
    }

    freeaddrinfo(res);
    free(service);

    if (sockfd < 0) {
        fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
        return NULL;
    }

    gid_to_wire_gid(&my_dest->gid, gid);
    sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn, my_dest->psn, gid);
    if (write(sockfd, msg, sizeof msg) != sizeof msg) {
        fprintf(stderr, "Couldn't send local address\n");
        goto out;
    }

    if (read(sockfd, msg, sizeof msg) != sizeof msg) {
        perror("client read");
        fprintf(stderr, "Couldn't read remote address\n");
        goto out;
    }

    write(sockfd, "done", sizeof "done");

    rem_dest = malloc(sizeof *rem_dest);
    if (!rem_dest)
        goto out;

    sscanf(msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn, &rem_dest->psn, gid);
    wire_gid_to_gid(gid, &rem_dest->gid);

    out:
    close(sockfd);
    return rem_dest;
}

static struct pingpong_dest *pp_server_exch_dest(struct pingpong_context *ctx,
                                                 int ib_port, enum ibv_mtu mtu,
                                                 int port, int sl,
                                                 const struct pingpong_dest *my_dest,
                                                 int sgid_idx) {
    struct addrinfo *res, *t;
    struct addrinfo hints = {
            .ai_flags    = AI_PASSIVE,
            .ai_family   = AF_INET,
            .ai_socktype = SOCK_STREAM
    };
    char *service;
    char msg[sizeof "0000:000000:000000:00000000000000000000000000000000"];
    int n;
    int sockfd = -1, connfd;
    struct pingpong_dest *rem_dest = NULL;
    char gid[33];

    if (asprintf(&service, "%d", port) < 0)
        return NULL;

    n = getaddrinfo(NULL, service, &hints, &res);

    if (n < 0) {
        fprintf(stderr, "%s for port %d\n", gai_strerror(n), port);
        free(service);
        return NULL;
    }

    for (t = res; t; t = t->ai_next) {
        sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
        if (sockfd >= 0) {
            n = 1;

            setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &n, sizeof n);

            if (!bind(sockfd, t->ai_addr, t->ai_addrlen))
                break;
            close(sockfd);
            sockfd = -1;
        }
    }

    freeaddrinfo(res);
    free(service);

    if (sockfd < 0) {
        fprintf(stderr, "Couldn't listen to port %d\n", port);
        return NULL;
    }

    listen(sockfd, 1);
    connfd = accept(sockfd, NULL, 0);
    close(sockfd);
    if (connfd < 0) {
        fprintf(stderr, "accept() failed\n");
        return NULL;
    }

    n = read(connfd, msg, sizeof msg);
    if (n != sizeof msg) {
        perror("server read");
        fprintf(stderr, "%d/%d: Couldn't read remote address\n", n, (int) sizeof msg);
        goto out;
    }

    rem_dest = malloc(sizeof *rem_dest);
    if (!rem_dest)
        goto out;

    sscanf(msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn, &rem_dest->psn, gid);
    wire_gid_to_gid(gid, &rem_dest->gid);

    if (pp_connect_ctx(ctx, ib_port, my_dest->psn, mtu, sl, rem_dest, sgid_idx)) {
        fprintf(stderr, "Couldn't connect to remote QP\n");
        free(rem_dest);
        rem_dest = NULL;
        goto out;
    }

    gid_to_wire_gid(&my_dest->gid, gid);
    sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn, my_dest->psn, gid);
    if (write(connfd, msg, sizeof msg) != sizeof msg) {
        fprintf(stderr, "Couldn't send local address\n");
        free(rem_dest);
        rem_dest = NULL;
        goto out;
    }

    read(connfd, msg, sizeof msg);

    out:
    close(connfd);
    return rem_dest;
}

#include <sys/param.h>
#include <tiff.h>

static struct pingpong_context *pp_init_ctx(struct ibv_device *ib_dev, int size,
                                            int rx_depth, int port,
                                            int use_event, int is_server) {
    struct pingpong_context *ctx;

    ctx = calloc(1, sizeof *ctx);
    if (!ctx)
        return NULL;

    ctx->size = size;
    ctx->rx_depth = rx_depth;
    ctx->routs = rx_depth;

    ctx->buf = malloc(roundup(size, page_size));
    if (!ctx->buf) {
        fprintf(stderr, "Couldn't allocate work buf.\n");
        return NULL;
    }

    memset(ctx->buf, 0x7b + is_server, size);

    ctx->context = ibv_open_device(ib_dev);
    if (!ctx->context) {
        fprintf(stderr, "Couldn't get context for %s\n",
                ibv_get_device_name(ib_dev));
        return NULL;
    }

    if (use_event) {
        ctx->channel = ibv_create_comp_channel(ctx->context);
        if (!ctx->channel) {
            fprintf(stderr, "Couldn't create completion channel\n");
            return NULL;
        }
    } else
        ctx->channel = NULL;

    ctx->pd = ibv_alloc_pd(ctx->context);
    if (!ctx->pd) {
        fprintf(stderr, "Couldn't allocate PD\n");
        return NULL;
    }

    ctx->mr = ibv_reg_mr(ctx->pd, ctx->buf, size, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    if (!ctx->mr) {
        fprintf(stderr, "Couldn't register MR\n");
        return NULL;
    }

    ctx->cq = ibv_create_cq(ctx->context, rx_depth + 1, NULL,
                            ctx->channel, 0);
    if (!ctx->cq) {
        fprintf(stderr, "Couldn't create CQ\n");
        return NULL;
    }

    {
        struct ibv_qp_init_attr attr = {
                .send_cq = ctx->cq,
                .recv_cq = ctx->cq,
                .cap     = {
                        .max_send_wr  = 1,
                        .max_recv_wr  = rx_depth,
                        .max_send_sge = 1,
                        .max_recv_sge = 1
                },
                .qp_type = IBV_QPT_RC
        };

        ctx->qp = ibv_create_qp(ctx->pd, &attr);
        if (!ctx->qp) {
            fprintf(stderr, "Couldn't create QP\n");
            return NULL;
        }
    }

    {
        struct ibv_qp_attr attr = {
                .qp_state        = IBV_QPS_INIT,
                .pkey_index      = 0,
                .port_num        = port,
                .qp_access_flags = IBV_ACCESS_REMOTE_READ |
                                   IBV_ACCESS_REMOTE_WRITE
        };

        if (ibv_modify_qp(ctx->qp, &attr,
                          IBV_QP_STATE |
                          IBV_QP_PKEY_INDEX |
                          IBV_QP_PORT |
                          IBV_QP_ACCESS_FLAGS)) {
            fprintf(stderr, "Failed to modify QP to INIT\n");
            return NULL;
        }
    }

    return ctx;
}

int pp_close_ctx(struct pingpong_context *ctx) {
    if (ibv_destroy_qp(ctx->qp)) {
        fprintf(stderr, "Couldn't destroy QP\n");
        return 1;
    }
    if (ibv_destroy_cq(ctx->cq)) {
        fprintf(stderr, "Couldn't destroy CQ\n");
        return 1;
    }
    if (ibv_dereg_mr(ctx->mr)) {
        fprintf(stderr, "Couldn't deregister MR\n");
        return 1;
    }
    if (ibv_dealloc_pd(ctx->pd)) {
        fprintf(stderr, "Couldn't deallocate PD\n");
        return 1;
    }
    if (ctx->channel) {
        if (ibv_destroy_comp_channel(ctx->channel)) {
            fprintf(stderr, "Couldn't destroy completion channel\n");
            return 1;
        }
    }
    /*	if (ibv_close_device(ctx->context)) {
        	fprintf(stderr, "Couldn't release context\n");
        	return 1;
    	}*/

    //free(ctx->buf);
    //free(ctx);

    return 0;
}

uint64_t create_id(uint16_t ctx_num, uint8_t type) {
    //printf("create_id ctxnum %d\n%d\n",ctx_num,type);
    uint64_t tmp_num = 0;
    uint64_t tmp_type = 0;
    tmp_num = ctx_num;
    tmp_type = type;
    tmp_num <<= 8;
    tmp_num = tmp_num | tmp_type;
    //printf("create_id %d\n",tmp_num);
    return tmp_num;
}

static int pp_post_recv(struct pingpong_context *ctx, int n) {
    struct ibv_sge list = {
            .addr    = (uintptr_t) ctx->buf,
            .length = ctx->size,
            .lkey    = ctx->mr->lkey
    };
    struct ibv_recv_wr wr = {
            .wr_id        = PINGPONG_RECV_WRID,
            ////my library
            /*wr_id fields [ctx_num][type]*/
            /*                       1 bit*/
            //.wr_id      = create_id(ctx->ctx_num, PINGPONG_RECV_WRID),
            .sg_list    = &list,
            .num_sge    = 1,
            .next       = NULL
    };


    struct ibv_recv_wr *bad_wr;
    int i;
    for (i = 0; i < n; ++i)
        if (ibv_post_recv(ctx->qp, &wr, &bad_wr))
            break;
    return i;
}

static int pp_post_send(struct pingpong_context *ctx, enum ibv_wr_opcode opcode, unsigned size, const char *local_ptr,
                        void *remote_ptr, uint32_t remote_key) {
    struct ibv_sge list = {
            .addr    = (uintptr_t) (local_ptr ? local_ptr : ctx->buf),
            .length = size,
            .lkey    = ctx->mr->lkey
    };

    struct ibv_send_wr wr = {
            .wr_id        = PINGPONG_SEND_WRID,
            .sg_list    = &list,
            .num_sge    = 1,
            .opcode     = opcode,
            .send_flags = IBV_SEND_SIGNALED,
            .next       = NULL
    };

    struct ibv_send_wr *bad_wr;

    if (remote_ptr) {
        wr.wr.rdma.remote_addr = (uintptr_t) remote_ptr;
        wr.wr.rdma.rkey = remote_key;
    }
    return ibv_post_send(ctx->qp, &wr, &bad_wr);
}

static int remote_write(struct pingpong_context *ctx, uint64_t remote_address, uint32_t remote_key, struct ibv_mr *mr) {
    struct ibv_sge list = {
            .addr    = (uint64_t) mr->addr,
            .length  = mr->length,
            .lkey    = mr->lkey
    };

    struct ibv_send_wr *bad_wr, wr = {
            .wr_id      = PINGPONG_SEND_WRID,
            .sg_list    = &list,
            .num_sge    = 1,
            .send_flags = IBV_SEND_SIGNALED,
            .next       = NULL,
            .opcode     = IBV_WR_RDMA_WRITE,
            .wr.rdma.remote_addr = remote_address,
            .wr.rdma.rkey        = remote_key
    };
    return ibv_post_send(ctx->qp, &wr, &bad_wr);
}

static int remote_read(struct pingpong_context *ctx, uint64_t remote_address, uint32_t remote_key, struct ibv_mr *mr) {
    struct ibv_sge list = {
            .addr    = (uint64_t) mr->addr,
            .length = mr->length,
            .lkey    = mr->lkey
    };

    struct ibv_send_wr *bad_wr, wr = {
            .wr_id        = PINGPONG_SEND_WRID,
            .sg_list    = &list,
            .num_sge    = 1,
            .send_flags = IBV_SEND_SIGNALED,
            .next       = NULL,
            .opcode     = IBV_WR_RDMA_READ,
            .wr.rdma.remote_addr = remote_address,
            .wr.rdma.rkey        = remote_key
    };
    return ibv_post_send(ctx->qp, &wr, &bad_wr);
}


static void usage(const char *argv0) {
    printf("Usage:\n");
    printf("  %s            start a server and wait for connection\n", argv0);
    printf("  %s <host>     connect to server at <host>\n", argv0);
    printf("\n");
    printf("Options:\n");
    printf("  -p, --port=<port>      listen on/connect to port <port> (default 18515)\n");
    printf("  -d, --ib-dev=<dev>     use IB device <dev> (default first device found)\n");
    printf("  -i, --ib-port=<port>   use port <port> of IB device (default 1)\n");
    printf("  -s, --size=<size>      size of message to exchange (default 4096)\n");
    printf("  -m, --mtu=<size>       path MTU (default 1024)\n");
    printf("  -r, --rx-depth=<dep>   number of receives to post at a time (default 500)\n");
    printf("  -n, --iters=<iters>    number of exchanges (default 1000)\n");
    printf("  -l, --sl=<sl>          service level value\n");
    printf("  -e, --events           sleep on CQ events (default poll)\n");
    printf("  -g, --gid-idx=<gid index> local port gid index\n");
}

int orig_main(struct kv_server_address *server, unsigned size, int argc, char *argv[],
              struct pingpong_context **result_ctx) {
    struct ibv_device **dev_list;
    struct ibv_device *ib_dev;
    struct pingpong_context *ctx;
    struct pingpong_dest my_dest;
    struct pingpong_dest *rem_dest;
    struct timeval start, end;
    char *ib_devname = NULL;
    char *servername = server->servername;
    int port = server->port;
    int ib_port = 1;
    enum ibv_mtu mtu = IBV_MTU_1024;
    int rx_depth = 1;
    int iters = 1000;
    int use_event = 0;
    int routs;
    int rcnt, scnt;
    int num_cq_events = 0;
    int sl = 0;
    int gidx = -1;
    char gid[33];

    srand48(getpid() * time(NULL));

    while (1) {
        int c;

        static struct option long_options[] = {
                {.name = "port", .has_arg = 1, .val = 'p'},
                {.name = "ib-dev", .has_arg = 1, .val = 'd'},
                {.name = "ib-port", .has_arg = 1, .val = 'i'},
                {.name = "size", .has_arg = 1, .val = 's'},
                {.name = "mtu", .has_arg = 1, .val = 'm'},
                {.name = "rx-depth", .has_arg = 1, .val = 'r'},
                {.name = "iters", .has_arg = 1, .val = 'n'},
                {.name = "sl", .has_arg = 1, .val = 'l'},
                {.name = "events", .has_arg = 0, .val = 'e'},
                {.name = "gid-idx", .has_arg = 1, .val = 'g'},
                {0}
        };

        c = getopt_long(argc, argv, "p:d:i:s:m:r:n:l:eg:", long_options, NULL);
        if (c == -1)
            break;

        switch (c) {
            case 'p':
                port = strtol(optarg, NULL, 0);
                if (port < 0 || port > 65535) {
                    usage(argv[0]);
                    return 1;
                }
                break;

            case 'd':
                ib_devname = strdup(optarg);
                break;

            case 'i':
                ib_port = strtol(optarg, NULL, 0);
                if (ib_port < 0) {
                    usage(argv[0]);
                    return 1;
                }
                break;

            case 's':
                size = strtol(optarg, NULL, 0);
                break;

            case 'm':
                mtu = pp_mtu_to_enum(strtol(optarg, NULL, 0));
                if (mtu < 0) {
                    usage(argv[0]);
                    return 1;
                }
                break;

            case 'r':
                rx_depth = strtol(optarg, NULL, 0);
                break;

            case 'n':
                iters = strtol(optarg, NULL, 0);
                break;

            case 'l':
                sl = strtol(optarg, NULL, 0);
                break;

            case 'e':
                ++use_event;
                break;

            case 'g':
                gidx = strtol(optarg, NULL, 0);
                break;

            default:
                usage(argv[0]);
                return 1;
        }
    }
    ib_port = 2;
    if (port != 0) g_port = port;
    if (port == 7095) indexer_flag = 1;
    if (port == 0) port = g_port;

    if (optind == argc - 1)
        servername = strdup(argv[optind]);
    else if (optind < argc) {
        usage(argv[0]);
        return 1;
    }

    page_size = sysconf(_SC_PAGESIZE);

    dev_list = ibv_get_device_list(NULL);
    if (!dev_list) {
        perror("Failed to get IB devices list");
        return 1;
    }

    if (!ib_devname) {
        ib_dev = *dev_list;
        if (!ib_dev) {
            fprintf(stderr, "No IB devices found\n");
            return 1;
        }
    } else {
        int i;
        for (i = 0; dev_list[i]; ++i)
            if (!strcmp(ibv_get_device_name(dev_list[i]), ib_devname))
                break;
        ib_dev = dev_list[i];
        if (!ib_dev) {
            fprintf(stderr, "IB device %s not found\n", ib_devname);
            return 1;
        }
    }

    ctx = pp_init_ctx(ib_dev, size, rx_depth, ib_port, use_event, !servername);
    if (!ctx)
        return 1;

    routs = pp_post_recv(ctx, ctx->rx_depth);
    if (routs < ctx->rx_depth) {
        fprintf(stderr, "Couldn't post receive (%d)\n", routs);
        return 1;
    }

    if (use_event)
        if (ibv_req_notify_cq(ctx->cq, 0)) {
            fprintf(stderr, "Couldn't request CQ notification\n");
            return 1;
        }


    if (pp_get_port_info(ctx->context, ib_port, &ctx->portinfo)) {
        fprintf(stderr, "Couldn't get port info\n");
        return 1;
    }

    my_dest.lid = ctx->portinfo.lid;
    if (ctx->portinfo.link_layer == IBV_LINK_LAYER_INFINIBAND && !my_dest.lid) {
        fprintf(stderr, "Couldn't get local LID\n");
        return 1;
    }

    if (gidx >= 0) {
        if (ibv_query_gid(ctx->context, ib_port, gidx, &my_dest.gid)) {
            fprintf(stderr, "Could not get local gid for gid index %d\n", gidx);
            return 1;
        }
    } else
        memset(&my_dest.gid, 0, sizeof my_dest.gid);

    my_dest.qpn = ctx->qp->qp_num;
    my_dest.psn = lrand48() & 0xffffff;
    inet_ntop(AF_INET6, &my_dest.gid, gid, sizeof gid);
    printf("  local address:  LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
           my_dest.lid, my_dest.qpn, my_dest.psn, gid);


    if (servername)
        rem_dest = pp_client_exch_dest(servername, port, &my_dest);
    else
        rem_dest = pp_server_exch_dest(ctx, ib_port, mtu, port, sl, &my_dest, gidx);

    if (!rem_dest)
        return 1;

    inet_ntop(AF_INET6, &rem_dest->gid, gid, sizeof gid);
    printf("  remote address: LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
           rem_dest->lid, rem_dest->qpn, rem_dest->psn, gid);

    if (servername)
        if (pp_connect_ctx(ctx, ib_port, my_dest.psn, mtu, sl, rem_dest, gidx))
            return 1;

    ibv_free_device_list(dev_list);
    free(rem_dest);
    *result_ctx = ctx;
    return 0;
}

int s_orig_main(struct kv_server_address *server, unsigned size, int argc, char *argv[],
                struct pingpong_context **result_ctx, int num) {
    struct ibv_device **dev_list;
    struct ibv_device *ib_dev;
    struct pingpong_context *ctx;
    struct pingpong_dest my_dest;
    struct pingpong_dest *rem_dest;
    struct timeval start, end;
    char *ib_devname = NULL;
    char *servername = server->servername;
    int port = server->port;
    int ib_port = 1;
    enum ibv_mtu mtu = IBV_MTU_1024;
    int rx_depth = 1;
    int iters = 1000;
    int use_event = 0;
    int routs;
    int rcnt, scnt;
    int num_cq_events = 0;
    int sl = 0;
    int gidx = -1;
    char gid[33];

    srand48(getpid() * time(NULL));

    while (1) {
        int c;

        static struct option long_options[] = {
                {.name = "port", .has_arg = 1, .val = 'p'},
                {.name = "ib-dev", .has_arg = 1, .val = 'd'},
                {.name = "ib-port", .has_arg = 1, .val = 'i'},
                {.name = "size", .has_arg = 1, .val = 's'},
                {.name = "mtu", .has_arg = 1, .val = 'm'},
                {.name = "rx-depth", .has_arg = 1, .val = 'r'},
                {.name = "iters", .has_arg = 1, .val = 'n'},
                {.name = "sl", .has_arg = 1, .val = 'l'},
                {.name = "events", .has_arg = 0, .val = 'e'},
                {.name = "gid-idx", .has_arg = 1, .val = 'g'},
                {0}
        };

        c = getopt_long(argc, argv, "p:d:i:s:m:r:n:l:eg:", long_options, NULL);
        if (c == -1)
            break;

        switch (c) {
            case 'p':
                port = strtol(optarg, NULL, 0);
                if (port < 0 || port > 65535) {
                    usage(argv[0]);
                    return 1;
                }
                break;

            case 'd':
                ib_devname = strdup(optarg);
                break;

            case 'i':
                ib_port = strtol(optarg, NULL, 0);
                printf("ib port= %d\n", ib_port);
                if (ib_port < 0) {
                    usage(argv[0]);
                    return 1;
                }
                break;

            case 's':
                size = strtol(optarg, NULL, 0);
                break;

            case 'm':
                mtu = pp_mtu_to_enum(strtol(optarg, NULL, 0));
                if (mtu < 0) {
                    usage(argv[0]);
                    return 1;
                }
                break;

            case 'r':
                rx_depth = strtol(optarg, NULL, 0);
                break;

            case 'n':
                iters = strtol(optarg, NULL, 0);
                break;

            case 'l':
                sl = strtol(optarg, NULL, 0);
                break;

            case 'e':
                ++use_event;
                break;

            case 'g':
                gidx = strtol(optarg, NULL, 0);
                break;

            default:
                usage(argv[0]);
                return 1;
        }
    }
    ib_port = 2;
    if (port != 0) g_port = port;
    if (port == 7095) indexer_flag = 1;
    if (port == 0) port = g_port;

    if (optind == argc - 1)
        servername = strdup(argv[optind]);
    else if (optind < argc) {
        usage(argv[0]);
        return 1;
    }

    page_size = sysconf(_SC_PAGESIZE);

    dev_list = ibv_get_device_list(NULL);
    if (!dev_list) {
        perror("Failed to get IB devices list");
        return 1;
    }

    if (!ib_devname) {
        ib_dev = *dev_list;
        if (!ib_dev) {
            fprintf(stderr, "No IB devices found\n");
            return 1;
        }
    } else {
        int i;
        for (i = 0; dev_list[i]; ++i)
            if (!strcmp(ibv_get_device_name(dev_list[i]), ib_devname))
                break;
        ib_dev = dev_list[i];
        if (!ib_dev) {
            fprintf(stderr, "IB device %s not found\n", ib_devname);
            return 1;
        }
    }

    ctx = pp_init_ctx(ib_dev, size, rx_depth, ib_port, use_event, !servername);
    if (!ctx)
        return 1;
    ctx->ctx_num = num;
    routs = pp_post_recv(ctx, ctx->rx_depth);
    if (routs < ctx->rx_depth) {
        fprintf(stderr, "Couldn't post receive (%d)\n", routs);
        return 1;
    }

    if (use_event)
        if (ibv_req_notify_cq(ctx->cq, 0)) {
            fprintf(stderr, "Couldn't request CQ notification\n");
            return 1;
        }


    if (pp_get_port_info(ctx->context, ib_port, &ctx->portinfo)) {
        fprintf(stderr, "Couldn't get port info\n");
        return 1;
    }

    my_dest.lid = ctx->portinfo.lid;
    if (ctx->portinfo.link_layer == IBV_LINK_LAYER_INFINIBAND && !my_dest.lid) {
        fprintf(stderr, "Couldn't get local LID\n");
        return 1;
    }

    if (gidx >= 0) {
        if (ibv_query_gid(ctx->context, ib_port, gidx, &my_dest.gid)) {
            fprintf(stderr, "Could not get local gid for gid index %d\n", gidx);
            return 1;
        }
    } else
        memset(&my_dest.gid, 0, sizeof my_dest.gid);

    my_dest.qpn = ctx->qp->qp_num;
    my_dest.psn = lrand48() & 0xffffff;
    inet_ntop(AF_INET6, &my_dest.gid, gid, sizeof gid);
    printf("  local address:  LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
           my_dest.lid, my_dest.qpn, my_dest.psn, gid);


    if (servername)
        rem_dest = pp_client_exch_dest(servername, port, &my_dest);
    else
        rem_dest = pp_server_exch_dest(ctx, ib_port, mtu, port, sl, &my_dest, gidx);

    if (!rem_dest)
        return 1;

    inet_ntop(AF_INET6, &rem_dest->gid, gid, sizeof gid);
    printf("  remote address: LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
           rem_dest->lid, rem_dest->qpn, rem_dest->psn, gid);

    if (servername)
        if (pp_connect_ctx(ctx, ib_port, my_dest.psn, mtu, sl, rem_dest, gidx))
            return 1;

    ibv_free_device_list(dev_list);
    free(rem_dest);
    *result_ctx = ctx;
    return 0;
}

void handle_server_packets_only(struct pingpong_context *ctx, struct packet *packet) {
    int tmp;
    int cnt = 0, req = 0;
    char key[KEY_SIZE];
    unsigned packet_size = 0;
    struct data_node *node, *temp;
    struct ibv_mr *mr;
    char *data, *value;
    memset(key, '\0', KEY_SIZE);

    packet = (struct packet *) ctx->buf;
    //printf("pkt type %d\n",packet->type);
    switch (packet->type) {
        case FIND:
            tmp = packet->find.num_of_servers;
            memcpy(key, packet->find.key, strlen(packet->find.key));
            packet = (struct packet *) ctx->buf;
            printf("key: %s", packet->find.key);
            assert(packet->type = LOCATION);
            int loc = hash(key, tmp);
            packet->location.selected_server = loc;
            packet_size = sizeof(struct packet) + sizeof(unsigned int);
            pp_post_send(ctx, IBV_WR_SEND, packet_size, NULL, NULL,
                         0);// Sends the packet to the currct client
            printf("return to CTX %d\n", ctx->ctx_num);
            printf("loc = %d\n", packet->location);
            printf("after return LOCATION\n\n");
            memset(key, '\0', strlen(key));
            break;
        case GET_REQUEST:   ////GET_REQUEST pkt received at server
            ////in this part we identify witch type of GET we get
            memset(key, '\0', KEY_SIZE);
            memcpy(key, packet->get_request.key, KEY_SIZE);
            memset(ctx->buf, '\0', strlen(ctx->buf) * sizeof(char));
            printf(("GET_REQUEST receivsd\n"));
            printf("key is %s\n", packet->get_request.key);
            node = get_data_node(packet->get_request.key);
            if (node == NULL) {
                printf("in null\n");
            }
            printf("node sets ==%d\n", node->sets);
            while (node->sets != 0);

            node->gets++;
            packet_size = strlen(node->data);
            if (packet_size < EAGER_PROTOCOL_LIMIT) {
                memset(ctx->buf, '\0', strlen(ctx->buf) * sizeof(char));
                r_pkt = (struct packet *) ctx->buf;
                r_pkt->type = EAGER_GET_RESPONSE;
            } else {
                r_pkt = (struct packet *) ctx->buf;
                r_pkt->type = RENDEZVOUS_GET_RESPONSE;
            }
            ////in this section we response with the right GET type msg
            switch (r_pkt->type) {      /*now we switch case base on the type of packet we return*/
                case EAGER_GET_RESPONSE:    ////sending EAGER_GET_RESPONSE
                    r_pkt = (struct packet *) ctx->buf;
                    printf("returning EAGER_GET_RESPONSE\n");
                    memset(ctx->buf, '\0', EAGER_PROTOCOL_LIMIT);
                    r_pkt->type = EAGER_GET_RESPONSE;
                    memset(r_pkt->eager_get_response.key, '\0', KEY_SIZE);
                    memcpy(r_pkt->eager_get_response.key, key, strlen(key));
                    memcpy(r_pkt->eager_get_response.value, node->data, strlen(node->data));
                    printf("get response key:%s\n", r_pkt->eager_get_response.key);

                    r_pkt->eager_get_response.value_length = strlen(r_pkt->eager_get_response.value);
                    packet_size = strlen(node->data) + strlen(key) + sizeof(struct packet);
                    pp_post_send(ctx, IBV_WR_SEND, packet_size, NULL, NULL, NULL); // Sends the packet to the server
                    node->gets--;
                    printf("return to %d\n", ctx->ctx_num);

                    printf("EAGER_GET_REPONSE SENT\n");
                    break;

                case RENDEZVOUS_GET_RESPONSE:////sending RENDEZVOUS_GET_RESPONSE
                    printf("returning RENDEZVOUS_GET_RESPONSE\n");
                    if (!node->mr) {
                        printf("MR REG\n");
                        mr = ibv_reg_mr(ctx->pd, node->data, strlen(node->data),
                                        IBV_ACCESS_REMOTE_READ | IBV_ACCESS_LOCAL_WRITE |
                                        IBV_ACCESS_REMOTE_WRITE);
                    }
                    node->mr = mr;
                    if (!node->mr) {
                        printf("mr failed %d\n", __LINE__);
                        exit(0);
                    }
                    r_pkt->rndv_get_response.buf_address = node->data;
                    r_pkt->rndv_get_response.r_key = node->mr->rkey;

                    r_pkt->rndv_get_response.value_size = strlen(node->data);
                    packet_size = sizeof(uint64_t) + sizeof(uint32_t) + sizeof(struct packet) + sizeof(int);
                    pp_post_send(ctx, IBV_WR_SEND, packet_size, NULL, NULL, NULL);
                    printf("RENDEZVOUS_GET_RESPONSE sent\n");
                    break;
                default:
                    printf("default\n");
                    exit(0);
            }
            break;
        case EAGER_SET_REQUEST: ////SET REQUEST RECEIVED
            printf("EAGER_SET_REQUEST received\n");

            node = get_data_node(packet->get_request.key);
            if (node) while (node->gets != 0);
            if (node == NULL) {
                printf("no key found\n");           ////create new node
                printf("recv packet key : %s\n", packet->eager_set_request.key);

                data = (char *) calloc(1, strlen(packet->eager_set_request.value) * sizeof(char));
                memcpy(data, packet->eager_set_request.value, strlen(packet->eager_set_request.value));
                add_node(packet->eager_set_request.key, data, strlen(packet->eager_set_request.value));
                node = get_data_node(packet->get_request.key);
            } else {
                node->sets++;
                printf("key found\n");
                free(node->data);
                node->data = (char *) calloc(1, strlen(packet->eager_set_request.value) * sizeof(char));
                memcpy(node->data, packet->eager_set_request.value, strlen(packet->eager_set_request.value));
                printf("data replaced\n");
                /*temp = head;
                while (1) {
                    if (temp->next) {
                        temp = temp->next;
                    } else {
                        print_node(temp);
                        break;
                    }
                }*/
            }
            node->sets--;

            ////so far we change the data need to be set, if want we can return ACK as EAGER_SET_RESPONSE
            break;
        case RENDEZVOUS_SET_REQUEST:
            printf("RENDEZVOUS_SET_REQUEST\n");
            memset(key, '\0', KEY_SIZE);
            memcpy(key, packet->rndv_set_request.key, strlen(packet->rndv_set_request.key));
            printf("key is %s", packet->rndv_set_request.key);
            value = (char *) calloc(1, packet->rndv_set_request.value_size);
            printf("data size is %d\n", packet->rndv_set_request.value_size);
            r_pkt = (struct packet *) ctx->buf;
            r_pkt->type = RENDEZVOUS_SET_RESPONSE;
            node = get_data_node(key);
            if (node) printf("node gets %d\n", node->gets);
            if (node) while (node->gets != 0);

            if (node == NULL) {
                add_node(key, value, 5);
                node = get_data_node(key);
            } else {
                node->sets++;
                printf("key found\n");
                free(node->data);
                node->data = value;
            }
            mr = ibv_reg_mr(ctx->pd, value, packet->rndv_set_request.value_size,
                            IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE);
            node->mr = mr;
            if (!node->mr) {
                printf("mr failed %d\n", __LINE__);
                exit(0);
            }
            r_pkt->rndv_set_response.buf_address = value;
            r_pkt->rndv_set_response.r_key = mr->rkey;

            packet_size = sizeof(uint64_t) + sizeof(r_pkt->rndv_set_response.r_key) +
                          sizeof(struct packet);       ////check pkt size
            pp_post_send(ctx, IBV_WR_SEND, packet_size, NULL, NULL, NULL);

            break;
        case ACK:
            //////take care of gets/ sets contrs in the node
            printf("ACK received\n");
            packet = (struct packet *) ctx->buf;
            printf("node key is %s\n", packet->ack.key);
            node = get_data_node(packet->ack.key);

            if (node->sets > 0) node->sets--;
            else node->gets--;
            if ((node->gets == 0) && (node->sets == 0)) {
                ibv_dereg_mr(node->mr);
                node->mr = NULL;
            }
            printf("ACK done\n");
            break;
        default:
            printf("in handle default\n");
            exit(0);
    }
    memset(ctx->buf, '\0', EAGER_PROTOCOL_LIMIT);
    return;
}

int pp_wait_completions(struct pingpong_context *ctx, int iters) {
    int rcnt, scnt, num_cq_events, use_event = 0;
    rcnt = scnt = 0;
    printf("in wait comp\n");
    while (rcnt + scnt < iters) {
        struct ibv_wc wc[2];
        int ne, i;
        do {
            ne = ibv_poll_cq(ctx->cq, 2, wc);
            if (ne < 0) {
                printf("1");
                fprintf(stderr, "poll CQ failed %d\n", ne);
                return 1;
            }

        } while (ne < 1);
        for (i = 0; i < ne; ++i) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                fprintf(stderr, "Failed status %s (%d) for wr_id %d\n",
                        ibv_wc_status_str(wc[i].status),
                        wc[i].status, (int) wc[i].wr_id);
                exit(1);
                return 1;
            }
            switch ((int) wc[i].wr_id) {
                case PINGPONG_SEND_WRID:
                    printf("PINGPONG_SEND_WRID\n");
                    ++scnt;
                    break;

                case PINGPONG_RECV_WRID:
                    printf("PINGPONG_RECV_WRID\n");
                    ++rcnt;
                    break;

                default:
                    fprintf(stderr, "Completion for unknown wr_id %d\n",
                            (int) wc[i].wr_id);
                    return 1;
            }
        }
    }
    return 0;
}

struct mkv_ctx {
    unsigned num_servers;
    struct pingpong_context *kv_ctxs[NUM_OF_SERVERS];
};

int multi_wait_completions(struct mkv_ctx *ctx, int iters, struct multi_answer *ret) {
    /*this function will retrun if the completion was success full and withch ctx was comleted*/
    int rcnt, scnt, num_cq_events, use_event = 0;
    int current_event;
    rcnt = scnt = 0;
    printf("in wait multi complition\n");
    while (rcnt + scnt < iters) {
        struct ibv_wc wc[2];
        int ne, i;
        do {
            while (1) {
                ne = ibv_poll_cq(ctx->kv_ctxs[gflag]->cq, 2, wc);
                if (ne < 0) {
                    fprintf(stderr, "poll CQ failed %d\n", ne);
                    return 1;
                }
                if (ne == 0) {
                    gflag = (gflag + 1) % MAX_CLIENTS;
                } else break;
            }
        } while (ne < 1);

        for (i = 0; i < ne; ++i) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                fprintf(stderr, "Failed status %s (%d) for wr_id %d\n",
                        ibv_wc_status_str(wc[i].status),
                        wc[i].status, (int) wc[i].wr_id);
                exit(1);
                return 1;
            }

            switch ((int) wc[i].wr_id) {
                case PINGPONG_SEND_WRID:
                    ++scnt;
                    printf("PINGPONG_SEND_WRID%d\n", wc[i].qp_num);

                    break;

                case PINGPONG_RECV_WRID:
                    ++rcnt;
                    printf("PINGPONG_RECV_WRID%d\n", gflag);
                    handle_server_packets_only(ctx->kv_ctxs[gflag], ctx->kv_ctxs[gflag]->buf);
                    pp_post_recv(ctx->kv_ctxs[gflag], 1);
                    break;

                default:
                    fprintf(stderr, "Completion for unknown wr_id %d\n",
                            (int) wc[i].wr_id);
                    return 1;
            }
        }
    }
    return 0;
}

//////FUNCTION FOR DATA LINKED LIST////////

void add_node(char *key, char *data, unsigned size) {//if it works need to remove size
    printf("\nin add_node\n\n");
    struct data_node *tmp;
    tmp = head;
    struct data_node *to_add = (struct data_node *) malloc(sizeof(struct data_node));

    if (!head->data) {    ////*case this is the first node*/
        printf("\n\nHEAD == NULL\n\n");
        memset(head->key, '\0', KEY_SIZE);
        memcpy(head->key, key, strlen(key));
        head->data = data;
        to_add->next = '\0';
        free(to_add);
        //printf("here %d\n", __LINE__);
        return;
    }
    memset(&to_add->key[0], 0, sizeof(to_add->key));
    memcpy(to_add->key, key, strlen(key));
    to_add->data = data;
    to_add->next = '\0';

    while (tmp) {
        if (tmp->next) {
            printf("in add_node key is \n %s \n", tmp->key);
            tmp = tmp->next;
        } else break;
    }
    tmp->next = to_add;
    to_add->gets = 0;
    to_add->sets = 1;
    return;
}

void print_node(struct data_node *to_print) {
    printf("node info:\n key : %s\nvalue:\n%s\n", to_print->key, to_print->data);
}

void replace_data(struct data_node *new) {
    struct data_node *old = get_data_node(new->key);
    free(old->data);
    new->data = (char *) malloc(strlen(new->data));
    memcpy(old->data, new->data, strlen(new->data));
    return;
}

struct data_node *get_data_node(char *key) {
    struct data_node *tmp = head;
    printf("in get_data_node\n");
    if (strcmp(head->key, "null") == 0) {
        printf("null returned\n");
        return NULL;
    }
    while (tmp->key) {
        if ((strncmp(tmp->key, key, strlen(key))) != 0) {
            if (tmp->next == '\0') {
                printf("null returned\n");
                return NULL;
            } else {
                tmp = tmp->next;
            }
        } else {
            break;
        }
    }
    printf("returned key: %s", tmp->key);
    return tmp;
}

void delete_all_nodes(struct data_node *head) {
    struct data_node *tmp;
    while (head->next != NULL) {
        tmp->next = head->next;
        free(head->key);
        free(head->data);
        head = tmp;
    }
    free(head->data);
    free(head->key);
    free(head);
    return;
}

int kv_get(void *kv_handle, const char *key, char **value) {
    struct pingpong_context *ctx = kv_handle;
    struct packet *get_packet = (struct packet *) ctx->buf;
    struct data_node *node;
    char *data;
    printf("here in get\n\n");
    memset(ctx->buf, '\0', EAGER_PROTOCOL_LIMIT);
    memcpy(get_packet->get_request.key, key, strlen(key));
    printf("\nget pkt key == %spkt type %d\n", get_packet->get_request.key, get_packet->type);
    pp_post_recv(ctx, 1);
    pp_post_send(ctx, IBV_WR_SEND, EAGER_PROTOCOL_LIMIT, NULL, NULL, 0);
    pp_wait_completions(ctx, 2);     ////wait for get request answer
    printf("pkt type %d\n", get_packet->type);
    /////swich case for the pkt returned
    printf("now we switching\n");
    switch (get_packet->type) {
        case EAGER_GET_RESPONSE:
            printf("\nHERE IN EAGER_GET_RESPONSE\n");
            node = get_data_node(get_packet->eager_get_response.key);
            if (node == NULL) {
                printf("no key found\n");
                data = (char *) calloc(1, strlen(get_packet->eager_get_response.value));
                memcpy(data, get_packet->eager_get_response.value, strlen(get_packet->eager_get_response.value));
                add_node(get_packet->eager_get_response.key, data, strlen(get_packet->eager_get_response.value));
                printf("node added\n\n");
            } else {//need to check that it works compare to the other switch
                printf("key found\n");
                free(node->data);
                node->data = (char *) calloc(1, strlen(get_packet->eager_get_response.value) * sizeof(char));
                memcpy(node->data, get_packet->eager_get_response.value, strlen(get_packet->eager_get_response.value));
                printf("data replaced\n");
            }
            pp_post_recv(ctx, 1);
            break;

        case RENDEZVOUS_GET_RESPONSE: /* TODO (10LOC): handle a long GET() on the server */
            printf("\n HERE IN RENDEZVOUS_GET_RESPONSE\n");
            node = get_data_node(key);
            struct ibv_mr *mr;
            uint64_t r_addr;
            uint32_t r_k;
            data = (char *) calloc(1, (get_packet->rndv_get_response.value_size) * sizeof(char));
            mr = ibv_reg_mr(ctx->pd, data, get_packet->rndv_get_response.value_size, IBV_ACCESS_LOCAL_WRITE);
            if (!mr) {
                printf("mr failed %d\n", __LINE__);
                exit(0);
            }
            r_addr = get_packet->rndv_get_response.buf_address;
            r_k = get_packet->rndv_get_response.r_key;
            printf("before remote_read, r_addres = %lu, rkey = %lu \n", r_addr, r_k);

            if (remote_read(ctx, r_addr, r_k, mr)) {
                fprintf(stderr, "Client couldn't post send\n");
                return 1;
            }
            pp_wait_completions(ctx, 1);    ////wait fo remote read to finish

            ibv_dereg_mr(mr);
            get_packet = (struct packet *) ctx->buf;
            get_packet->type = ACK;
            memset(get_packet->ack.key, '\0', KEY_SIZE);
            memcpy(get_packet->ack.key, key, strlen(key));
            int pkt_size = sizeof(struct packet) + strlen(get_packet->ack.key);
            if (pp_post_send(ctx, IBV_WR_SEND, pkt_size, NULL, NULL, 0)) {
                fprintf(stderr, "Client couldn't post send\n");
                return 1;
            }
            pp_wait_completions(ctx, 1);//// complition for ack


            if (node == NULL) {
                add_node(key, data, 5);
            } else {
                //printf("key found\n");
                free(node->data);
                node->data = data;
                //printf("data replaced\n");
            }
            /*print_node(head);
            if (head->next) {
                print_node(head->next);
            }*/
            break;
        default:
            printf("\nHERE IN kv_get default\n");
            printf("get pkt point to %p\n", &get_packet);
            printf("pkt type %d\n", get_packet->type);
            //printf("this LID %d\n", ctx->portinfo.lid);
            return 0;
    }
    return 0;
}

int kv_set(void *kv_handle, const char *key, const char *value) {
    int data_size;
    struct pingpong_context *ctx = kv_handle;
    struct packet *set_packet = (struct packet *) ctx->buf;
    int size_of_packet = strlen(value);
    unsigned packet_size = strlen(key) + strlen(value) + sizeof(struct packet);

    printf("in kv_set\n");
    if (size_of_packet < (EAGER_PROTOCOL_LIMIT)) {
        //// *Eager protocol*/
        printf("\nhere in kv_set EAGER_SET_REQUEST\n");
        memset(ctx->buf, '\0', EAGER_PROTOCOL_LIMIT);
        memset(set_packet->eager_set_request.key, '\0', 50 * sizeof(char));
        memset(set_packet->eager_set_request.value, '\0', 4096 * sizeof(char));
        /*loading the data we want to send*/
        set_packet->type = EAGER_SET_REQUEST;
        memcpy(set_packet->eager_set_request.key, key, strlen(key));
        memcpy(set_packet->eager_set_request.value, value, strlen(value));
        pp_post_send(ctx, IBV_WR_SEND, packet_size, NULL, NULL, 0); /* Sends the packet to the server */

        pp_wait_completions(ctx, 1);
    } else {
        ////*RENDEZVOUS protocol*/
        struct packet *rndv_res_packet;
        struct ibv_mr *mr;
        uint64_t r_addr;
        uint32_t r_k = 0;
        data_size = strlen(value) + 1;
        printf("\nhere in kv_set RENDEZVOUS_SET_REQUEST\n");
        memset(ctx->buf, '\0', EAGER_PROTOCOL_LIMIT);
        set_packet->type = RENDEZVOUS_SET_REQUEST;
        memcpy(set_packet->rndv_set_request.key, key, strlen(key));
        set_packet->rndv_set_request.value_size = data_size;

        packet_size = sizeof(struct packet) + strlen(set_packet->rndv_set_request.key) +
                      sizeof(set_packet->rndv_set_request.value_size);
        printf("pkt size %d line %d\n", packet_size, __LINE__);
        if (pp_post_send(ctx, IBV_WR_SEND, packet_size, NULL, NULL, 0) != 0) {
            fprintf(stderr, "Client couldn't post send\n");
            exit(0);
        }
        mr = ibv_reg_mr(ctx->pd, value, set_packet->rndv_set_request.value_size, IBV_ACCESS_LOCAL_WRITE);
        if (!mr) {
            printf("mr failed\n");
            exit(0);
        }
        pp_post_recv(ctx, 1);
        ////wait for complition on send and receive answer from the server
        pp_wait_completions(ctx, 2);
        rndv_res_packet = (struct packet *) ctx->buf;
        r_addr = rndv_res_packet->rndv_set_response.buf_address;
        r_k = rndv_res_packet->rndv_set_response.r_key;

        if (remote_write(ctx, r_addr, r_k, mr)) {
            fprintf(stderr, "Client couldn't post send\n");
            return 1;
        }
        pp_wait_completions(ctx, 1);    ////complition fot remote write
        ibv_dereg_mr(mr);
        rndv_res_packet = (struct packet *) ctx->buf;
        rndv_res_packet->type = ACK;
        memset(rndv_res_packet->ack.key, '\0', KEY_SIZE);
        memcpy(rndv_res_packet->ack.key, key, strlen(key));
        printf("ACK key: %s\n%s\n", rndv_res_packet->ack.key, key);
        packet_size = sizeof(struct packet) + strlen(rndv_res_packet->ack.key);
        if (pp_post_send(ctx, IBV_WR_SEND, packet_size, NULL, NULL, 0)) {
            fprintf(stderr, "Client couldn't post send\n");
            return 1;
        }

        pp_wait_completions(ctx, 1);//// complition for ack
    }
    memset(ctx->buf, '\0', EAGER_PROTOCOL_LIMIT);
    printf("leaving kv_set buf clean\n");
    return 0;
}

void kv_release(char *value) {
    /* TODO (2LOC): free value */
}

int read_from_test_file(FILE *fp, char *operation_buf, char *key_buf, char *val_buf) {
    /*first check eof*/
    /*read op*/
    fgets(operation_buf, 5, fp);
    if (feof(fp)) {
        printf("\n End of file reached.");
        return -1;
    }
    /*read key*/
    fgets(key_buf, EAGER_PROTOCOL_LIMIT, fp);
    /*read val*/
    /*if op is get not need to read val*/
    if (strcmp(operation_buf, "get\n") == 0) {
        return 1;
    }
    fgets(val_buf, MAX_TEST_SIZE, fp);
    return 1;
}

int kv_close(void *kv_handle) {
    return pp_close_ctx((struct pingpong_context *) kv_handle);
}

void run_server() {
    struct kv_server_address server = {0};
    struct mkv_ctx *ctx;
    ctx = malloc(sizeof(*ctx) + MAX_CLIENTS * sizeof(void *));
    if (!ctx) {
        printf("not ctx\n");
    }
    int cnt = 0;
    unsigned packet_size = 0;
    struct multi_answer *current_ctx = (struct multi_answer *) malloc(sizeof(struct multi_answer));
    ////////creating array of ctx. then creat connection to each 1 of them, after we change the cq of each ctx to point
    ////////the cq of ctx[0], this way we create multi ctx, each have hes own qp with 1 cq for all pf them.
    for (int i = 0; i < MAX_CLIENTS; i++) {
        printf("waitnig for clients\n");
        assert(0 == s_orig_main(&server, EAGER_PROTOCOL_LIMIT, g_argc, g_argv, &ctx->kv_ctxs[i], i));
        ctx->kv_ctxs[i]->ctx_num = i;
        memset(ctx->kv_ctxs[i]->buf, '\0', EAGER_PROTOCOL_LIMIT);
        pp_post_recv(ctx->kv_ctxs[i], 1);
    }
    if (indexer_flag == 1) {
        /////post receive to all the clients
        ///// at this point all the clients are connected, the indexer will send start_work msg
        printf("\n\nIM THE INDEXER\n\n");
        for (cnt = 0; cnt < MAX_CLIENTS; cnt++) {
            memcpy(ctx->kv_ctxs[cnt]->buf, "start", 10);
            memset(ctx->kv_ctxs[cnt]->buf, '\0', EAGER_PROTOCOL_LIMIT);
        }
        printf("CQ ADDRESS:\n");
        for (cnt = 0; cnt < MAX_CLIENTS; cnt++) {
            printf("cq[%d] at  %p\n", cnt, ctx->kv_ctxs[cnt]->cq);
        }
        for (cnt = 0; cnt < MAX_CLIENTS; cnt++) {
            pp_post_send(ctx->kv_ctxs[cnt], IBV_WR_SEND, packet_size, NULL, NULL, 0);
        }
        ////send start msg to all the clients and wait for their complition
    } else {
        printf("\n\n!!! RUN SERVER!!!\n\n");
    }
    printf("\n\nroutine \n\n");
    while (1) {
        multi_wait_completions(ctx, 1, current_ctx);
    }
}


int mkv_open(struct kv_server_address *servers, void **mkv_h) {
    struct mkv_ctx *ctx;
    unsigned count = 0;
    while (servers[count++].servername); /* count servers */
    ctx = malloc(sizeof(*ctx) + count * sizeof(void *));
    if (!ctx) {
        return 1;
    }
    printf("\nMKV_OPEN\n");
    ctx->num_servers = count;
    for (count = 0; count < ctx->num_servers; count++) {
        printf("\n%d\n", count + 1);
        if (orig_main(&servers[count], EAGER_PROTOCOL_LIMIT, g_argc, g_argv, &ctx->kv_ctxs[count])) {
            return 1;
        }
    }
    *mkv_h = ctx;
    return 0;
}

int mkv_set(void *mkv_h, unsigned kv_id, const char *key, const char *value) {
    struct mkv_ctx *ctx = mkv_h;

    return kv_set(ctx->kv_ctxs[kv_id], key, value);
}

int mkv_get(void *mkv_h, unsigned kv_id, const char *key, char **value) {
    struct mkv_ctx *ctx = mkv_h;
    assert(0 == kv_get(ctx->kv_ctxs[kv_id], key, value));
    return 0;
}

void mkv_release(char *value) {
    kv_release(value);
}

void mkv_close(void *mkv_h) {
    unsigned count;
    struct mkv_ctx *ctx = mkv_h;
    for (count = 0; count < ctx->num_servers; count++) {
        pp_close_ctx((struct pingpong_context *) ctx->kv_ctxs[count]);
    }
    free(ctx);
}

struct dkv_ctx {
    struct mkv_ctx *mkv;
    struct pingpong_context *indexer;
};

int dkv_open(struct kv_server_address *servers, /* array of servers */
             struct kv_server_address *indexer, /* single indexer */
             void **dkv_h) {
    struct dkv_ctx *ctx = (struct dkv_ctx *) malloc(sizeof(struct dkv_ctx));
    printf("conect to indexer\n");
    if (orig_main(indexer, EAGER_PROTOCOL_LIMIT, g_argc, g_argv, &ctx->indexer)) {
        return 1;
    }
    printf("conect to servers\n");
    if (mkv_open(servers, (void **) &ctx->mkv)) {
        return 1;
    }
    *dkv_h = ctx;
    return 0;
}

unsigned int get_location(void *dkv_h, const char *key) {
    printf("in get_location\n");
    struct dkv_ctx *ctx = dkv_h;
    struct packet *set_packet = (struct packet *) ctx->indexer->buf;
    unsigned packet_size = strlen(key) + sizeof(struct packet);
    set_packet->type = FIND;
    set_packet->find.num_of_servers = NUM_OF_SERVERS;
    strcpy(set_packet->find.key, key);

    pp_post_recv(ctx->indexer, 1); // Posts a receive-buffer for LOCATION
    pp_post_send(ctx->indexer, IBV_WR_SEND, packet_size, NULL, NULL, 0); // Sends the packet to the server
    assert(pp_wait_completions(ctx->indexer, 2) == 0); // wait for both to complete
    assert(set_packet->type = LOCATION);
    printf("location receivd SET %d\n", set_packet->location.selected_server);
    return set_packet->location.selected_server;

}

int dkv_set(void *dkv_h, const char *key, const char *value, unsigned length, unsigned int loc) {
    struct dkv_ctx *ctx = dkv_h;
    struct packet *set_packet = (struct packet *) ctx->indexer->buf;

    return mkv_set(ctx->mkv, loc, key, value);
}

int dkv_get(void *dkv_h, const char *key, char **value, unsigned *length, unsigned int loc) {
    struct dkv_ctx *ctx = dkv_h;
    struct packet *set_packet = (struct packet *) ctx->indexer->buf;
    printf("get to %d\n", loc);
    assert(0 == mkv_get(ctx->mkv, loc, key, value));
    return 0;
}

void dkv_release(char *value) {
    mkv_release(value);
}

int dkv_close(void *dkv_h) {
    struct dkv_ctx *ctx = dkv_h;
    pp_close_ctx(ctx->indexer);
    mkv_close(ctx->mkv);
    free(ctx);
}

int main(int argc, char **argv) {
    void *kv_ctx; /* handle to internal KV-client context */
    head = (struct data_node *) malloc(sizeof(struct data_node));
    memset(&head->key[0], 0, sizeof(head->key));
    struct data_node *temp;
    temp = head;
    char t[4] = "null";
    memcpy(head->key, t, KEY_SIZE);
    head->mr = NULL;
    head->sets = 1;
    head->gets = 0;
    head->data = '\0';
    head->next = '\0';
    int cnt = 0;
    int req = 0;
    int location;
    char *recv_buffer;

    char send_buffer[MAX_TEST_SIZE] = {0};
    //////

    FILE *fp = fopen("testfile.txt", "r");
    if (!fp) {
        printf("File open failed\n");
        exit(0);
    }

    struct kv_server_address servers[NUM_OF_SERVERS] = {
            {
                    .servername = "localhost",
                    .port = 7096
            },
            {0}
    };
    for (int i = 0; i < NUM_OF_SERVERS; i++) {
        servers[i].port = 7096 + i;
    }

    struct kv_server_address indexer[1] = {
            {
                    .servername = "localhost",
                    .port = 7095
            },
            {0}
    };

    g_argc = argc;
    g_argv = argv;
    if (argc >= 6) {
        /*-----CLIENT-----*/
        printf("\n\n!!!CLIENT!!!\n\n");
        assert(0 == dkv_open(servers, indexer, &kv_ctx));
    } else {
        /*-----SERVER-----*/
        run_server();
    }
    printf("\n\n!!!CLIENT!!! HERE\n\n");

    /////////////////////////////////////
    //////here i need to wait until all clients will connect to the servers, in order to do that we need to
    //////send ready msg from at least on of the servers

    struct dkv_ctx *ctx = kv_ctx;
    pp_post_recv(ctx->indexer, 1);
    pp_wait_completions(ctx->indexer, 1);

    //////EAGER TST
    assert(100 < MAX_TEST_SIZE);
    /*for (int i = 0; i < 5; i++) {
        memset(send_buffer, 'a', 100);
        assert(0 == dkv_set(kv_ctx, "1", send_buffer, strlen(send_buffer)));
        memset(send_buffer, 'b', 100);
        assert(0 == dkv_set(kv_ctx, "1", send_buffer, strlen(send_buffer)));
        memset(send_buffer, 'c', 100);
        assert(0 == dkv_set(kv_ctx, "1", send_buffer, strlen(send_buffer)));
        memset(send_buffer, 'e', 100);
        assert(0 == dkv_set(kv_ctx, "2", send_buffer, strlen(send_buffer)));
        memset(send_buffer, 'f', 100);
        assert(0 == dkv_set(kv_ctx, "2", send_buffer, strlen(send_buffer)));
        memset(send_buffer, 'g', 100);
        assert(0 == dkv_set(kv_ctx, "2", send_buffer, strlen(send_buffer)));
    }
    assert(0 == dkv_get(kv_ctx, "1", &recv_buffer, &g_argc));
    assert(0 == dkv_get(kv_ctx, "2", &recv_buffer, &g_argc));
    //assert(0 == strcmp(send_buffer, recv_buffer));


    //////RNDW TST
    /*for (int i = 0; i < 10; i++) {
        memset(send_buffer, 'a', 5000);
        assert(0 == dkv_set(kv_ctx, "1", send_buffer, strlen(send_buffer)));
        memset(send_buffer, 'b', 5000);
        assert(0 == dkv_set(kv_ctx, "1", send_buffer, strlen(send_buffer)));
        memset(send_buffer, 'c', 5000);
        assert(0 == dkv_set(kv_ctx, "1", send_buffer, strlen(send_buffer)));
        memset(send_buffer, 'a', 5000);
        assert(0 == dkv_set(kv_ctx, "2", send_buffer, strlen(send_buffer)));
        memset(send_buffer, 'b', 5000);
        assert(0 == dkv_set(kv_ctx, "2", send_buffer, strlen(send_buffer)));
        memset(send_buffer, 'c', 5000);
        assert(0 == dkv_set(kv_ctx, "2", send_buffer, strlen(send_buffer)));
    }
    assert(0 == dkv_get(kv_ctx, "1", &recv_buffer, &g_argc));
    assert(0 == dkv_get(kv_ctx, "2", &recv_buffer, &g_argc));
    printf("after 1 get\n");*/

    /////READ FROM TST FILE
    while (1) {
        char *operation_buf = (char *) calloc(4, sizeof(char));
        char *key_buf = (char *) calloc(EAGER_PROTOCOL_LIMIT, sizeof(char));
        char *val_buf = (char *) calloc(MAX_TEST_SIZE, sizeof(char));//change here

        if (read_from_test_file(fp, operation_buf, key_buf, val_buf) == -1) {
            break;
        }
        location = get_location(kv_ctx, key_buf);
        printf("op is: %s", operation_buf);
        printf("REQUEST NUM = %d\n", req);
        req++;
        if (strcmp(operation_buf, "set\n") == 0) {
            printf("key is:\n%s\n", key_buf);
            assert(0 == dkv_set(kv_ctx, key_buf, val_buf, strlen(val_buf), location));
        } else if (strcmp(operation_buf, "get\n") == 0) {// set operation
            printf("key is:\n%s\n", key_buf);
            assert(0 == dkv_get(kv_ctx, key_buf, &recv_buffer, &g_argc, location));
            printf("\n\n%d GET!!!\n\n", cnt);
            cnt++;
        } else {
            printf("unknown operation\n");
            printf("%s\n", operation_buf);
            exit(1);
        }
        free(operation_buf);
        free(key_buf);
        free(val_buf);
    }
    cnt = 0;
    temp = head;
    printf("\n\nIM MEMORY\n\n");
    while (temp) {
        printf("\n\nNODE NUM %d\n", cnt);
        cnt++;
        printf("key: %s\n val: %s\n", temp->key, temp->data);
        if (temp->next) temp = temp->next;
        else break;
    }
    printf("\n\nEND!!END!!END!!\n\n");
    kv_close(kv_ctx);
    return 0;
}
