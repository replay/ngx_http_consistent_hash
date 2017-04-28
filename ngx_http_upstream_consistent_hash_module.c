

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

#define CONSISTENT_DEBUG 0

#define MMC_CONSISTENT_POINTS 160 
#define MMC_CONSISTENT_BUCKETS 1024

#define HASH_DATA_LENGTH 28


typedef struct {
    ngx_array_t                     *values;
    ngx_array_t                     *lengths;
} ngx_http_upstream_consistent_hash_srv_conf_t;

typedef struct {
    uint32_t                     point;
    struct sockaddr             *sockaddr;
    socklen_t                    socklen;
    ngx_str_t                    name;
} ngx_http_upstream_consistent_hash_node;

typedef struct {
    ngx_int_t                                     numpoints;
    ngx_uint_t                                    nnodes;
    ngx_http_upstream_consistent_hash_node       *nodes;
} ngx_http_upstream_consistent_hash_continuum;

typedef struct {
    ngx_http_upstream_consistent_hash_node       *buckets[MMC_CONSISTENT_BUCKETS];
    ngx_http_upstream_consistent_hash_continuum  *continuum;
} ngx_http_upstream_consistent_hash_buckets;

typedef struct {
    /* the round robin data must be first */
    ngx_http_upstream_consistent_hash_buckets    *peers;

    u_char                                        tries;
    uint32_t                                      point;

    ngx_event_get_peer_pt                         get_rr_peer;
} ngx_http_upstream_consistent_hash_peer_data_t;

static void * ngx_http_upstream_consistent_hash_create_srv_conf(ngx_conf_t *cf);

static ngx_int_t ngx_http_upstream_init_consistent_hash(ngx_conf_t*, 
        ngx_http_upstream_srv_conf_t*);
static ngx_int_t ngx_http_upstream_init_consistent_hash_peer(
        ngx_http_request_t*, ngx_http_upstream_srv_conf_t*);
static char * ngx_http_upstream_consistent_hash(ngx_conf_t*,
        ngx_command_t*, void*);
static ngx_int_t ngx_http_upstream_get_consistent_hash_peer(
        ngx_peer_connection_t*, void*);
static void ngx_http_upstream_free_consistent_hash_peer(
        ngx_peer_connection_t*, void*, ngx_uint_t); 
static int ngx_http_upstream_consistent_hash_compare_continuum_nodes (
        const ngx_http_upstream_consistent_hash_node*, 
        const ngx_http_upstream_consistent_hash_node*);
#if (CONSISTENT_DEBUG)
static void ngx_http_upstream_consistent_hash_print_continuum (ngx_conf_t*, 
        ngx_http_upstream_consistent_hash_continuum*);
static void ngx_http_upstream_consistent_hash_print_buckets (ngx_conf_t *cf, 
        ngx_http_upstream_consistent_hash_buckets*);
#endif
static ngx_http_upstream_consistent_hash_node* 
ngx_http_upstream_consistent_hash_find(
        ngx_http_upstream_consistent_hash_continuum*, uint32_t);


static ngx_command_t  ngx_http_upstream_consistent_hash_commands[] = { 

    {   ngx_string("consistent_hash"),
        NGX_HTTP_UPS_CONF|NGX_CONF_TAKE1,
        ngx_http_upstream_consistent_hash,
        0,
        0,
        NULL },

    ngx_null_command
};

static ngx_http_module_t  ngx_http_upstream_consistent_hash_module_ctx = { 
    NULL,                                  /* preconfiguration */
    NULL,                                  /* postconfiguration */

    NULL,                                  /* create main configuration */
    NULL,                                  /* init main configuration */

    ngx_http_upstream_consistent_hash_create_srv_conf, /* create server configuration */
    NULL,                                              /* merge server configuration */

    NULL,                                  /* create location configuration */
    NULL                                   /* merge location configuration */
};

ngx_module_t  ngx_http_upstream_consistent_hash_module = {
    NGX_MODULE_V1,
    &ngx_http_upstream_consistent_hash_module_ctx, /* module context */
    ngx_http_upstream_consistent_hash_commands,    /* module directives */
    NGX_HTTP_MODULE,                               /* module type */
    NULL,                                          /* init master */
    NULL,                                          /* init module */
    NULL,                                          /* init process */
    NULL,                                          /* init thread */
    NULL,                                          /* exit thread */
    NULL,                                          /* exit process */
    NULL,                                          /* exit master */
    NGX_MODULE_V1_PADDING
};


ngx_int_t
ngx_http_upstream_init_consistent_hash(ngx_conf_t *cf, 
        ngx_http_upstream_srv_conf_t *us)
{
    /* ip max 15, :port max 6, maxweight is highest number of uchar */
    u_char                                        hash_data[HASH_DATA_LENGTH];
    uint32_t                                      step;
    ngx_uint_t                                    i, j, k, n, points = 0;
    ngx_http_upstream_server_t                   *server;
    ngx_http_upstream_consistent_hash_buckets    *buckets;
    ngx_http_upstream_consistent_hash_continuum  *continuum;

    for (i=0;i<HASH_DATA_LENGTH;i++) hash_data[i] = 0;

    step = (uint32_t) (0xffffffff / MMC_CONSISTENT_BUCKETS);

    buckets = ngx_pcalloc(cf->pool, 
            sizeof(ngx_http_upstream_consistent_hash_buckets));

    us->peer.init = ngx_http_upstream_init_consistent_hash_peer;

    if (!us->servers) {
        return NGX_ERROR;
    }

    server = us->servers->elts;

    for (n = 0, i = 0; i < us->servers->nelts; i++) {
        n += server[i].naddrs;
        points += server[i].weight * server[i].naddrs * MMC_CONSISTENT_POINTS;
    }

    continuum = ngx_pcalloc(cf->pool, 
            sizeof(ngx_http_upstream_consistent_hash_continuum));
    continuum->nodes = ngx_pcalloc(cf->pool, 
            sizeof(ngx_http_upstream_consistent_hash_node) * points);

    for (i = 0; i < us->servers->nelts; i++) {
        for (j = 0; j < server[i].naddrs; j++) {
            for (k = 0; k < (MMC_CONSISTENT_POINTS * server[i].weight); k++) {
                ngx_snprintf(hash_data, HASH_DATA_LENGTH, "%V-%ui%Z", &server[i].addrs[j].name, k);
                continuum->nodes[continuum->nnodes].sockaddr = server[i].addrs[j].sockaddr;
                continuum->nodes[continuum->nnodes].socklen = server[i].addrs[j].socklen;
                continuum->nodes[continuum->nnodes].name = server[i].addrs[j].name;
                continuum->nodes[continuum->nnodes].name.data[server[i].addrs[j].name.len] = 0;
                continuum->nodes[continuum->nnodes].point = ngx_crc32_long(hash_data, ngx_strlen(hash_data));
                continuum->nnodes++;
            }
        }
    }

    qsort(continuum->nodes, continuum->nnodes, 
            sizeof(ngx_http_upstream_consistent_hash_node), 
            (const void*) ngx_http_upstream_consistent_hash_compare_continuum_nodes);

    for (i = 0; i < MMC_CONSISTENT_BUCKETS; i++) {
        buckets->buckets[i] = 
            ngx_http_upstream_consistent_hash_find(continuum, step * i);
    }

#if (CONSISTENT_DEBUG)
    ngx_http_upstream_consistent_hash_print_continuum(cf, continuum);
    ngx_http_upstream_consistent_hash_print_buckets(cf, buckets);
#endif

    buckets->continuum = continuum;
    us->peer.data = buckets;

    return NGX_OK;
}


static int 
ngx_http_upstream_consistent_hash_compare_continuum_nodes(
        const ngx_http_upstream_consistent_hash_node *node1, 
        const ngx_http_upstream_consistent_hash_node *node2)
{
    if (node1->point < node2->point) {
        return -1;
    }
    else if (node1->point > node2->point) {
        return 1;
    }

    return 0;
}


static ngx_int_t
ngx_http_upstream_init_consistent_hash_peer(ngx_http_request_t *r,
        ngx_http_upstream_srv_conf_t *us)
{
    ngx_str_t                                          evaluated_key_to_hash;
    ngx_http_upstream_consistent_hash_srv_conf_t      *uchscf;
    ngx_http_upstream_consistent_hash_peer_data_t     *uchpd;

    uchscf = ngx_http_conf_upstream_srv_conf(us,
                                          ngx_http_upstream_consistent_hash_module);
    if (uchscf == NULL) {
        return NGX_ERROR;
    }

    uchpd = ngx_pcalloc(r->pool, sizeof(ngx_http_upstream_consistent_hash_peer_data_t));
    if (uchpd == NULL) {
        return NGX_ERROR;
    }

    r->upstream->peer.data = uchpd->peers;
    uchpd->peers = us->peer.data;

    if (ngx_http_script_run(r, &evaluated_key_to_hash, 
                uchscf->lengths->elts, 0, uchscf->values->elts) == NULL)
    {
        return NGX_ERROR;
    }

    uchpd->point = 
        ngx_crc32_long(evaluated_key_to_hash.data, evaluated_key_to_hash.len);

    r->upstream->peer.free = ngx_http_upstream_free_consistent_hash_peer;
    r->upstream->peer.get = ngx_http_upstream_get_consistent_hash_peer;
    r->upstream->peer.data = uchpd;

    return NGX_OK;
}


static ngx_int_t
ngx_http_upstream_get_consistent_hash_peer(ngx_peer_connection_t *pc, 
        void *data)
{
    ngx_http_upstream_consistent_hash_peer_data_t *uchpd = data;

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, pc->log, 0,
                   "consistent hash point: %ui", uchpd->point);

    pc->cached = 0;
    pc->connection = NULL;

    pc->sockaddr = 
        uchpd->peers->buckets[uchpd->point % MMC_CONSISTENT_BUCKETS]->sockaddr;
    pc->socklen = 
        uchpd->peers->buckets[uchpd->point % MMC_CONSISTENT_BUCKETS]->socklen;
    pc->name = 
        &uchpd->peers->buckets[uchpd->point % MMC_CONSISTENT_BUCKETS]->name;

    return NGX_OK;
}


static ngx_http_upstream_consistent_hash_node*
ngx_http_upstream_consistent_hash_find(
        ngx_http_upstream_consistent_hash_continuum *continuum, 
        uint32_t point)
{
    ngx_uint_t mid = 0, lo = 0, hi = continuum->nnodes - 1;

    while (1)
    {
        if (point <= continuum->nodes[lo].point || 
                point > continuum->nodes[hi].point) {
            return &continuum->nodes[lo];
        }

        /* test middle point */
        mid = lo + (hi - lo) / 2;

        /* perfect match */
        if (point <= continuum->nodes[mid].point && 
                point > (mid ? continuum->nodes[mid-1].point : 0)) {
            return &continuum->nodes[mid];
        }

        /* too low, go up */
        if (continuum->nodes[mid].point < point) {
            lo = mid + 1;
        }
        else {
            hi = mid - 1;
        }
    }
}


static void 
ngx_http_upstream_free_consistent_hash_peer(ngx_peer_connection_t *pc, void *data, 
        ngx_uint_t state) 
{
    pc->tries = 0;
}


static void *
ngx_http_upstream_consistent_hash_create_srv_conf(ngx_conf_t *cf)
{
    ngx_http_upstream_consistent_hash_srv_conf_t  *conf;

    conf = ngx_pcalloc(cf->pool,
                       sizeof(ngx_http_upstream_consistent_hash_srv_conf_t));
    if (conf == NULL) {
        return NULL;
    }

    /*
     * set by ngx_pcalloc(): 
     *
     *     conf->lengths = NULL; 
     *     conf->values = NULL;
    */

    return conf;
}


static char *
ngx_http_upstream_consistent_hash(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_str_t                                    *value;
    ngx_http_script_compile_t                     sc;
    ngx_http_upstream_srv_conf_t                 *uscf;
    ngx_http_upstream_consistent_hash_srv_conf_t *uchscf;

    value = cf->args->elts;

    uscf = ngx_http_conf_get_module_srv_conf(cf, ngx_http_upstream_module);
    uchscf = ngx_http_conf_upstream_srv_conf(uscf,
                                          ngx_http_upstream_consistent_hash_module);

    ngx_memzero(&sc, sizeof(ngx_http_script_compile_t));

    sc.cf = cf;
    sc.source = &value[1];
    sc.lengths = &uchscf->lengths;
    sc.values = &uchscf->values;
    sc.complete_lengths = 1;
    sc.complete_values = 1;

    if (ngx_http_script_compile(&sc) != NGX_OK) {
        return NGX_CONF_ERROR;
    }

    uscf->peer.init_upstream = ngx_http_upstream_init_consistent_hash;

    uscf->flags = NGX_HTTP_UPSTREAM_CREATE
        |NGX_HTTP_UPSTREAM_WEIGHT;

    return NGX_CONF_OK;
}


#if (CONSISTENT_DEBUG)
static void 
ngx_http_upstream_consistent_hash_print_continuum (ngx_conf_t *cf, 
        ngx_http_upstream_consistent_hash_continuum *continuum)
{
    ngx_uint_t i;

    printf("print continuum\n");

    for (i = 0; i < continuum->nnodes; i++) {
        printf("%i: name %.19s point %u\n", (int)i, 
                (char*)continuum->nodes[i].name.data, 
                (unsigned int)continuum->nodes[i].point);
    }
}


static void 
ngx_http_upstream_consistent_hash_print_buckets (ngx_conf_t *cf, 
        ngx_http_upstream_consistent_hash_buckets *buckets)
{
    ngx_uint_t i;

    printf("print buckets\n");

    for (i = 0; i < MMC_CONSISTENT_BUCKETS; i++) {
        printf("%i: name %s point %u\n", (int)i,
                (char*)buckets->buckets[i]->name.data, 
                (unsigned int)buckets->buckets[i]->point);
    }
}
#endif
