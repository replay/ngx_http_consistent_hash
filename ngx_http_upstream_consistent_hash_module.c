

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

#define MMC_CONSISTENT_POINTS 160 
#define MMC_CONSISTENT_BUCKETS 1024



typedef struct
{
    ngx_uint_t                   point;
    struct sockaddr             *sockaddr;
    socklen_t                    socklen;
    ngx_str_t                    name;
} ngx_http_upstream_consistent_hash_node;

typedef struct
{
    ngx_int_t                                     numpoints;
    ngx_uint_t                                    nnodes;
    ngx_http_upstream_consistent_hash_node       *nodes;
} ngx_http_upstream_consistent_hash_continuum;

typedef struct
{
    ngx_http_upstream_consistent_hash_node       *buckets[MMC_CONSISTENT_BUCKETS];
    ngx_http_upstream_consistent_hash_continuum  *continuum;
} ngx_http_upstream_consistent_hash_buckets;

typedef struct {
    /* the round robin data must be first */
    ngx_http_upstream_consistent_hash_buckets  *peers;

    u_char                                        tries;
    ngx_uint_t                                    point;

    ngx_event_get_peer_pt                         get_rr_peer;
} ngx_http_upstream_consistent_hash_peer_data_t;

ngx_int_t ngx_http_upstream_init_consistent_hash(ngx_conf_t*, ngx_http_upstream_srv_conf_t*);
static ngx_int_t ngx_http_upstream_init_consistent_hash_peer(ngx_http_request_t*, ngx_http_upstream_srv_conf_t*);
static char * ngx_http_upstream_consistent_hash(ngx_conf_t*, ngx_command_t*, void*);
static ngx_int_t ngx_http_upstream_get_consistent_hash_peer(ngx_peer_connection_t*, void*);
void ngx_http_upstream_free_consistent_hash_peer(ngx_peer_connection_t*, void*, ngx_uint_t); 
int ngx_http_upstream_consistent_hash_compare_continuum_nodes (const ngx_http_upstream_consistent_hash_node*, const ngx_http_upstream_consistent_hash_node*);
void ngx_http_upstream_consistent_hash_print_continuum (ngx_conf_t*, ngx_http_upstream_consistent_hash_continuum*);
void ngx_http_upstream_consistent_hash_print_buckets (ngx_conf_t *cf, ngx_http_upstream_consistent_hash_buckets*);
static ngx_http_upstream_consistent_hash_node* ngx_http_upstream_consistent_hash_find(ngx_http_upstream_consistent_hash_continuum*, ngx_uint_t);

static ngx_array_t * ngx_http_upstream_consistent_hash_key_vars_lengths;
static ngx_array_t * ngx_http_upstream_consistent_hash_key_vars_values;

static ngx_command_t  ngx_http_upstream_consistent_hash_commands[] = { 

    { ngx_string("consistent_hash"),
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

    NULL,                                  /* create server configuration */
    NULL,                                  /* merge server configuration */

    NULL,                                  /* create location configuration */
    NULL                                   /* merge location configuration */
};

ngx_module_t  ngx_http_upstream_consistent_hash_module = {
    NGX_MODULE_V1,
    &ngx_http_upstream_consistent_hash_module_ctx, /* module context */
    ngx_http_upstream_consistent_hash_commands,    /* module directives */
    NGX_HTTP_MODULE,                       /* module type */
    NULL,                                  /* init master */
    NULL,                                  /* init module */
    NULL,                                  /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    NULL,                                  /* exit process */
    NULL,                                  /* exit master */
    NGX_MODULE_V1_PADDING
};

ngx_int_t
ngx_http_upstream_init_consistent_hash(ngx_conf_t *cf, ngx_http_upstream_srv_conf_t *us)
{
    ngx_uint_t                                    i, j, k, n, points = 0;
    u_char                                       *hash_data;
    ngx_uint_t                                    step = 0xffffffff / MMC_CONSISTENT_BUCKETS;
    ngx_http_upstream_server_t                   *server;
    ngx_http_upstream_consistent_hash_continuum  *continuum;
    ngx_http_upstream_consistent_hash_buckets    *buckets;

    buckets = ngx_pcalloc(cf->pool, sizeof(ngx_http_upstream_consistent_hash_buckets));

//    ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "starting to init continuum", 0);

    us->peer.init = ngx_http_upstream_init_consistent_hash_peer;

    if (!us->servers) {
      return NGX_ERROR;
    }

    server = us->servers->elts;

    for (n = 0, i = 0; i < us->servers->nelts; i++) {
        n += server[i].naddrs;
        points += server[i].weight * MMC_CONSISTENT_POINTS;
        //points += MMC_CONSISTENT_POINTS;
    }
    
    continuum = ngx_pcalloc(cf->pool, sizeof(ngx_http_upstream_consistent_hash_continuum));
    continuum->nodes = ngx_pcalloc(cf->pool, sizeof(ngx_http_upstream_consistent_hash_node) * points);

    /* ip max 15, :port max 6, maxweight is highest number of uchar */
    hash_data = malloc(sizeof(u_char) * 28);

    for (i = 0; i < us->servers->nelts; i++) {
        server[i].name.data[server[i].name.len] = 0;
        for (k = 0; k < (MMC_CONSISTENT_POINTS * server[i].weight); k++) {
            snprintf((char*) hash_data, sizeof(u_char) * 28, "%s-%i", server[i].name.data, (int)k);
            j = (MMC_CONSISTENT_POINTS * server[i].weight) % server[i].naddrs; // do some balancing if there are multiple ips for that hostname
            continuum->nodes[continuum->nnodes].sockaddr = server[i].addrs[j].sockaddr;
            continuum->nodes[continuum->nnodes].socklen = server[i].addrs[j].socklen;
            continuum->nodes[continuum->nnodes].name = server[i].name;
            continuum->nodes[continuum->nnodes].name.data[server[i].addrs[j].name.len] = 0;
            continuum->nodes[continuum->nnodes].point = ngx_crc32_long(hash_data, strlen((char *) hash_data));
            //printf("adding server to continuum at %u: key %s, point %u\n", (unsigned int) j, (char *)hash_data, (unsigned int) continuum->nodes[continuum->nnodes].point);
            continuum->nnodes++;
        }
    }

    free (hash_data);

    qsort(continuum->nodes, continuum->nnodes, sizeof(ngx_http_upstream_consistent_hash_node), (const void*) ngx_http_upstream_consistent_hash_compare_continuum_nodes);

    //ngx_http_upstream_consistent_hash_print_continuum(cf, continuum);

    for (i=0; i<MMC_CONSISTENT_BUCKETS; i++) {
      buckets->buckets[i] = ngx_http_upstream_consistent_hash_find(continuum, step * i);
      //printf("added bucket num %u, received host %s\n", (unsigned int) i, buckets->buckets[i]->name.data);
    }

    //ngx_http_upstream_consistent_hash_print_buckets(cf, buckets);

    //ngx_http_upstream_consistent_hash_print_continuum(cf, continuum);
    buckets->continuum = continuum;
    us->peer.data = buckets;

    return NGX_OK;
}


int ngx_http_upstream_consistent_hash_compare_continuum_nodes (const ngx_http_upstream_consistent_hash_node *node1, const ngx_http_upstream_consistent_hash_node *node2)
{
  if (node1->point < node2->point)
  {
    return -1;
  }
  if (node1->point > node2->point)
  {
    return 1;
  }
  return 0;
}

static ngx_int_t
ngx_http_upstream_init_consistent_hash_peer(ngx_http_request_t *r,
    ngx_http_upstream_srv_conf_t *us)
{
  ngx_str_t evaluated_key_to_hash;
  ngx_http_upstream_consistent_hash_peer_data_t     *uchpd;

  uchpd = ngx_pcalloc(r->pool, sizeof(ngx_http_upstream_consistent_hash_peer_data_t));
  if (uchpd == NULL) {
    return NGX_ERROR;
  }
  r->upstream->peer.data = uchpd->peers;
  uchpd->peers = us->peer.data;

  if (ngx_http_script_run(r, &evaluated_key_to_hash, ngx_http_upstream_consistent_hash_key_vars_lengths->elts, 0, ngx_http_upstream_consistent_hash_key_vars_values->elts) == NULL)
  {
    return NGX_ERROR;
  }
  uchpd->point = ngx_crc32_long(evaluated_key_to_hash.data, evaluated_key_to_hash.len);

  //printf("key %s results to point %u which is bucket %u\n", evaluated_key_to_hash.data, (unsigned) uchpd->point, (unsigned) uchpd->point % MMC_CONSISTENT_BUCKETS);

  r->upstream->peer.free = ngx_http_upstream_free_consistent_hash_peer;
  r->upstream->peer.get = ngx_http_upstream_get_consistent_hash_peer;
  r->upstream->peer.data = uchpd;

  return NGX_OK;
}

static ngx_int_t
ngx_http_upstream_get_consistent_hash_peer(ngx_peer_connection_t *pc, void *data)
{
  ngx_http_upstream_consistent_hash_peer_data_t *uchpd = data;

  pc->cached = 0;
  pc->connection = NULL;

  pc->sockaddr = uchpd->peers->buckets[uchpd->point % MMC_CONSISTENT_BUCKETS]->sockaddr;
  pc->socklen = uchpd->peers->buckets[uchpd->point % MMC_CONSISTENT_BUCKETS]->socklen;
  pc->name = &uchpd->peers->buckets[uchpd->point % MMC_CONSISTENT_BUCKETS]->name;
  
  //printf("returning %s\n", pc->name->data);
  return NGX_OK;
}

static ngx_http_upstream_consistent_hash_node*
ngx_http_upstream_consistent_hash_find(ngx_http_upstream_consistent_hash_continuum *continuum, ngx_uint_t point)
{
  ngx_uint_t mid = 0, lo = 0, hi = continuum->nnodes - 1;

  while (1)
  {
    if (point <= continuum->nodes[lo].point || point > continuum->nodes[hi].point) {
      return &continuum->nodes[lo];
    }

    /* test middle point */
    mid = lo + (hi - lo) / 2;

    /* perfect match */
    if (point <= continuum->nodes[mid].point && point > (mid ? continuum->nodes[mid-1].point : 0)) {
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

void 
ngx_http_upstream_free_consistent_hash_peer(ngx_peer_connection_t *pc, void *data, 
    ngx_uint_t state) 
{
    pc->tries = 0;
}

static char *
ngx_http_upstream_consistent_hash(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_upstream_srv_conf_t *uscf;
    ngx_http_script_compile_t sc;
    ngx_str_t *value;
    

    value = cf->args->elts;

    ngx_memzero(&sc, sizeof(ngx_http_script_compile_t));
    
    ngx_http_upstream_consistent_hash_key_vars_lengths = NULL;
    ngx_http_upstream_consistent_hash_key_vars_values = NULL;

    sc.cf = cf;
    sc.source = &value[1];
    sc.lengths = &ngx_http_upstream_consistent_hash_key_vars_lengths;
    sc.values = &ngx_http_upstream_consistent_hash_key_vars_values;
    sc.complete_lengths = 1;
    sc.complete_values = 1;

    if (ngx_http_script_compile(&sc) != NGX_OK) {
        return NGX_CONF_ERROR;
    }

    uscf = ngx_http_conf_get_module_srv_conf(cf, ngx_http_upstream_module);

    uscf->peer.init_upstream = ngx_http_upstream_init_consistent_hash;

    uscf->flags = NGX_HTTP_UPSTREAM_CREATE
                  |NGX_HTTP_UPSTREAM_WEIGHT;

    return NGX_CONF_OK;
}


/// debugging
void 
ngx_http_upstream_consistent_hash_print_buckets (ngx_conf_t *cf, ngx_http_upstream_consistent_hash_buckets *buckets)
{
  ngx_uint_t i;

  printf("print buckets\n");

  for (i = 0; i < MMC_CONSISTENT_BUCKETS; i++)
  {
    printf("%i: name %s point %u\n", (int)i, (char*)buckets->buckets[i]->name.data, (unsigned int)buckets->buckets[i]->point);
  }
}

void 
ngx_http_upstream_consistent_hash_print_continuum (ngx_conf_t *cf, ngx_http_upstream_consistent_hash_continuum *continuum)
{
  ngx_uint_t i;

  printf("print continuum\n");

  for (i = 0; i < continuum->nnodes; i++)
  {
    printf("%i: name %.19s point %u\n", (int)i, (char*)continuum->nodes[i].name.data, (unsigned int)continuum->nodes[i].point);
  }
}
