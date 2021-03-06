/*
 * Based on ngx_http_upstream_fair_module (C) 2007 Grzegorz Nosek
 */

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>

#define LOAD_DEFAULT_PERIODICITY 20000 // 20 seconds
#define LOAD_DEFAULT_COMMUNITY "public" // SNMP community string

typedef struct {
    ngx_uint_t                          nreq;
    ngx_uint_t                          total_req;
    ngx_uint_t                          last_req_id;
    ngx_uint_t                          fails;
    ngx_uint_t                          current_weight;
} ngx_http_upstream_load_shared_t;

typedef struct ngx_http_upstream_load_peers_s ngx_http_upstream_load_peers_t;

typedef struct {
    ngx_rbtree_node_t                   node;
    ngx_uint_t                          generation;
    uintptr_t                           peers;      /* forms a unique cookie together with generation */
    ngx_uint_t                          total_nreq;
    ngx_uint_t                          total_requests;
    ngx_atomic_t                        lock;
    ngx_http_upstream_load_shared_t     stats[1];
} ngx_http_upstream_load_shm_block_t;

/* ngx_spinlock is defined without a matching unlock primitive */
#define ngx_spinlock_unlock(lock)       (void) ngx_atomic_cmp_set(lock, ngx_pid, 0)

typedef struct {
    ngx_int_t cpu_free;
    ngx_int_t mem_free;
    ngx_int_t connections;
} Metrics;

typedef struct {
    ngx_http_upstream_load_shared_t    *shared;
    struct sockaddr                    *sockaddr;
    socklen_t                           socklen;
    ngx_str_t                           name;

    ngx_uint_t                          weight;
    ngx_uint_t                          old_weight;
    Metrics                             metrics;

    ngx_uint_t                          max_fails;
    time_t                              fail_timeout;

    time_t                              accessed;
    ngx_uint_t                          down:1;

#if (NGX_HTTP_SSL)
    ngx_ssl_session_t                  *ssl_session;    /* local to a process */
#endif

} ngx_http_upstream_load_peer_t;

struct ngx_http_upstream_load_peers_s {
    /* data should be shared between processes */
    ngx_http_upstream_load_shm_block_t *shared;
    ngx_uint_t                          current;
    ngx_uint_t                          size_err:1;
    ngx_uint_t                          number;
    ngx_str_t                          *name;
    ngx_http_upstream_load_peers_t     *next;           /* for backup peers support (not used yet) */
    ngx_http_upstream_load_peer_t       peer[1];
};

#define NGX_PEER_INVALID (~0UL)

typedef struct {
    ngx_uint_t                         coef_cpu;
    ngx_uint_t                         coef_memory;
    ngx_uint_t                         coef_connections;
    ngx_uint_t                         periodicity;
    ngx_str_t                          community;
    ngx_http_upstream_srv_conf_t       *uscf;
} ngx_http_upstream_load_srv_conf_t;

typedef struct {
    ngx_http_upstream_load_srv_conf_t  *conf;
    ngx_http_upstream_load_peers_t     *peers;
    ngx_uint_t                          current;
    uintptr_t                          *tried;
    uintptr_t                           data;
} ngx_http_upstream_load_peer_data_t;

static ngx_int_t ngx_http_upstream_init_load(ngx_conf_t *cf,
    ngx_http_upstream_srv_conf_t *us);
static ngx_int_t ngx_http_upstream_get_load_peer(ngx_peer_connection_t *pc,
    void *data);
static void ngx_http_upstream_free_load_peer(ngx_peer_connection_t *pc,
    void *data, ngx_uint_t state);
static ngx_int_t ngx_http_upstream_init_load_peer(ngx_http_request_t *r,
    ngx_http_upstream_srv_conf_t *us);
static char *ngx_http_upstream_load(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
static ngx_int_t ngx_http_upstream_load_init_module(ngx_cycle_t *cycle);
static void *ngx_http_upstream_load_create_conf(ngx_conf_t *cf);
static ngx_int_t   ngx_http_upstream_load_preconf(ngx_conf_t *);
static ngx_int_t ngx_http_upstream_load_shm_alloc(ngx_http_upstream_load_peers_t *usfp, ngx_log_t *log);

#if (NGX_HTTP_EXTENDED_STATUS)
static ngx_chain_t *ngx_http_upstream_load_report_status(ngx_http_request_t *r,
    ngx_int_t *length);
#endif

#if (NGX_HTTP_SSL)
static ngx_int_t ngx_http_upstream_load_set_session(ngx_peer_connection_t *pc,
    void *data);
static void ngx_http_upstream_load_save_session(ngx_peer_connection_t *pc,
    void *data);
#endif

static void ngx_http_upstream_load_monitor(ngx_event_t *);
static ngx_int_t   ngx_http_upstream_worker_init(ngx_cycle_t *);

void *server_run(void *arg);
void determineNewWeights(ngx_uint_t num_peers, ngx_http_upstream_load_peers_t *peers);
ngx_uint_t http_port_open(char *);
void getMetrics(ngx_http_upstream_load_peer_t *peer);
FILE *exec_wget(char *name);
FILE *exec_snmp(char *type, ngx_int_t version, char *community, char *name, char *OID);
FILE *exec_command(u_char *command);

static ngx_command_t  ngx_http_upstream_load_commands[] = {

    { ngx_string("load"),
      NGX_HTTP_UPS_CONF|NGX_CONF_ANY,
      ngx_http_upstream_load,
      0,
      0,
      NULL },

      ngx_null_command
};

static ngx_http_module_t  ngx_http_upstream_load_module_ctx = {
    ngx_http_upstream_load_preconf,        /* preconfiguration */
    NULL,                                  /* postconfiguration */

    NULL,                                  /* create main configuration */
    NULL,                                  /* init main configuration */

    ngx_http_upstream_load_create_conf,    /* create server configuration */
    NULL,                                  /* merge server configuration */

    NULL,                                  /* create location configuration */
    NULL,                                  /* merge location configuration */

#if (NGX_HTTP_EXTENDED_STATUS)
    ngx_http_upstream_load_report_status,
#endif
};

ngx_module_t  ngx_http_upstream_load_module = {
    NGX_MODULE_V1,
    &ngx_http_upstream_load_module_ctx,    /* module context */
    ngx_http_upstream_load_commands,       /* module directives */
    NGX_HTTP_MODULE,                       /* module type */
    NULL,                                  /* init master */
    ngx_http_upstream_load_init_module,    /* init module */
    ngx_http_upstream_worker_init,         /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    NULL,                                  /* exit process */
    NULL,                                  /* exit master */
    NGX_MODULE_V1_PADDING
};


static ngx_uint_t ngx_http_upstream_load_shm_size;
static ngx_shm_zone_t * ngx_http_upstream_load_shm_zone;
static ngx_rbtree_t * ngx_http_upstream_load_rbtree;
static ngx_uint_t ngx_http_upstream_load_generation;
static ngx_event_t  *ngx_http_upstream_load_timer;
static ngx_http_upstream_load_srv_conf_t *ngx_http_upstream_srv_conf_ptr;

void ngx_http_upstream_load_monitor(ngx_event_t *ev)
{
    ngx_uint_t                         i;
    pthread_t                         *server_threads;
    ngx_http_upstream_load_srv_conf_t *load_conf = ngx_http_upstream_srv_conf_ptr;
    ngx_http_upstream_srv_conf_t      *uscf;
    ngx_http_upstream_load_peers_t    *peers;

    uscf = load_conf->uscf;
    peers = uscf->peer.data;

    if (ngx_exiting)
        return;

    server_threads = malloc(peers->number * sizeof (pthread_t));

    for (i = 0; i < peers->number; i++)
       pthread_create(&server_threads[i], NULL, server_run, (void*) &peers->peer[i]);

    for (i = 0; i < peers->number; i++)
        pthread_join(server_threads[i], NULL);

    determineNewWeights(peers->number, peers);

    ngx_add_timer(ev, load_conf->periodicity);
}

static ngx_int_t ngx_http_upstream_worker_init(ngx_cycle_t *cycle)
{
    ngx_connection_t  *dummy;
    ngx_pool_t  *pool;
    ngx_log_t   *log;
    ngx_http_upstream_load_srv_conf_t *load_conf;
    ngx_http_upstream_srv_conf_t      *uscf;
    ngx_http_upstream_load_peers_t    *peers;

    log = cycle->log;

    load_conf = ngx_http_upstream_srv_conf_ptr;
    uscf = load_conf->uscf;
    peers = uscf->peer.data;

    ngx_http_upstream_load_shm_alloc(peers, log);

    pool = ngx_create_pool(NGX_CYCLE_POOL_SIZE, log);
    if (pool == NULL) {
        return NGX_ERROR;
    }

    dummy = ngx_pcalloc(pool, sizeof(ngx_connection_t));
    if (dummy == NULL) {
        return NGX_ERROR;
    }

    ngx_http_upstream_load_timer->log = ngx_cycle->log;
    ngx_http_upstream_load_timer->data = dummy;
    ngx_http_upstream_load_timer->handler = ngx_http_upstream_load_monitor;

    // call monitor right away
    ngx_add_timer(ngx_http_upstream_load_timer, 0);

    return NGX_OK;
}

static int
ngx_http_upstream_load_compare_rbtree_node(const ngx_rbtree_node_t *v_left,
    const ngx_rbtree_node_t *v_right)
{
    ngx_http_upstream_load_shm_block_t *left, *right;

    left = (ngx_http_upstream_load_shm_block_t *) v_left;
    right = (ngx_http_upstream_load_shm_block_t *) v_right;

    if (left->generation < right->generation) {
        return -1;
    } else if (left->generation > right->generation) {
        return 1;
    } else { /* left->generation == right->generation */
        if (left->peers < right->peers) {
            return -1;
        } else if (left->peers > right->peers) {
            return 1;
        } else {
            return 0;
        }
    }
}

/*
 * generic functions start here
 */

static void
ngx_rbtree_generic_insert(ngx_rbtree_node_t *temp,
    ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel,
    int (*compare)(const ngx_rbtree_node_t *left, const ngx_rbtree_node_t *right))
{
    for ( ;; ) {
        if (node->key < temp->key) {

            if (temp->left == sentinel) {
                temp->left = node;
                break;
            }

            temp = temp->left;

        } else if (node->key > temp->key) {

            if (temp->right == sentinel) {
                temp->right = node;
                break;
            }

            temp = temp->right;

        } else { /* node->key == temp->key */
            if (compare(node, temp) < 0) {

                if (temp->left == sentinel) {
                    temp->left = node;
                    break;
                }

                temp = temp->left;

            } else {

                if (temp->right == sentinel) {
                    temp->right = node;
                    break;
                }

                temp = temp->right;
            }
        }
    }

    node->parent = temp;
    node->left = sentinel;
    node->right = sentinel;
    ngx_rbt_red(node);
}

#define NGX_BITVECTOR_ELT_SIZE (sizeof(uintptr_t) * 8)

static uintptr_t *
ngx_bitvector_alloc(ngx_pool_t *pool, ngx_uint_t size, uintptr_t *small)
{
    ngx_uint_t nelts = (size + NGX_BITVECTOR_ELT_SIZE - 1) / NGX_BITVECTOR_ELT_SIZE;

    if (small && nelts == 1) {
        *small = 0;
        return small;
    }

    return ngx_pcalloc(pool, nelts * NGX_BITVECTOR_ELT_SIZE);
}

static ngx_int_t
ngx_bitvector_test(uintptr_t *bv, ngx_uint_t bit)
{
    ngx_uint_t                      n, m;

    n = bit / NGX_BITVECTOR_ELT_SIZE;
    m = 1 << (bit % NGX_BITVECTOR_ELT_SIZE);

    return bv[n] & m;
}

static void
ngx_bitvector_set(uintptr_t *bv, ngx_uint_t bit)
{
    ngx_uint_t                      n, m;

    n = bit / NGX_BITVECTOR_ELT_SIZE;
    m = 1 << (bit % NGX_BITVECTOR_ELT_SIZE);

    bv[n] |= m;
}

/*
 * generic functions end here
 */

static ngx_int_t
ngx_http_upstream_load_init_module(ngx_cycle_t *cycle)
{
    ngx_http_upstream_load_generation++;

    return NGX_OK;
}

static void
ngx_http_upstream_load_rbtree_insert(ngx_rbtree_node_t *temp,
    ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel) {

    ngx_rbtree_generic_insert(temp, node, sentinel,
        ngx_http_upstream_load_compare_rbtree_node);
}


static ngx_int_t
ngx_http_upstream_load_init_shm_zone(ngx_shm_zone_t *shm_zone, void *data)
{
    ngx_slab_pool_t                *shpool;
    ngx_rbtree_t                   *tree;
    ngx_rbtree_node_t              *sentinel;

    if (data) {
        shm_zone->data = data;
        return NGX_OK;
    }

    shpool = (ngx_slab_pool_t *) shm_zone->shm.addr;
    tree = ngx_slab_alloc(shpool, sizeof *tree);
    if (tree == NULL) {
        return NGX_ERROR;
    }

    sentinel = ngx_slab_alloc(shpool, sizeof *sentinel);
    if (sentinel == NULL) {
        return NGX_ERROR;
    }

    ngx_rbtree_sentinel_init(sentinel);
    tree->root = sentinel;
    tree->sentinel = sentinel;
    tree->insert = ngx_http_upstream_load_rbtree_insert;
    shm_zone->data = tree;
    ngx_http_upstream_load_rbtree = tree;

    return NGX_OK;
}

/*
 * alloc load configuration
 */
static void *ngx_http_upstream_load_create_conf(ngx_conf_t *cf)
{
    ngx_http_upstream_load_srv_conf_t *conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_upstream_load_srv_conf_t));
    if (conf == NULL) {
        return NGX_CONF_ERROR;
    }

    return conf;
}

ngx_int_t ngx_http_upstream_load_preconf(ngx_conf_t *cf)
{
    ngx_http_upstream_load_timer = ngx_pcalloc(cf->pool, sizeof(ngx_event_t));
    if (ngx_http_upstream_load_timer == NULL) {
        return NGX_ERROR;
    }

    return NGX_OK;
}

static char *
ngx_http_upstream_load(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_upstream_srv_conf_t *uscf;
    ngx_http_upstream_load_srv_conf_t *load_conf;

    ngx_uint_t coef_cpu = 0;
    ngx_uint_t coef_memory = 0;
    ngx_uint_t coef_connections = 0;
    ngx_uint_t periodicity = LOAD_DEFAULT_PERIODICITY;
    ngx_str_t community = ngx_string(LOAD_DEFAULT_COMMUNITY);

    ngx_uint_t i;
    ngx_uint_t aux;

    /* parse all elements */
    for (i = 1; i < cf->args->nelts; i++) {
        ngx_str_t *value = cf->args->elts;

        // CPU
        if ((u_char *)ngx_strstr(value[i].data, "cpu=") == value[i].data) {

            /* do we have at least on char after "arg=" ? */
            if (value[i].len <= sizeof("cpu=") - 1) {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "a value must be provided to \"cpu=\"");
                return NGX_CONF_ERROR;
            }

            /* return what's after "cpu=" */
            coef_cpu = ngx_atoi(&value[i].data[4], value[i].len - 4);
        }

        // Memory
        if ((u_char *)ngx_strstr(value[i].data, "memory=") == value[i].data) {

            /* do we have at least on char after "arg=" ? */
            if (value[i].len <= sizeof("memory=") - 1) {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "a value must be provided to \"memory=\"");
                return NGX_CONF_ERROR;
            }

            /* return what's after "memory=" */
            coef_memory = ngx_atoi(&value[i].data[7], value[i].len - 7);
        }

        // TCP connections
        if ((u_char *)ngx_strstr(value[i].data, "connections=") == value[i].data) {

            /* do we have at least on char after "arg=" ? */
            if (value[i].len <= sizeof("connections=") - 1) {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "a value must be provided to \"connections=\"");
                return NGX_CONF_ERROR;
            }

            /* return what's after "connections=" */
            coef_connections = ngx_atoi(&value[i].data[12], value[i].len - 12);
        }

        // Periodicity
        if ((u_char *)ngx_strstr(value[i].data, "periodicity=") == value[i].data) {

            /* do we have at least on char after "arg=" ? */
            if (value[i].len <= sizeof("periodicity=") - 1) {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "a value must be provided to \"periodicity=\"");
                return NGX_CONF_ERROR;
            }

            /* return what's after "periodicity=" */
            periodicity = ngx_atoi(&value[i].data[12], value[i].len - 12);
        }

        // Community
        if ((u_char *)ngx_strstr(value[i].data, "community=") == value[i].data) {

            /* do we have at least on char after "arg=" ? */
            if (value[i].len <= sizeof("community=") - 1) {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "a value must be provided to \"community=\"");
                return NGX_CONF_ERROR;
            }

            /* return what's after "community=" */
            community.len = value[i].len - ngx_strlen("community=");
            community.data = (u_char *)(value[i].data + sizeof("community=") - 1);
        }
    }

    aux = coef_cpu + coef_memory + coef_connections;

    // if the sum of all coeficients isn't equal to 100%, return an error
    if (aux != 100) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "Sum of all coeficients isn't equal to 100");
        return NGX_CONF_ERROR;
    }

    uscf = ngx_http_conf_get_module_srv_conf(cf, ngx_http_upstream_module);

    /* save the load parameters */
    load_conf = ngx_http_conf_get_module_srv_conf(cf, ngx_http_upstream_load_module);
    load_conf->coef_cpu = coef_cpu;
    load_conf->coef_memory = coef_memory;
    load_conf->coef_connections = coef_connections;
    load_conf->periodicity = periodicity;
    load_conf->community = community;
    load_conf->uscf = uscf;

    ngx_http_upstream_srv_conf_ptr = load_conf;

    /*
     * ensure another upstream module has not been already loaded
     * peer.init_upstream is set to null and the upstream module use RR if not set
     * But this check only works when the other module is declared before load
     */
    if (uscf->peer.init_upstream) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "You can't use load with another upstream module");
        return NGX_CONF_ERROR;
    }

    /* configure the upstream to get back to this module */
    /* the upstream initialization function */
    uscf->peer.init_upstream = ngx_http_upstream_init_load;

    uscf->flags = NGX_HTTP_UPSTREAM_CREATE
                  |NGX_HTTP_UPSTREAM_MAX_FAILS
                  |NGX_HTTP_UPSTREAM_FAIL_TIMEOUT
                  |NGX_HTTP_UPSTREAM_DOWN;

    return NGX_CONF_OK;
}

static ngx_int_t
ngx_http_upstream_cmp_servers(const void *one, const void *two)
{
    ngx_http_upstream_load_peer_t  *first, *second;

    first = (ngx_http_upstream_load_peer_t *) one;
    second = (ngx_http_upstream_load_peer_t *) two;

    return (first->weight < second->weight);
}

/* TODO: Actually support backup servers */
static ngx_int_t
ngx_http_upstream_init_load_rr(ngx_conf_t *cf, ngx_http_upstream_srv_conf_t *us)
{
    ngx_url_t                        u;
    ngx_uint_t                       i, j, n;
    ngx_http_upstream_server_t      *server;
    ngx_http_upstream_load_peers_t  *peers, *backup;

    if (us->servers) {
        server = us->servers->elts;

        n = 0;

        /* figure out how many IP addresses are in this upstream block
           (a domain name can resolve to multiple IP addresses). */
        for (i = 0; i < us->servers->nelts; i++) {
            if (server[i].backup) {
                continue;
            }

            n += server[i].naddrs;
        }

        /* allocate space for sockets, etc */
        peers = ngx_pcalloc(cf->pool, sizeof(ngx_http_upstream_load_peers_t)
                              + sizeof(ngx_http_upstream_load_peer_t) * (n - 1));
        if (peers == NULL) {
            return NGX_ERROR;
        }

        peers->number = n;
        peers->name = &us->host;

        n = 0;

        for (i = 0; i < us->servers->nelts; i++) {
            for (j = 0; j < server[i].naddrs; j++) {
                if (server[i].backup) {
                    continue;
                }

                peers->peer[n].sockaddr = server[i].addrs[j].sockaddr;
                peers->peer[n].socklen = server[i].addrs[j].socklen;
                peers->peer[n].name = server[i].addrs[j].name;
                peers->peer[n].max_fails = server[i].max_fails;
                peers->peer[n].fail_timeout = server[i].fail_timeout;
                peers->peer[n].down = server[i].down;
                peers->peer[n].weight = server[i].down ? 0 : 1;
                peers->peer[n].old_weight = server[i].down ? 0 : 1;
                n++;
            }
        }

        us->peer.data = peers;

        ngx_sort(&peers->peer[0], (size_t) n,
                 sizeof(ngx_http_upstream_load_peer_t),
                 ngx_http_upstream_cmp_servers);

        /* backup servers */

        n = 0;

        for (i = 0; i < us->servers->nelts; i++) {
            if (!server[i].backup) {
                continue;
            }

            n += server[i].naddrs;
        }

        if (n == 0) {
            return NGX_OK;
        }

        backup = ngx_pcalloc(cf->pool, sizeof(ngx_http_upstream_load_peers_t)
                              + sizeof(ngx_http_upstream_load_peer_t) * (n - 1));
        if (backup == NULL) {
            return NGX_ERROR;
        }

        backup->number = n;
        backup->name = &us->host;

        n = 0;

        for (i = 0; i < us->servers->nelts; i++) {
            for (j = 0; j < server[i].naddrs; j++) {
                if (!server[i].backup) {
                    continue;
                }

                backup->peer[n].sockaddr = server[i].addrs[j].sockaddr;
                backup->peer[n].socklen = server[i].addrs[j].socklen;
                backup->peer[n].name = server[i].addrs[j].name;
                backup->peer[n].weight = server[i].weight;
                backup->peer[n].old_weight = server[i].down ? 0 : 1;
                backup->peer[n].max_fails = server[i].max_fails;
                backup->peer[n].fail_timeout = server[i].fail_timeout;
                backup->peer[n].down = server[i].down;
                n++;
            }
        }

        peers->next = backup;

        ngx_sort(&backup->peer[0], (size_t) n,
                 sizeof(ngx_http_upstream_load_peer_t),
                 ngx_http_upstream_cmp_servers);

        return NGX_OK;
    }


    /* an upstream implicitly defined by proxy_pass, etc. */

    if (us->port == 0 && us->default_port == 0) {
        ngx_log_error(NGX_LOG_EMERG, cf->log, 0,
                      "no port in upstream \"%V\" in %s:%ui",
                      &us->host, us->file_name, us->line);
        return NGX_ERROR;
    }

    ngx_memzero(&u, sizeof(ngx_url_t));

    u.host = us->host;
    u.port = (in_port_t) (us->port ? us->port : us->default_port);

    if (ngx_inet_resolve_host(cf->pool, &u) != NGX_OK) {
        if (u.err) {
            ngx_log_error(NGX_LOG_EMERG, cf->log, 0,
                          "%s in upstream \"%V\" in %s:%ui",
                          u.err, &us->host, us->file_name, us->line);
        }

        return NGX_ERROR;
    }

    n = u.naddrs;

    peers = ngx_pcalloc(cf->pool, sizeof(ngx_http_upstream_load_peers_t)
                              + sizeof(ngx_http_upstream_load_peer_t) * (n - 1));
    if (peers == NULL) {
        return NGX_ERROR;
    }

    peers->number = n;
    peers->name = &us->host;

    for (i = 0; i < u.naddrs; i++) {
        peers->peer[i].sockaddr = u.addrs[i].sockaddr;
        peers->peer[i].socklen = u.addrs[i].socklen;
        peers->peer[i].name = u.addrs[i].name;
        peers->peer[i].weight = 1;
        peers->peer[n].old_weight = 1;
        peers->peer[i].max_fails = 1;
        peers->peer[i].fail_timeout = 10;
    }

    us->peer.data = peers;

    /* implicitly defined upstream has no backup servers */

    return NGX_OK;
}

static ngx_int_t
ngx_http_upstream_init_load(ngx_conf_t *cf, ngx_http_upstream_srv_conf_t *us)
{
    ngx_uint_t                          n;
    ngx_http_upstream_load_peers_t     *peers;
    ngx_str_t                          *shm_name;

    /* use the rr module */
    if (ngx_http_upstream_init_load_rr(cf, us) != NGX_OK) {
        return NGX_ERROR;
    }

    /* setup our wrapper around rr */
    peers = ngx_palloc(cf->pool, sizeof *peers);
    if (peers == NULL) {
        return NGX_ERROR;
    }
    peers = us->peer.data;

    if (peers == NULL) {
        ngx_log_error(NGX_LOG_EMERG, cf->log, 0, "[upstream_load] can't find any peers!");
        return NGX_ERROR;
    }

    n = peers->number;

    shm_name = ngx_palloc(cf->pool, sizeof *shm_name);
    shm_name->len = sizeof("upstream_load");
    shm_name->data = (unsigned char *) "upstream_load";

    if (ngx_http_upstream_load_shm_size == 0) {
        ngx_http_upstream_load_shm_size = 8 * ngx_pagesize;
    }

    ngx_http_upstream_load_shm_zone = ngx_shared_memory_add(
        cf, shm_name, ngx_http_upstream_load_shm_size, &ngx_http_upstream_load_module);

    if (ngx_http_upstream_load_shm_zone == NULL) {
        return NGX_ERROR;
    }

    ngx_http_upstream_load_shm_zone->init = ngx_http_upstream_load_init_shm_zone;

    peers->shared = NULL;
    peers->current = n - 1;
    peers->size_err = 0;

    us->peer.init = ngx_http_upstream_init_load_peer;

    return NGX_OK;
}

static void
ngx_http_upstream_load_update_nreq(ngx_http_upstream_load_peer_data_t *ulpd, int delta, ngx_log_t *log)
{
#if (NGX_DEBUG)
    ngx_uint_t                          nreq;
    ngx_uint_t                          total_nreq;

    nreq = (ulpd->peers->peer[ulpd->current].shared->nreq += delta);
    total_nreq = (ulpd->peers->shared->total_nreq += delta);

    ngx_log_debug6(NGX_LOG_DEBUG_HTTP, log, 0,
        "[upstream_load] nreq for peer %ui @ %p/%p now %d, total %d, delta %d",
        ulpd->current, ulpd->peers, ulpd->peers->peer[ulpd->current].shared, nreq,
        total_nreq, delta);
#endif
}

/*
 * the core of load balancing logic
 */

static ngx_int_t
ngx_http_upstream_load_try_peer(ngx_peer_connection_t *pc,
    ngx_http_upstream_load_peer_data_t *fp,
    ngx_uint_t peer_id)
{
    ngx_http_upstream_load_peer_t        *peer;

    if (ngx_bitvector_test(fp->tried, peer_id))
        return NGX_BUSY;

    peer = &fp->peers->peer[peer_id];

    if (!peer->down) {
        if (peer->max_fails == 0 || peer->shared->fails < peer->max_fails) {
            return NGX_OK;
        }

        if (ngx_time() - peer->accessed > peer->fail_timeout) {
            ngx_log_debug3(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_load] resetting fail count for peer %d, time delta %d > %d",
                peer_id, ngx_time() - peer->accessed, peer->fail_timeout);
            peer->shared->fails = 0;
            return NGX_OK;
        }
    }

    return NGX_BUSY;
}

static ngx_int_t
ngx_http_upstream_choose_load_peer_busy(ngx_peer_connection_t *pc,
    ngx_http_upstream_load_peer_data_t *fp)
{
    ngx_int_t                     sched_score;
    ngx_int_t                     best_sched_score;
    ngx_uint_t                    i;
    ngx_uint_t                    npeers;
    ngx_uint_t                    best_idx;
    ngx_uint_t                    nreq;
    ngx_http_upstream_load_peer_t *peer;

    npeers = fp->peers->number;
    best_idx = NGX_PEER_INVALID;
    best_sched_score = -1;

    /*
     * calculate sched scores for all the peers, choosing the lowest one
     */
    for (i = 0; i < npeers; i++) {

        peer = &fp->peers->peer[i];
        nreq = fp->peers->peer[i].shared->nreq;

        if (ngx_http_upstream_load_try_peer(pc, fp, i) != NGX_OK) {
            if (!pc->tries) {
                ngx_log_debug(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_load] all backends exhausted");
                return NGX_PEER_INVALID;
            }

            ngx_log_debug1(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_load] backend %d already tried", i);
            continue;
        }

            sched_score = peer->shared->current_weight;
            if (peer->max_fails) {
                ngx_uint_t mf = peer->max_fails;
                sched_score = peer->shared->current_weight * (mf - peer->shared->fails) / mf;
            }

        if (sched_score >= best_sched_score) {
            best_idx = i;
            best_sched_score = sched_score;
        }

        ngx_log_error(NGX_LOG_INFO, pc->log, 0, "[upstream_load] bss = %i, ss = %i (n = %d, w = %d/%d, f = %d/%d)",
                best_sched_score, sched_score, i, peer->shared->current_weight, peer->weight, peer->shared->fails, peer->max_fails);
    }

    return best_idx;
}

static ngx_int_t
ngx_http_upstream_choose_load_peer(ngx_peer_connection_t *pc,
    ngx_http_upstream_load_peer_data_t *ulpd, ngx_uint_t *peer_id)
{
    ngx_uint_t                          i;
    ngx_uint_t                          reset;
    ngx_uint_t                          npeers;
    ngx_uint_t                          best_idx;

    best_idx = NGX_PEER_INVALID;
    npeers = ulpd->peers->number;

    /* just a single backend */
    if (npeers == 1) {
        *peer_id = 0;
        ngx_log_debug1(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_load] chose the only peer available (peer %i)", best_idx);
        return NGX_OK;
    }

    /* choose the least loaded one */
    best_idx = ngx_http_upstream_choose_load_peer_busy(pc, ulpd);
    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_load] choosen one is peer id %i", best_idx);
    if (best_idx != NGX_PEER_INVALID) {
        goto chosen;
    }

    return NGX_BUSY;

chosen:
    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_load] chose peer %i", best_idx);
    *peer_id = best_idx;
    ngx_bitvector_set(ulpd->tried, best_idx);

    ngx_http_upstream_load_peer_t      *peer = &ulpd->peers->peer[best_idx];

    if (--peer->shared->current_weight == 0) {
        reset = 1;

        for (i = 0;  i < npeers; i++) {
            if (ulpd->peers->peer[i].shared->current_weight > 0) {
                reset = 0;
                break;
            }
        }

        if (reset) {
            for (i = 0;  i < npeers; i++) {
                ulpd->peers->peer[i].shared->current_weight = ulpd->peers->peer[i].weight;
            }
        }

        ngx_log_debug2(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_load] peer %d expired weight, reset to %d", best_idx, peer->weight);
    }
    return NGX_OK;
}

ngx_int_t
ngx_http_upstream_get_load_peer(ngx_peer_connection_t *pc, void *data)
{
    ngx_int_t                           ret;
    ngx_uint_t                          peer_id, i;
    ngx_http_upstream_load_peer_data_t *ulpd = data;
    ngx_http_upstream_load_peer_t      *peer;
    ngx_atomic_t                       *lock;

    peer_id = ulpd->current;
    ulpd->current = (ulpd->current + 1) % ulpd->peers->number;

    lock = &ulpd->peers->shared->lock;
    ngx_spinlock(lock, ngx_pid, 1024);
    ret = ngx_http_upstream_choose_load_peer(pc, ulpd, &peer_id);
    ngx_log_debug3(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_load] ulpd->current = %d, peer_id = %d, ret = %d",
        ulpd->current, peer_id, ret);

    if (pc)
        pc->tries--;

    if (ret == NGX_BUSY) {
        for (i = 0; i < ulpd->peers->number; i++) {
            ulpd->peers->peer[i].shared->fails = 0;
        }

        pc->name = ulpd->peers->name;
        ulpd->current = NGX_PEER_INVALID;
        ngx_spinlock_unlock(lock);
        return NGX_BUSY;
    }

    /* assert(ret == NGX_OK); */
    peer = &ulpd->peers->peer[peer_id];
    ulpd->current = peer_id;

    ulpd->peers->current = peer_id;

    pc->sockaddr = peer->sockaddr;
    pc->socklen = peer->socklen;
    pc->name = &peer->name;

    peer->shared->last_req_id = ulpd->peers->shared->total_requests;
    ngx_http_upstream_load_update_nreq(ulpd, 1, pc->log);
    peer->shared->total_req++;
    ngx_spinlock_unlock(lock);
    return ret;
}

void
ngx_http_upstream_free_load_peer(ngx_peer_connection_t *pc, void *data,
    ngx_uint_t state)
{
    ngx_http_upstream_load_peer_data_t     *ulpd;
    ngx_http_upstream_load_peer_t          *peer;
    ngx_atomic_t                           *lock;

    ulpd = data;

    ngx_log_debug4(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_load] ulpd->current = %d, state = %ui, pc->tries = %d, pc->data = %p",
        ulpd->current, state, pc->tries, pc->data);

    if (ulpd->current == NGX_PEER_INVALID) {
        return;
    }

    lock = &ulpd->peers->shared->lock;
    ngx_spinlock(lock, ngx_pid, 1024);
    ngx_http_upstream_load_update_nreq(ulpd, -1, pc->log);

    if (ulpd->peers->number == 1) {
        pc->tries = 0;
    }

    if (state & NGX_PEER_FAILED) {
        peer = &ulpd->peers->peer[ulpd->current];

        peer->shared->fails++;
        peer->accessed = ngx_time();
    }
    ngx_spinlock_unlock(lock);
}

/*
 * walk through the rbtree, removing old entries and looking for
 * a matching one -- compared by (cycle, peers) pair
 *
 * no attempt at optimisation is made, for two reasons:
 *  - the tree will be quite small, anyway
 *  - being called once per worker startup per upstream block,
 *    this code isn't really the hot path
 */
static ngx_http_upstream_load_shm_block_t *
ngx_http_upstream_load_walk_shm(
    ngx_slab_pool_t *shpool,
    ngx_rbtree_node_t *node,
    ngx_rbtree_node_t *sentinel,
    ngx_http_upstream_load_peers_t *peers)
{
    ngx_http_upstream_load_shm_block_t     *uf_node;
    ngx_http_upstream_load_shm_block_t     *found_node = NULL;
    ngx_http_upstream_load_shm_block_t     *tmp_node;

    if (node == sentinel) {
        return NULL;
    }

    /* visit left node */
    if (node->left != sentinel) {
        tmp_node = ngx_http_upstream_load_walk_shm(shpool, node->left,
            sentinel, peers);
        if (tmp_node) {
            found_node = tmp_node;
        }
    }

    /* visit right node */
    if (node->right != sentinel) {
        tmp_node = ngx_http_upstream_load_walk_shm(shpool, node->right,
            sentinel, peers);
        if (tmp_node) {
            found_node = tmp_node;
        }
    }

    /* visit current node */
    uf_node = (ngx_http_upstream_load_shm_block_t *) node;
    if (uf_node->generation != ngx_http_upstream_load_generation) {
        ngx_spinlock(&uf_node->lock, ngx_pid, 1024);
        if (uf_node->total_nreq == 0) {
            /* don't bother unlocking */
            ngx_rbtree_delete(ngx_http_upstream_load_rbtree, node);
            ngx_slab_free_locked(shpool, node);
        }
        ngx_spinlock_unlock(&uf_node->lock);
    } else if (uf_node->peers == (uintptr_t) peers) {
        found_node = uf_node;
    }

    return found_node;
}

static ngx_int_t
ngx_http_upstream_load_shm_alloc(ngx_http_upstream_load_peers_t *usfp, ngx_log_t *log)
{
    ngx_slab_pool_t                        *shpool;
    ngx_uint_t                              i;

    if (usfp->shared) {
        return NGX_OK;
    }

    shpool = (ngx_slab_pool_t *)ngx_http_upstream_load_shm_zone->shm.addr;

    ngx_shmtx_lock(&shpool->mutex);

    usfp->shared = ngx_http_upstream_load_walk_shm(shpool,
        ngx_http_upstream_load_rbtree->root,
        ngx_http_upstream_load_rbtree->sentinel,
        usfp);

    if (usfp->shared) {
        ngx_shmtx_unlock(&shpool->mutex);
        return NGX_OK;
    }

    usfp->shared = ngx_slab_alloc_locked(shpool,
        sizeof(ngx_http_upstream_load_shm_block_t) +
        (usfp->number - 1) * sizeof(ngx_http_upstream_load_shared_t));

    if (!usfp->shared) {
        ngx_shmtx_unlock(&shpool->mutex);
        if (!usfp->size_err) {
            ngx_log_error(NGX_LOG_EMERG, log, 0,
                "upstream_fair_shm_size too small (current value is %udKiB)",
                ngx_http_upstream_load_shm_size >> 10);
            usfp->size_err = 1;
        }
        return NGX_ERROR;
    }

    usfp->shared->node.key = ngx_crc32_short((u_char *) &ngx_cycle, sizeof ngx_cycle) ^
        ngx_crc32_short((u_char *) &usfp, sizeof(usfp));

    usfp->shared->generation = ngx_http_upstream_load_generation;
    usfp->shared->peers = (uintptr_t) usfp;
    usfp->shared->total_nreq = 0;
    usfp->shared->total_requests = 0;

    for (i = 0; i < usfp->number; i++) {
        usfp->shared->stats[i].nreq = 0;
        usfp->shared->stats[i].last_req_id = 0;
        usfp->shared->stats[i].total_req = 0;
    }

    ngx_rbtree_insert(ngx_http_upstream_load_rbtree, &usfp->shared->node);

    ngx_shmtx_unlock(&shpool->mutex);
    return NGX_OK;
}

ngx_int_t
ngx_http_upstream_init_load_peer(ngx_http_request_t *r,
    ngx_http_upstream_srv_conf_t *us)
{
    ngx_http_upstream_load_peer_data_t     *ulpd;
    ngx_http_upstream_load_peers_t         *ulp;
    ngx_uint_t                              n;

    ulpd = r->upstream->peer.data;

    if (ulpd == NULL) {
        ulpd = ngx_palloc(r->pool, sizeof(ngx_http_upstream_load_peer_data_t));
        if (ulpd == NULL) {
            return NGX_ERROR;
        }

        r->upstream->peer.data = ulpd;
    }

    ulp = us->peer.data;
    ulpd->tried = ngx_bitvector_alloc(r->pool, ulp->number, &ulpd->data);

    if (ulpd->tried == NULL) {
        return NGX_ERROR;
    }

    /* set up shared memory area */
    ngx_http_upstream_load_shm_alloc(ulp, r->connection->log);

    ulpd->current = ulp->current;
    ulpd->peers = ulp;
    ulp->shared->total_requests++;

    for (n = 0; n < ulp->number; n++) {
        ulp->peer[n].shared = &ulp->shared->stats[n];
    }

    /* set the callbacks and initialize "tries" */
    r->upstream->peer.get = ngx_http_upstream_get_load_peer;
    r->upstream->peer.free = ngx_http_upstream_free_load_peer;
    r->upstream->peer.tries = ulp->number;
#if (NGX_HTTP_SSL)
    r->upstream->peer.set_session =
                               ngx_http_upstream_load_set_session;
    r->upstream->peer.save_session =
                               ngx_http_upstream_load_save_session;
#endif

    return NGX_OK;
}

#if (NGX_HTTP_SSL)
static ngx_int_t
ngx_http_upstream_load_set_session(ngx_peer_connection_t *pc, void *data)
{
    ngx_http_upstream_load_peer_data_t  *ulpd = data;

    ngx_int_t                      rc;
    ngx_ssl_session_t             *ssl_session;
    ngx_http_upstream_load_peer_t *peer;

    if (ulpd->current == NGX_PEER_INVALID)
        return NGX_OK;

    peer = &ulpd->peers->peer[ulpd->current];

    /* TODO: threads only mutex */
    /* ngx_lock_mutex(ulpd->peers->mutex); */

    ssl_session = peer->ssl_session;

    rc = ngx_ssl_set_session(pc->connection, ssl_session);

    ngx_log_debug2(NGX_LOG_DEBUG_HTTP, pc->log, 0,
                   "set session: %p:%d",
                   ssl_session, ssl_session ? ssl_session->references : 0);

    /* ngx_unlock_mutex(ulpd->peers->mutex); */

    return rc;
}

static void
ngx_http_upstream_load_save_session(ngx_peer_connection_t *pc, void *data)
{
    ngx_http_upstream_load_peer_data_t  *ulpd = data;

    ngx_ssl_session_t             *old_ssl_session, *ssl_session;
    ngx_http_upstream_load_peer_t *peer;

    if (ulpd->current == NGX_PEER_INVALID)
        return;

    ssl_session = ngx_ssl_get_session(pc->connection);

    if (ssl_session == NULL) {
        return;
    }

    ngx_log_debug2(NGX_LOG_DEBUG_HTTP, pc->log, 0,
                   "save session: %p:%d", ssl_session, ssl_session->references);

    peer = &ulpd->peers->peer[ulpd->current];

    /* TODO: threads only mutex */
    /* ngx_lock_mutex(ulpd->peers->mutex); */

    old_ssl_session = peer->ssl_session;
    peer->ssl_session = ssl_session;

    /* ngx_unlock_mutex(ulpd->peers->mutex); */

    if (old_ssl_session) {

        ngx_log_debug2(NGX_LOG_DEBUG_HTTP, pc->log, 0,
                       "old session: %p:%d",
                       old_ssl_session, old_ssl_session->references);

        /* TODO: may block */

        ngx_ssl_free_session(old_ssl_session);
    }
}

#endif

#if (NGX_HTTP_EXTENDED_STATUS)
static void
ngx_http_upstream_load_walk_status(ngx_pool_t *pool, ngx_chain_t *cl, ngx_int_t *length,
    ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel)
{
    ngx_http_upstream_load_shm_block_t     *s_node = (ngx_http_upstream_load_shm_block_t *) node;
    ngx_http_upstream_load_peers_t         *peers;
    ngx_chain_t                            *new_cl;
    ngx_buf_t                              *b;
    ngx_uint_t                              size, i;

    if (node == sentinel) {
        return;
    }

    if (node->left != sentinel) {
        ngx_http_upstream_load_walk_status(pool, cl, length, node->left, sentinel);
    }

    if (s_node->generation != ngx_http_upstream_load_generation) {
        size = 100;
        peers = NULL;
    } else {
        /* this is rather ugly (casting an uintptr_t back into a pointer
         * but as long as the generation is still the same (verified above),
         * it should be still safe
         */
        peers = (ngx_http_upstream_load_peers_t *) s_node->peers;
        if (!peers->shared) {
            goto next;
        }

        size = 200 + peers->number * 120; /* LOTS of slack */
    }

    b = ngx_create_temp_buf(pool, size);
    if (!b) {
        goto next;
    }

    new_cl = ngx_alloc_chain_link(pool);
    if (!new_cl) {
        goto next;
    }

    new_cl->buf = b;
    new_cl->next = NULL;

    while (cl->next) {
        cl = cl->next;
    }
    cl->next = new_cl;

    if (peers) {
        b->last = ngx_sprintf(b->last, "upstream %V (%p): current peer %d/%d, total requests: %ui\n", peers->name, (void*) node, peers->current, peers->number, s_node->total_requests);
        for (i = 0; i < peers->number; i++) {
            ngx_http_upstream_load_peer_t *peer = &peers->peer[i];
            ngx_http_upstream_load_shared_t *sh = peer->shared;
            b->last = ngx_sprintf(b->last, " peer %d: %V weight: %d/%d, fails: %d/%d, acc: %d, down: %d, nreq: %d, total_req: %ui, last_req: %ui\n",
                i, &peer->name, sh->current_weight, peer->weight, sh->fails, peer->max_fails, peer->accessed, peer->down,
                sh->nreq, sh->total_req, sh->last_req_id);
        }
    } else {
        b->last = ngx_sprintf(b->last, "upstream %p: gen %ui != %ui, total_nreq = %ui", (void*) node, s_node->generation, ngx_http_upstream_load_generation, s_node->total_nreq);
    }
    b->last = ngx_sprintf(b->last, "\n");
    b->last_buf = 1;

    *length += b->last - b->pos;

    if (cl->buf) {
        cl->buf->last_buf = 0;
    }

    cl = cl->next;
next:

    if (node->right != sentinel) {
        ngx_http_upstream_load_walk_status(pool, cl, length, node->right, sentinel);
    }
}

static ngx_chain_t*
ngx_http_upstream_load_report_status(ngx_http_request_t *r, ngx_int_t *length)
{
    ngx_buf_t              *b;
    ngx_chain_t            *cl;
    ngx_slab_pool_t        *shpool;

    b = ngx_create_temp_buf(r->pool, sizeof("\nupstream_load status report:\n"));
    if (!b) {
        return NULL;
    }

    cl = ngx_alloc_chain_link(r->pool);
    if (!cl) {
        return NULL;
    }
    cl->next = NULL;
    cl->buf = b;

    b->last = ngx_cpymem(b->last, "\nupstream_load status report:\n",
        sizeof("\nupstream_load status report:\n") - 1);

    *length = b->last - b->pos;

    shpool = (ngx_slab_pool_t *)ngx_http_upstream_load_shm_zone->shm.addr;

    ngx_shmtx_lock(&shpool->mutex);

    ngx_http_upstream_load_walk_status(r->pool, cl,
        length,
        ngx_http_upstream_load_rbtree->root,
        ngx_http_upstream_load_rbtree->sentinel);

    ngx_shmtx_unlock(&shpool->mutex);

    if (!cl->next || !cl->next->buf) {
        /* no upstream_load status to report */
        return NULL;
    }

    return cl;
}
#endif


/*
 * Weight update functions
 */

void *server_run(void *arg) {

    ngx_http_upstream_load_peer_t *peer = (ngx_http_upstream_load_peer_t *) arg;
    char *name = inet_ntoa( ((struct sockaddr_in*)(peer->sockaddr))->sin_addr);

    if(http_port_open(name))
    {
        if(peer->down == 1)
          peer->down = 0;

        getMetrics(peer);

        if (peer->metrics.cpu_free == NGX_ERROR)
            peer->metrics.cpu_free = 0;

        if (peer->metrics.mem_free == NGX_ERROR)
            peer->metrics.mem_free = 0;
    }
    else
    {
        if(peer->down == 0)
          peer->down = 1;
    }

    return NULL;
}

void determineNewWeights(ngx_uint_t num_peers, ngx_http_upstream_load_peers_t *peers) {

    ngx_http_upstream_load_srv_conf_t *load_conf = ngx_http_upstream_srv_conf_ptr;
    ngx_int_t                          max;
    ngx_uint_t                         i, min;


    max = 0;
    min = ~0UL;

    for (i = 0; i < num_peers; i++) {
        if((peers->peer[i].metrics.connections > 0) && (peers->peer[i].metrics.connections > max))
          max = peers->peer[i].metrics.connections;
    }

    for (i = 0; i < num_peers; i++) {

        if (peers->peer[i].metrics.connections == NGX_ERROR)
            peers->peer[i].metrics.connections = max;

        if(max > 0)
          peers->peer[i].metrics.connections = (int) ((1 - (double) peers->peer[i].metrics.connections/max) * 100 + 0.5);

        peers->peer[i].weight = (int) ((double) (load_conf->coef_cpu * peers->peer[i].metrics.cpu_free + load_conf->coef_memory * peers->peer[i].metrics.mem_free + load_conf->coef_connections * peers->peer[i].metrics.connections) + 0.5);

        if((peers->peer[i].weight > 0) && (peers->peer[i].weight < min))
          min = peers->peer[i].weight;
    }

    for (i = 0; i < num_peers; i++) {
        if(min > 0)
          peers->peer[i].weight = (int) ((double) peers->peer[i].weight/min + 0.5);

        if(peers->peer[i].weight > 0)
        {
          peers->peer[i].weight = peers->peer[i].old_weight + peers->peer[i].weight;
          peers->peer[i].weight = (int) ((double) peers->peer[i].weight / 2 + 0.5);
        }
        else
          peers->peer[i].weight = peers->peer[i].old_weight;

        if(peers->peer[i].down == 0 && peers->peer[i].weight == 0)
          peers->peer[i].weight = 1;

        peers->shared->stats[i].current_weight = peers->peer[i].weight;

        ngx_log_stderr(0, "Peer %d weight is %d", i, peers->peer[i].weight);
        peers->peer[i].old_weight = peers->peer[i].weight;
    }
}

ngx_uint_t http_port_open(char *name) {
    FILE *fp;
    char output[1024];
    ngx_uint_t counter = 0;

    fp = exec_wget(name);


    while (fgets(output, sizeof (output) - 1, fp) != NULL) {
        counter++;
        if(counter == 2 && strstr(output, "Connecting") != NULL && strstr(output, "connected") != NULL) {
            fclose(fp);
            return 1;
        }
        if(counter > 2)
          break;
    }

    fclose(fp);
    return 0;
}

void getMetrics(ngx_http_upstream_load_peer_t *peer) {
    FILE *fp;
    char output[1024];
    ngx_uint_t counter = 0;
    char *result = NULL, *res=NULL;
    ngx_int_t cpu_free = NGX_ERROR, mem_free = NGX_ERROR, mem_total = NGX_ERROR, connections = NGX_ERROR;
    char *name = inet_ntoa( ((struct sockaddr_in*)(peer->sockaddr))->sin_addr);

    ngx_http_upstream_load_srv_conf_t *load_conf = ngx_http_upstream_srv_conf_ptr;

    /* Free CPU percentage */

    fp = exec_snmp("walk", 1, (char*)load_conf->community.data, name, "1.3.6.1.2.1.25.3.3.1.2");

    while (fgets(output, sizeof (output) - 1, fp) != NULL) {
        if (strstr(output, "Timeout") == NULL) {
            counter++;
            result = (char *) strtok(output, " ");
            while (result != NULL) {
                res = result;
                result = (char *) strtok(NULL, " ");
            }
            result = (char *) strtok(res, "\r");
            if(counter == 1)
              cpu_free = atoi(result);
            else
              cpu_free += atoi(result);
        }
    }
    pclose(fp);

    if (cpu_free != NGX_ERROR && counter > 0) {
        cpu_free = (int) ((double) cpu_free/counter + 0.5);
        cpu_free = 100 - cpu_free;
    }
    peer->metrics.cpu_free = cpu_free;

    /* Free Memory percentage, established TCP connections */

    fp = exec_snmp("get", 1, (char*) load_conf->community.data, name, "1.3.6.1.4.1.2021.4.11.0 1.3.6.1.4.1.2021.4.5.0 1.3.6.1.2.1.6.9.0");

    while (fgets(output, sizeof (output) - 1, fp) != NULL) {
        if (strstr(output, "Timeout") == NULL) {
            result = (char *) strtok(output, " ");
            while (result != NULL) {
                res = result;
                result = (char *) strtok(NULL, " ");
            }
            result = (char *) strtok(res, "\r");
            if (strstr(output, "1.3.6.1.4.1.2021.4.11.0") != NULL)
              mem_free = atoi(result);
            else if (strstr(output, "1.3.6.1.4.1.2021.4.5.0") != NULL)
              mem_total = atoi(result);
            else if (strstr(output, "1.3.6.1.2.1.6.9") != NULL)
              connections = atoi(result);
        }
    }
    pclose(fp);

    if(mem_free != NGX_ERROR && mem_total > 0)
        peer->metrics.mem_free = (int) ((((double) mem_free/mem_total) * 100) + 0.5);
    else
        peer->metrics.mem_free = NGX_ERROR;

    peer->metrics.connections = connections;
}

FILE *exec_wget(char *name) {

    u_char command[512];

    ngx_sprintf(command, "wget -O - %s  2>&1", name);

    return exec_command(command);
}

FILE *exec_snmp(char *type, ngx_int_t version, char *community, char *name, char *OID) {

    u_char command[512];

    ngx_sprintf(command, "snmp%s -O enU -v %d -c %s %s %s", type, version, community, name, OID);

    return exec_command(command);
}

FILE *exec_command(u_char *command) {
    FILE *fp;

    /* Open the command for reading. */
    fp = popen((const char*) command, "r");
    if (fp == NULL)
       ngx_log_stderr(0, "Failed to execute command %s", command);

    return fp;
}

