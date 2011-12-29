/*
 * Based on ngx_http_upstream_fair_module (C) 2007 Grzegorz Nosek
 */

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

typedef struct ngx_http_upstream_fair_peers_s ngx_http_upstream_fair_peers_t;

/* ngx_spinlock is defined without a matching unlock primitive */
#define ngx_spinlock_unlock(lock)       (void) ngx_atomic_cmp_set(lock, ngx_pid, 0)

typedef struct {

    
} ngx_http_upstream_load_peer_t;

struct ngx_http_upstream_fair_peers_s {

    
};

typedef struct {

    
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

static ngx_command_t  ngx_http_upstream_load_commands[] = {

    { ngx_string("load"),
      NGX_HTTP_UPS_CONF|NGX_CONF_NOARGS,
      ngx_http_upstream_load,
      0,
      0,
      NULL },

      ngx_null_command
};

static ngx_http_module_t  ngx_http_upstream_load_module_ctx = {
    NULL,                                  /* preconfiguration */
    NULL,                                  /* postconfiguration */

    NULL,                                  /* create main configuration */
    NULL,                                  /* init main configuration */

    NULL,                                  /* create server configuration */
    NULL,                                  /* merge server configuration */

    NULL,                                  /* create location configuration */
    NULL,                                  /* merge location configuration */

#if (NGX_HTTP_EXTENDED_STATUS)
    ngx_http_upstream_fair_report_status,
#endif
};

ngx_module_t  ngx_http_upstream_load_module = {
    NGX_MODULE_V1,
    &ngx_http_upstream_load_module_ctx, /* module context */
    ngx_http_upstream_load_commands,    /* module directives */
    NGX_HTTP_MODULE,                       /* module type */
    NULL,                                  /* init master */
    ngx_http_upstream_load_init_module,    /* init module */
    NULL,                                  /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    NULL,                                  /* exit process */
    NULL,                                  /* exit master */
    NGX_MODULE_V1_PADDING
};

static ngx_uint_t ngx_http_upstream_load_generation;

static ngx_int_t
ngx_http_upstream_load_init_module(ngx_cycle_t *cycle)
{
    ngx_http_upstream_load_generation++;
    return NGX_OK;
}

static char *
ngx_http_upstream_load(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_upstream_srv_conf_t  *uscf;

    uscf = ngx_http_conf_get_module_srv_conf(cf, ngx_http_upstream_module);

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
    ngx_url_t                      u;
    ngx_uint_t                     i, j, n;
    ngx_http_upstream_server_t    *server;
    ngx_http_upstream_load_peers_t  *peers, *backup;

    if (us->servers) {
        server = us->servers->elts;

        n = 0;

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
                peers->peer[n].weight = server[i].down ? 0 : server[i].weight;
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
                backup->peer[n].max_fails = server[i].max_fails;
                backup->peer[n].fail_timeout = server[i].fail_timeout;
                backup->peer[n].down = server[i].down;
                n++;
            }
        }

        peers->next = backup;

        ngx_sort(&backup->peer[0], (size_t) n,
                 sizeof(ngx_http_upstream_fair_peer_t),
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

    peers = ngx_pcalloc(cf->pool, sizeof(ngx_http_upstream_fair_peers_t)
                              + sizeof(ngx_http_upstream_fair_peer_t) * (n - 1));
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
    ngx_http_upstream_fair_peers_t     *peers;

    /* do the dirty work using rr module */
    if (ngx_http_upstream_init_load_rr(cf, us) != NGX_OK) {
        return NGX_ERROR;
    }

    /* setup our wrapper around rr */
    peers = ngx_palloc(cf->pool, sizeof *peers);
    if (peers == NULL) {
        return NGX_ERROR;
    }
    peers = us->peer.data;
    n = peers->number;

    peers->shared = NULL;
    peers->current = n - 1;
    peers->size_err = 0;

    us->peer.init = ngx_http_upstream_init_load_peer;

    return NGX_OK;
}

static void
ngx_http_upstream_fair_update_nreq(ngx_http_upstream_load_peer_data_t *ulpd, int delta, ngx_log_t *log)
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

static ngx_int_t
ngx_http_upstream_choose_load_peer(ngx_peer_connection_t *pc,
    ngx_http_upstream_load_peer_data_t *fp, ngx_uint_t *peer_id)
{

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
    if (!ulpd->peers->no_rr) {
        ulpd->peers->current = peer_id;
    }
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
    ngx_http_upstream_load_peer_data_t     *ulpd = data;
    ngx_http_upstream_load_peer_t          *peer;
    ngx_atomic_t                           *lock;

    ngx_log_debug4(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_load] ulpd->current = %d, state = %ui, pc->tries = %d, pc->data = %p",
        ulpd->current, state, pc->tries, pc->data);

    if (ulpd->current == NGX_PEER_INVALID) {
        return;
    }

    lock = &ulpd->peers->shared->lock;
    ngx_spinlock(lock, ngx_pid, 1024);
    if (!ngx_bitvector_test(ulpd->done, ulpd->current)) {
        ngx_bitvector_set(ulpd->done, ulpd->current);
        ngx_http_upstream_fair_update_nreq(ulpd, -1, pc->log);
    }

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
    ulpd->done = ngx_bitvector_alloc(r->pool, ulp->number, &ulpd->data2);

    if (ulpd->tried == NULL || ulpd->done == NULL) {
        return NGX_ERROR;
    }

    /* set up shared memory area */
    ngx_http_upstream_fair_shm_alloc(ulp, r->connection->log);

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
                               ngx_http_upstream_fair_set_session;
    r->upstream->peer.save_session =
                               ngx_http_upstream_fair_save_session;
#endif

    return NGX_OK;
}