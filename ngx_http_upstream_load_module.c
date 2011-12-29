/*
 * Based on nginx source (C) Igor Sysoev
 */

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

static ngx_command_t  ngx_http_upstream_load_commands[] = {

    { ngx_string("load"),
      NGX_HTTP_UPS_CONF|NGX_CONF_ANY,
      ngx_http_upstream_load,
      0,
      0,
      NULL },

      ngx_null_command
};