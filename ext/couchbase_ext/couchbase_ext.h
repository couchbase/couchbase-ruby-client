/* vim: ft=c et ts=8 sts=4 sw=4 cino=
 *
 *   Copyright 2011, 2012 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef COUCHBASE_EXT_H
#define COUCHBASE_EXT_H

#include <ruby.h>
#ifndef RUBY_ST_H
#include <st.h>
#endif

#include "couchbase_config.h"
#ifndef HAVE_GETHRTIME
typedef uint64_t hrtime_t;
extern hrtime_t gethrtime(void);
#endif

#include <stdint.h>
#include <libcouchbase/couchbase.h>

#ifdef HAVE_RUBY_ENCODING_H
#include "ruby/encoding.h"
#define STR_NEW(ptr, len) rb_external_str_new((ptr), (len))
#define STR_NEW_CSTR(str) rb_external_str_new_cstr((str))
#else
#define STR_NEW(ptr, len) rb_str_new((ptr), (len))
#define STR_NEW_CSTR(str) rb_str_new2((str))
#endif

#ifdef HAVE_STDARG_PROTOTYPES
#include <stdarg.h>
#define va_init_list(a,b) va_start(a,b)
#else
#include <varargs.h>
#define va_init_list(a,b) va_start(a)
#endif

#define debug_object(OBJ) \
    rb_funcall(rb_stderr, rb_intern("print"), 1, rb_funcall(OBJ, rb_intern("object_id"), 0)); \
    rb_funcall(rb_stderr, rb_intern("print"), 1, STR_NEW_CSTR(" ")); \
    rb_funcall(rb_stderr, rb_intern("print"), 1, rb_funcall(OBJ, rb_intern("class"), 0)); \
    rb_funcall(rb_stderr, rb_intern("print"), 1, STR_NEW_CSTR(" ")); \
    rb_funcall(rb_stderr, rb_intern("puts"), 1, rb_funcall(OBJ, rb_intern("inspect"), 0));

#define FMT_MASK        0x3
#define FMT_DOCUMENT    0x0
#define FMT_MARSHAL     0x1
#define FMT_PLAIN       0x2

#define PACKET_HEADER_SIZE 24
/* Structs */
struct bucket_st
{
    lcb_t handle;
    struct lcb_io_opt_st *io;
    uint16_t port;
    char *authority;
    char *hostname;
    char *pool;
    char *bucket;
    char *username;
    char *password;
    int async;
    int quiet;
    VALUE default_format;    /* should update +default_flags+ on change */
    uint32_t default_flags;
    time_t default_ttl;
    time_t default_observe_timeout;
    uint32_t timeout;
    size_t threshold;       /* the number of bytes to trigger event loop, zero if don't care */
    size_t nbytes;          /* the number of bytes scheduled to be sent */
    VALUE exception;        /* error delivered by error_callback */
    VALUE on_error_proc;    /* is using to deliver errors in async mode */
    VALUE environment;      /* sym_development or sym_production */
    char *key_prefix;
    VALUE key_prefix_val;
    char *node_list;
    VALUE object_space;
    VALUE self;             /* the pointer to bucket representation in ruby land */
};

struct http_request_st;
struct context_st
{
    struct bucket_st* bucket;
    int extended;
    VALUE proc;
    void *rv;
    VALUE exception;
    VALUE observe_options;
    VALUE force_format;
    VALUE operation;
    VALUE headers_val;
    int headers_built;
    struct http_request_st *request;
    int quiet;
    int arith;           /* incr: +1, decr: -1, other: 0 */
    size_t nqueries;
};

struct http_request_st {
    struct bucket_st *bucket;
    VALUE bucket_obj;
    VALUE type;
    int extended;
    int running;
    int completed;
    lcb_http_request_t request;
    lcb_http_cmd_t cmd;
    struct context_st *ctx;
    VALUE on_body_callback;
};

struct timer_st
{
    struct bucket_st *bucket;
    int periodic;
    uint32_t usec;
    lcb_timer_t timer;
    VALUE self;
    VALUE callback;
};

/* Classes */
extern VALUE cBucket;
extern VALUE cCouchRequest;
extern VALUE cResult;
extern VALUE cTimer;

/* Modules */
extern VALUE mCouchbase;
extern VALUE mError;
extern VALUE mMarshal;
extern VALUE mMultiJson;
extern VALUE mURI;

/* Symbols */
extern ID sym_add;
extern ID sym_append;
extern ID sym_assemble_hash;
extern ID sym_body;
extern ID sym_bucket;
extern ID sym_cas;
extern ID sym_chunked;
extern ID sym_content_type;
extern ID sym_create;
extern ID sym_decrement;
extern ID sym_default_flags;
extern ID sym_default_format;
extern ID sym_default_observe_timeout;
extern ID sym_default_ttl;
extern ID sym_delete;
extern ID sym_delta;
extern ID sym_development;
extern ID sym_document;
extern ID sym_environment;
extern ID sym_extended;
extern ID sym_flags;
extern ID sym_flush;
extern ID sym_format;
extern ID sym_found;
extern ID sym_get;
extern ID sym_hostname;
extern ID sym_http_request;
extern ID sym_increment;
extern ID sym_initial;
extern ID sym_key_prefix;
extern ID sym_lock;
extern ID sym_management;
extern ID sym_marshal;
extern ID sym_method;
extern ID sym_node_list;
extern ID sym_not_found;
extern ID sym_num_replicas;
extern ID sym_observe;
extern ID sym_password;
extern ID sym_periodic;
extern ID sym_persisted;
extern ID sym_plain;
extern ID sym_pool;
extern ID sym_port;
extern ID sym_post;
extern ID sym_prepend;
extern ID sym_production;
extern ID sym_put;
extern ID sym_quiet;
extern ID sym_replace;
extern ID sym_replica;
extern ID sym_send_threshold;
extern ID sym_set;
extern ID sym_stats;
extern ID sym_timeout;
extern ID sym_touch;
extern ID sym_ttl;
extern ID sym_type;
extern ID sym_unlock;
extern ID sym_username;
extern ID sym_version;
extern ID sym_view;
extern ID id_arity;
extern ID id_call;
extern ID id_delete;
extern ID id_dump;
extern ID id_dup;
extern ID id_flatten_bang;
extern ID id_has_key_p;
extern ID id_host;
extern ID id_iv_cas;
extern ID id_iv_completed;
extern ID id_iv_error;
extern ID id_iv_flags;
extern ID id_iv_from_master;
extern ID id_iv_headers;
extern ID id_iv_key;
extern ID id_iv_node;
extern ID id_iv_operation;
extern ID id_iv_status;
extern ID id_iv_time_to_persist;
extern ID id_iv_time_to_replicate;
extern ID id_iv_value;
extern ID id_load;
extern ID id_match;
extern ID id_observe_and_wait;
extern ID id_parse;
extern ID id_password;
extern ID id_path;
extern ID id_port;
extern ID id_scheme;
extern ID id_to_s;
extern ID id_user;
extern ID id_verify_observe_options;

/* Errors */
extern VALUE eBaseError;
extern VALUE eValueFormatError;
                                       /* LCB_SUCCESS = 0x00         */
                                       /* LCB_AUTH_CONTINUE = 0x01   */
extern VALUE eAuthError;               /* LCB_AUTH_ERROR = 0x02      */
extern VALUE eDeltaBadvalError;        /* LCB_DELTA_BADVAL = 0x03    */
extern VALUE eTooBigError;             /* LCB_E2BIG = 0x04           */
extern VALUE eBusyError;               /* LCB_EBUSY = 0x05           */
extern VALUE eInternalError;           /* LCB_EINTERNAL = 0x06       */
extern VALUE eInvalidError;            /* LCB_EINVAL = 0x07          */
extern VALUE eNoMemoryError;           /* LCB_ENOMEM = 0x08          */
extern VALUE eRangeError;              /* LCB_ERANGE = 0x09          */
extern VALUE eLibcouchbaseError;       /* LCB_ERROR = 0x0a           */
extern VALUE eTmpFailError;            /* LCB_ETMPFAIL = 0x0b        */
extern VALUE eKeyExistsError;          /* LCB_KEY_EEXISTS = 0x0c     */
extern VALUE eNotFoundError;           /* LCB_KEY_ENOENT = 0x0d      */
extern VALUE eLibeventError;           /* LCB_LIBEVENT_ERROR = 0x0e  */
extern VALUE eNetworkError;            /* LCB_NETWORK_ERROR = 0x0f   */
extern VALUE eNotMyVbucketError;       /* LCB_NOT_MY_VBUCKET = 0x10  */
extern VALUE eNotStoredError;          /* LCB_NOT_STORED = 0x11      */
extern VALUE eNotSupportedError;       /* LCB_NOT_SUPPORTED = 0x12   */
extern VALUE eUnknownCommandError;     /* LCB_UNKNOWN_COMMAND = 0x13 */
extern VALUE eUnknownHostError;        /* LCB_UNKNOWN_HOST = 0x14    */
extern VALUE eProtocolError;           /* LCB_PROTOCOL_ERROR = 0x15  */
extern VALUE eTimeoutError;            /* LCB_ETIMEDOUT = 0x16       */
extern VALUE eConnectError;            /* LCB_CONNECT_ERROR = 0x17   */
extern VALUE eBucketNotFoundError;     /* LCB_BUCKET_ENOENT = 0x18   */
extern VALUE eClientNoMemoryError;     /* LCB_CLIENT_ENOMEM = 0x19   */
extern VALUE eClientTmpFailError;      /* LCB_CLIENT_ETMPFAIL = 0x20 */

void strip_key_prefix(struct bucket_st *bucket, VALUE key);
VALUE cb_check_error(lcb_error_t rc, const char *msg, VALUE key);
VALUE cb_check_error_with_status(lcb_error_t rc, const char *msg, VALUE key, lcb_http_status_t status);
VALUE cb_gc_protect(struct bucket_st *bucket, VALUE val);
VALUE cb_gc_unprotect(struct bucket_st *bucket, VALUE val);
VALUE cb_proc_call(VALUE recv, int argc, ...);
int cb_first_value_i(VALUE key, VALUE value, VALUE arg);
void cb_build_headers(struct context_st *ctx, const char * const *headers);
void maybe_do_loop(struct bucket_st *bucket);
VALUE unify_key(struct bucket_st *bucket, VALUE key, int apply_prefix);
VALUE encode_value(VALUE val, uint32_t flags);
VALUE decode_value(VALUE blob, uint32_t flags, VALUE force_format);
uint32_t flags_set_format(uint32_t flags, ID format);
ID flags_get_format(uint32_t flags);

void storage_callback(lcb_t handle, const void *cookie, lcb_storage_t operation, lcb_error_t error, const lcb_store_resp_t *resp);
void get_callback(lcb_t handle, const void *cookie, lcb_error_t error, const lcb_get_resp_t *resp);
void touch_callback(lcb_t handle, const void *cookie, lcb_error_t error, const lcb_touch_resp_t *resp);
void delete_callback(lcb_t handle, const void *cookie, lcb_error_t error, const lcb_remove_resp_t *resp);
void stat_callback(lcb_t handle, const void *cookie, lcb_error_t error, const lcb_server_stat_resp_t *resp);
void flush_callback(lcb_t handle, const void *cookie, lcb_error_t error, const lcb_flush_resp_t *resp);
void arithmetic_callback(lcb_t handle, const void *cookie, lcb_error_t error, const lcb_arithmetic_resp_t *resp);
void version_callback(lcb_t handle, const void *cookie, lcb_error_t error, const lcb_server_version_resp_t *resp);
void http_complete_callback(lcb_http_request_t request, lcb_t handle, const void *cookie, lcb_error_t error, const lcb_http_resp_t *resp);
void http_data_callback(lcb_http_request_t request, lcb_t handle, const void *cookie, lcb_error_t error, const lcb_http_resp_t *resp);
void observe_callback(lcb_t handle, const void *cookie, lcb_error_t error, const lcb_observe_resp_t *resp);
void unlock_callback(lcb_t handle, const void *cookie, lcb_error_t error, const lcb_unlock_resp_t *resp);


VALUE cb_bucket_alloc(VALUE klass);
void cb_bucket_free(void *ptr);
VALUE cb_bucket_init_copy(VALUE copy, VALUE orig);
VALUE cb_bucket_init(int argc, VALUE *argv, VALUE self);
VALUE cb_bucket_inspect(VALUE self);
VALUE cb_bucket_touch(int argc, VALUE *argv, VALUE self);
VALUE cb_bucket_delete(int argc, VALUE *argv, VALUE self);
VALUE cb_bucket_stats(int argc, VALUE *argv, VALUE self);
VALUE cb_bucket_set(int argc, VALUE *argv, VALUE self);
VALUE cb_bucket_add(int argc, VALUE *argv, VALUE self);
VALUE cb_bucket_replace(int argc, VALUE *argv, VALUE self);
VALUE cb_bucket_append(int argc, VALUE *argv, VALUE self);
VALUE cb_bucket_prepend(int argc, VALUE *argv, VALUE self);
VALUE cb_bucket_aset(int argc, VALUE *argv, VALUE self);
VALUE cb_bucket_get(int argc, VALUE *argv, VALUE self);
VALUE cb_bucket_incr(int argc, VALUE *argv, VALUE self);
VALUE cb_bucket_decr(int argc, VALUE *argv, VALUE self);
VALUE cb_bucket_unlock(int argc, VALUE *argv, VALUE self);
VALUE cb_bucket_run(int argc, VALUE *argv, VALUE self);
VALUE cb_bucket_stop(VALUE self);
VALUE cb_bucket_flush(int argc, VALUE *argv, VALUE self);
VALUE cb_bucket_version(int argc, VALUE *argv, VALUE self);
VALUE cb_bucket_disconnect(VALUE self);
VALUE cb_bucket_reconnect(int argc, VALUE *argv, VALUE self);
VALUE cb_bucket_make_http_request(int argc, VALUE *argv, VALUE self);
VALUE cb_bucket_observe(int argc, VALUE *argv, VALUE self);
VALUE cb_bucket_connected_p(VALUE self);
VALUE cb_bucket_async_p(VALUE self);
VALUE cb_bucket_quiet_get(VALUE self);
VALUE cb_bucket_quiet_set(VALUE self, VALUE val);
VALUE cb_bucket_default_flags_get(VALUE self);
VALUE cb_bucket_default_flags_set(VALUE self, VALUE val);
VALUE cb_bucket_default_format_get(VALUE self);
VALUE cb_bucket_default_format_set(VALUE self, VALUE val);
VALUE cb_bucket_on_error_set(VALUE self, VALUE val);
VALUE cb_bucket_on_error_get(VALUE self);
VALUE cb_bucket_timeout_get(VALUE self);
VALUE cb_bucket_timeout_set(VALUE self, VALUE val);
VALUE cb_bucket_key_prefix_get(VALUE self);
VALUE cb_bucket_key_prefix_set(VALUE self, VALUE val);
VALUE cb_bucket_url_get(VALUE self);
VALUE cb_bucket_hostname_get(VALUE self);
VALUE cb_bucket_port_get(VALUE self);
VALUE cb_bucket_authority_get(VALUE self);
VALUE cb_bucket_bucket_get(VALUE self);
VALUE cb_bucket_pool_get(VALUE self);
VALUE cb_bucket_username_get(VALUE self);
VALUE cb_bucket_password_get(VALUE self);
VALUE cb_bucket_environment_get(VALUE self);
VALUE cb_bucket_num_replicas_get(VALUE self);
VALUE cb_bucket_default_observe_timeout_get(VALUE self);
VALUE cb_bucket_default_observe_timeout_set(VALUE self, VALUE val);

VALUE cb_http_request_alloc(VALUE klass);
VALUE cb_http_request_init(int argc, VALUE *argv, VALUE self);
VALUE cb_http_request_inspect(VALUE self);
VALUE cb_http_request_on_body(VALUE self);
VALUE cb_http_request_perform(VALUE self);
VALUE cb_http_request_pause(VALUE self);
VALUE cb_http_request_continue(VALUE self);
VALUE cb_http_request_path_get(VALUE self);
VALUE cb_http_request_extended_get(VALUE self);
VALUE cb_http_request_chunked_get(VALUE self);

VALUE cb_result_success_p(VALUE self);
VALUE cb_result_inspect(VALUE self);

VALUE cb_timer_alloc(VALUE klass);
VALUE cb_timer_inspect(VALUE self);
VALUE cb_timer_cancel(VALUE self);
VALUE cb_timer_init(int argc, VALUE *argv, VALUE self);

/* Method arguments */

enum command_t {
    cmd_touch       = 0x01,
    cmd_remove      = 0x02,
    cmd_store       = 0x03,
    cmd_get         = 0x04,
    cmd_arith       = 0x05,
    cmd_stats       = 0x06,
    cmd_flush       = 0x07,
    cmd_version     = 0x08,
    cmd_observe     = 0x09,
    cmd_unlock     = 0x10
};

struct params_st
{
    enum command_t type;
    union {
        struct {
            /* number of items */
            size_t num;
            /* array of the items */
            lcb_touch_cmd_t *items;
            /* array of the pointers to the items */
            const lcb_touch_cmd_t **ptr;
            unsigned int quiet : 1;
            unsigned int array : 1;
            lcb_time_t ttl;
        } touch;
        struct {
            /* number of items */
            size_t num;
            /* array of the items */
            lcb_remove_cmd_t *items;
            /* array of the pointers to the items */
            const lcb_remove_cmd_t **ptr;
            unsigned int array : 1;
            /* 1 if it should silense NOT_FOUND errors */
            unsigned int quiet : 1;
            lcb_cas_t cas;
        } remove;
        struct {
            /* number of items */
            size_t num;
            /* array of the items */
            lcb_store_cmd_t *items;
            /* array of the pointers to the items */
            const lcb_store_cmd_t **ptr;
            lcb_storage_t operation;
            lcb_uint32_t flags;
            lcb_time_t ttl;
            lcb_cas_t cas;
            lcb_datatype_t datatype;
            VALUE observe;
        } store;
        struct {
            /* number of items */
            size_t num;
            /* array of the items */
            lcb_get_cmd_t *items;
            /* array of the pointers to the items */
            const lcb_get_cmd_t **ptr;
            /* array of the items for GET_REPLICA command */
            lcb_get_replica_cmd_t *items_gr;
            /* array of the pointers to the items for GET_REPLICA command */
            const lcb_get_replica_cmd_t **ptr_gr;
            unsigned int array : 1;
            unsigned int lock : 1;
            unsigned int replica : 1;
            unsigned int assemble_hash : 1;
            unsigned int extended : 1;
            unsigned int quiet : 1;
            /* arguments given in form of hash key-ttl to "get and touch" */
            unsigned int gat : 1;
            lcb_time_t ttl;
            VALUE forced_format;
            VALUE keys_ary;
        } get;
        struct {
            /* number of items */
            size_t num;
            /* array of the items */
            lcb_arithmetic_cmd_t *items;
            /* array of the pointers to the items */
            const lcb_arithmetic_cmd_t **ptr;
            unsigned int array : 1;
            unsigned int extended : 1;
            unsigned int create : 1;
            lcb_time_t ttl;
            lcb_uint64_t initial;
            lcb_uint64_t delta;
            int sign;
            VALUE format;
            lcb_datatype_t datatype;
        } arith;
        struct {
            /* number of items */
            size_t num;
            /* array of the items */
            lcb_server_stats_cmd_t *items;
            /* array of the pointers to the items */
            const lcb_server_stats_cmd_t **ptr;
            unsigned int array : 1;
        } stats;
        struct {
            /* number of items */
            size_t num;
            /* array of the items */
            lcb_flush_cmd_t *items;
            /* array of the pointers to the items */
            const lcb_flush_cmd_t **ptr;
        } flush;
        struct {
            /* number of items */
            size_t num;
            /* array of the items */
            lcb_server_version_cmd_t *items;
            /* array of the pointers to the items */
            const lcb_server_version_cmd_t **ptr;
        } version;
        struct {
            /* number of items */
            size_t num;
            /* array of the items */
            lcb_observe_cmd_t *items;
            /* array of the pointers to the items */
            const lcb_observe_cmd_t **ptr;
            unsigned int array : 1;
        } observe;
        struct {
            /* number of items */
            size_t num;
            /* array of the items */
            lcb_unlock_cmd_t *items;
            /* array of the pointers to the items */
            const lcb_unlock_cmd_t **ptr;
            unsigned int quiet : 1;
            lcb_cas_t cas;
        } unlock;
    } cmd;
    struct bucket_st *bucket;
    /* helper index for iterators */
    size_t idx;
    /* the approximate size of the data to be sent */
    size_t npayload;
};

void cb_params_destroy(struct params_st *params);
void cb_params_build(struct params_st *params, int argc, VALUE argv);


#endif

