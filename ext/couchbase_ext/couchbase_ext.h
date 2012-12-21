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
#ifdef HAVE_STDINT_H
#include <stdint.h>
#endif
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

#ifndef HAVE_RB_HASH_LOOKUP2
VALUE rb_hash_lookup2(VALUE, VALUE, VALUE);
#endif
#ifndef HAVE_TYPE_ST_INDEX_T
typedef st_data_t st_index_t;
#endif

#define cb_debug_object(OBJ) do { \
    VALUE debug_args[6] = { \
        rb_funcall(OBJ, rb_intern("object_id"), 0), \
        STR_NEW_CSTR(" "), \
        rb_funcall(OBJ, rb_intern("class"), 0), \
        STR_NEW_CSTR(" "), \
        rb_funcall(OBJ, rb_intern("inspect"), 0), \
        STR_NEW_CSTR("\n") }; \
    rb_funcall2(rb_stderr, rb_intern("print"), 6, debug_args); \
} while(0)

#define CB_FMT_MASK        0x3
#define CB_FMT_DOCUMENT    0x0
#define CB_FMT_MARSHAL     0x1
#define CB_FMT_PLAIN       0x2

#define CB_PACKET_HEADER_SIZE 24
/* Structs */
struct cb_bucket_st
{
    lcb_t handle;
    lcb_type_t type;
    struct lcb_io_opt_st *io;
    uint16_t port;
    VALUE authority;
    VALUE hostname;
    VALUE pool;
    VALUE bucket;
    VALUE username;
    VALUE password;
    int async;
    int quiet;
    VALUE default_format;    /* should update +default_flags+ on change */
    uint32_t default_flags;
    time_t default_ttl;
    time_t default_observe_timeout;
    lcb_uint64_t default_arith_create;  /* should the incr/decr create the key? if non-zero, will use arith_init */
    lcb_uint64_t default_arith_init;    /* default initial value for incr/decr */
    uint32_t timeout;
    size_t threshold;       /* the number of bytes to trigger event loop, zero if don't care */
    size_t nbytes;          /* the number of bytes scheduled to be sent */
    VALUE exception;        /* error delivered by error_callback */
    VALUE on_error_proc;    /* is using to deliver errors in async mode */
    VALUE environment;      /* sym_development or sym_production */
    VALUE key_prefix_val;
    VALUE node_list;
    st_table *object_space;
    char destroying;
    VALUE self;             /* the pointer to bucket representation in ruby land */
};

struct cb_http_request_st;
struct cb_context_st
{
    struct cb_bucket_st* bucket;
    int extended;
    VALUE proc;
    VALUE rv;
    VALUE exception;
    VALUE observe_options;
    VALUE force_format;
    VALUE operation;
    VALUE headers_val;
    int headers_built;
    struct cb_http_request_st *request;
    int quiet;
    int arith;           /* incr: +1, decr: -1, other: 0 */
    size_t nqueries;
};

struct cb_http_request_st {
    struct cb_bucket_st *bucket;
    VALUE bucket_obj;
    VALUE type;
    int extended;
    int running;
    int completed;
    lcb_http_request_t request;
    lcb_http_cmd_t cmd;
    struct cb_context_st *ctx;
    VALUE on_body_callback;
};

struct cb_timer_st
{
    struct cb_bucket_st *bucket;
    int periodic;
    uint32_t usec;
    lcb_timer_t timer;
    VALUE self;
    VALUE callback;
};

/* Classes */
extern VALUE cb_cBucket;
extern VALUE cb_cCouchRequest;
extern VALUE cb_cResult;
extern VALUE cb_cTimer;

/* Modules */
extern VALUE cb_mCouchbase;
extern VALUE cb_mError;
extern VALUE cb_mMarshal;
extern VALUE cb_mMultiJson;
extern VALUE cb_mURI;

/* Symbols */
extern ID cb_sym_add;
extern ID cb_sym_append;
extern ID cb_sym_assemble_hash;
extern ID cb_sym_body;
extern ID cb_sym_bucket;
extern ID cb_sym_cas;
extern ID cb_sym_chunked;
extern ID cb_sym_cluster;
extern ID cb_sym_content_type;
extern ID cb_sym_create;
extern ID cb_sym_decrement;
extern ID cb_sym_default_arithmetic_init;
extern ID cb_sym_default_flags;
extern ID cb_sym_default_format;
extern ID cb_sym_default_observe_timeout;
extern ID cb_sym_default_ttl;
extern ID cb_sym_delete;
extern ID cb_sym_delta;
extern ID cb_sym_development;
extern ID cb_sym_document;
extern ID cb_sym_environment;
extern ID cb_sym_extended;
extern ID cb_sym_flags;
extern ID cb_sym_format;
extern ID cb_sym_found;
extern ID cb_sym_get;
extern ID cb_sym_hostname;
extern ID cb_sym_http_request;
extern ID cb_sym_increment;
extern ID cb_sym_initial;
extern ID cb_sym_key_prefix;
extern ID cb_sym_lock;
extern ID cb_sym_management;
extern ID cb_sym_marshal;
extern ID cb_sym_method;
extern ID cb_sym_node_list;
extern ID cb_sym_not_found;
extern ID cb_sym_num_replicas;
extern ID cb_sym_observe;
extern ID cb_sym_password;
extern ID cb_sym_periodic;
extern ID cb_sym_persisted;
extern ID cb_sym_plain;
extern ID cb_sym_pool;
extern ID cb_sym_port;
extern ID cb_sym_post;
extern ID cb_sym_prepend;
extern ID cb_sym_production;
extern ID cb_sym_put;
extern ID cb_sym_quiet;
extern ID cb_sym_replace;
extern ID cb_sym_replica;
extern ID cb_sym_send_threshold;
extern ID cb_sym_set;
extern ID cb_sym_stats;
extern ID cb_sym_timeout;
extern ID cb_sym_touch;
extern ID cb_sym_ttl;
extern ID cb_sym_type;
extern ID cb_sym_unlock;
extern ID cb_sym_username;
extern ID cb_sym_version;
extern ID cb_sym_view;
extern ID cb_id_arity;
extern ID cb_id_call;
extern ID cb_id_delete;
extern ID cb_id_dump;
extern ID cb_id_dup;
extern ID cb_id_flatten_bang;
extern ID cb_id_has_key_p;
extern ID cb_id_host;
extern ID cb_id_iv_body;
extern ID cb_id_iv_cas;
extern ID cb_id_iv_completed;
extern ID cb_id_iv_error;
extern ID cb_id_iv_flags;
extern ID cb_id_iv_from_master;
extern ID cb_id_iv_headers;
extern ID cb_id_iv_inner_exception;
extern ID cb_id_iv_key;
extern ID cb_id_iv_node;
extern ID cb_id_iv_operation;
extern ID cb_id_iv_status;
extern ID cb_id_iv_time_to_persist;
extern ID cb_id_iv_time_to_replicate;
extern ID cb_id_iv_value;
extern ID cb_id_load;
extern ID cb_id_match;
extern ID cb_id_observe_and_wait;
extern ID cb_id_parse;
extern ID cb_id_parse_body_bang;
extern ID cb_id_password;
extern ID cb_id_path;
extern ID cb_id_port;
extern ID cb_id_scheme;
extern ID cb_id_sprintf;
extern ID cb_id_to_s;
extern ID cb_id_user;
extern ID cb_id_verify_observe_options;

/* Errors */
extern VALUE cb_eBaseError;
extern VALUE cb_eValueFormatError;
extern VALUE cb_eHTTPError;
                                       /* LCB_SUCCESS = 0x00         */
                                       /* LCB_AUTH_CONTINUE = 0x01   */
extern VALUE cb_eAuthError;               /* LCB_AUTH_ERROR = 0x02      */
extern VALUE cb_eDeltaBadvalError;        /* LCB_DELTA_BADVAL = 0x03    */
extern VALUE cb_eTooBigError;             /* LCB_E2BIG = 0x04           */
extern VALUE cb_eBusyError;               /* LCB_EBUSY = 0x05           */
extern VALUE cb_eInternalError;           /* LCB_EINTERNAL = 0x06       */
extern VALUE cb_eInvalidError;            /* LCB_EINVAL = 0x07          */
extern VALUE cb_eNoMemoryError;           /* LCB_ENOMEM = 0x08          */
extern VALUE cb_eRangeError;              /* LCB_ERANGE = 0x09          */
extern VALUE cb_eLibcouchbaseError;       /* LCB_ERROR = 0x0a           */
extern VALUE cb_eTmpFailError;            /* LCB_ETMPFAIL = 0x0b        */
extern VALUE cb_eKeyExistsError;          /* LCB_KEY_EEXISTS = 0x0c     */
extern VALUE cb_eNotFoundError;           /* LCB_KEY_ENOENT = 0x0d      */
extern VALUE cb_eDlopenFailedError;       /* LCB_DLOPEN_FAILED = 0x0e   */
extern VALUE cb_eDlsymFailedError;        /* LCB_DLSYM_FAILED = 0x0f    */
extern VALUE cb_eNetworkError;            /* LCB_NETWORK_ERROR = 0x10   */
extern VALUE cb_eNotMyVbucketError;       /* LCB_NOT_MY_VBUCKET = 0x11  */
extern VALUE cb_eNotStoredError;          /* LCB_NOT_STORED = 0x12      */
extern VALUE cb_eNotSupportedError;       /* LCB_NOT_SUPPORTED = 0x13   */
extern VALUE cb_eUnknownCommandError;     /* LCB_UNKNOWN_COMMAND = 0x14 */
extern VALUE cb_eUnknownHostError;        /* LCB_UNKNOWN_HOST = 0x15    */
extern VALUE cb_eProtocolError;           /* LCB_PROTOCOL_ERROR = 0x16  */
extern VALUE cb_eTimeoutError;            /* LCB_ETIMEDOUT = 0x17       */
extern VALUE cb_eConnectError;            /* LCB_CONNECT_ERROR = 0x18   */
extern VALUE cb_eBucketNotFoundError;     /* LCB_BUCKET_ENOENT = 0x19   */
extern VALUE cb_eClientNoMemoryError;     /* LCB_CLIENT_ENOMEM = 0x1a   */
extern VALUE cb_eClientTmpFailError;      /* LCB_CLIENT_ETMPFAIL = 0x1b */
extern VALUE cb_eBadHandleError;          /* LCB_EBADHANDLE = 0x1c      */

/* Default Strings */
extern VALUE cb_vStrDefault;
extern VALUE cb_vStrEmpty;
extern VALUE cb_vStrLocalhost;

void cb_strip_key_prefix(struct cb_bucket_st *bucket, VALUE key);
VALUE cb_check_error(lcb_error_t rc, const char *msg, VALUE key);
VALUE cb_check_error_with_status(lcb_error_t rc, const char *msg, VALUE key, lcb_http_status_t status);
void cb_gc_protect_ptr(struct cb_bucket_st *bucket, void *ptr, VALUE val);
void cb_gc_unprotect_ptr(struct cb_bucket_st *bucket, void *ptr);
VALUE cb_proc_call(struct cb_bucket_st *bucket, VALUE recv, int argc, ...);
int cb_first_value_i(VALUE key, VALUE value, VALUE arg);
void cb_build_headers(struct cb_context_st *ctx, const char * const *headers);
void cb_maybe_do_loop(struct cb_bucket_st *bucket);
VALUE cb_unify_key(struct cb_bucket_st *bucket, VALUE key, int apply_prefix);
VALUE cb_encode_value(VALUE val, uint32_t flags);
VALUE cb_decode_value(VALUE blob, uint32_t flags, VALUE force_format);
uint32_t cb_flags_set_format(uint32_t flags, ID format);
ID cb_flags_get_format(uint32_t flags);
void cb_async_error_notify(struct cb_bucket_st *bucket, VALUE exc);


void cb_storage_callback(lcb_t handle, const void *cookie, lcb_storage_t operation, lcb_error_t error, const lcb_store_resp_t *resp);
void cb_get_callback(lcb_t handle, const void *cookie, lcb_error_t error, const lcb_get_resp_t *resp);
void cb_touch_callback(lcb_t handle, const void *cookie, lcb_error_t error, const lcb_touch_resp_t *resp);
void cb_delete_callback(lcb_t handle, const void *cookie, lcb_error_t error, const lcb_remove_resp_t *resp);
void cb_stat_callback(lcb_t handle, const void *cookie, lcb_error_t error, const lcb_server_stat_resp_t *resp);
void cb_arithmetic_callback(lcb_t handle, const void *cookie, lcb_error_t error, const lcb_arithmetic_resp_t *resp);
void cb_version_callback(lcb_t handle, const void *cookie, lcb_error_t error, const lcb_server_version_resp_t *resp);
void cb_http_complete_callback(lcb_http_request_t request, lcb_t handle, const void *cookie, lcb_error_t error, const lcb_http_resp_t *resp);
void cb_http_data_callback(lcb_http_request_t request, lcb_t handle, const void *cookie, lcb_error_t error, const lcb_http_resp_t *resp);
void cb_observe_callback(lcb_t handle, const void *cookie, lcb_error_t error, const lcb_observe_resp_t *resp);
void cb_unlock_callback(lcb_t handle, const void *cookie, lcb_error_t error, const lcb_unlock_resp_t *resp);

struct cb_context_st *cb_context_alloc(struct cb_bucket_st *bucket);
struct cb_context_st *cb_context_alloc_common(struct cb_bucket_st *bucket, VALUE proc, size_t nqueries);
void cb_context_free(struct cb_context_st *ctx);

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
VALUE cb_bucket_default_arithmetic_init_get(VALUE self);
VALUE cb_bucket_default_arithmetic_init_set(VALUE self, VALUE val);

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

enum cb_command_t {
    cb_cmd_touch       = 0x01,
    cb_cmd_remove      = 0x02,
    cb_cmd_store       = 0x03,
    cb_cmd_get         = 0x04,
    cb_cmd_arith       = 0x05,
    cb_cmd_stats       = 0x06,
    cb_cmd_version     = 0x08,
    cb_cmd_observe     = 0x09,
    cb_cmd_unlock      = 0x10
};

struct cb_params_st
{
    enum cb_command_t type;
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
    struct cb_bucket_st *bucket;
    /* helper index for iterators */
    size_t idx;
    /* the approximate size of the data to be sent */
    size_t npayload;
    VALUE ensurance;
    VALUE args;
};

void cb_params_destroy(struct cb_params_st *params);
void cb_params_build(struct cb_params_st *params);

LIBCOUCHBASE_API
lcb_error_t cb_create_ruby_mt_io_opts(int version, lcb_io_opt_t *io, void *arg);
#endif

