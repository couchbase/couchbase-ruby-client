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

#include "couchbase_config.h"

#if defined(_WIN32) && defined(__cplusplus)
#define __STDC_LIMIT_MACROS
#include <cstdint>
#endif
#ifdef HAVE_STDINT_H
#include <stdint.h>
#endif

#include <ruby.h>
#ifndef RUBY_ST_H
#include <st.h>
#endif

#ifdef HAVE_RUBY_THREAD_H
#include <ruby/thread.h>
#endif

#ifndef HAVE_GETHRTIME
typedef uint64_t hrtime_t;
extern hrtime_t gethrtime(void);
#endif

#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#ifdef HAVE_ERRNO_H
#include <errno.h>
#endif

#ifndef INVALID_SOCKET
#define INVALID_SOCKET -1
#endif

#include <libcouchbase/couchbase.h>
#include <libcouchbase/views.h>
#include <libcouchbase/n1ql.h>

#ifdef HAVE_RUBY_ENCODING_H
#include "ruby/encoding.h"
#define STR_NEW(ptr, len) rb_external_str_new((ptr), (len))
#define STR_NEW_CSTR(str) rb_external_str_new_cstr((str))
#else
#define STR_NEW(ptr, len) rb_str_new((ptr), (len))
#define STR_NEW_CSTR(str) rb_str_new2((str))
#endif

#ifndef HAVE_TYPE_ST_INDEX_T
typedef st_data_t st_index_t;
#endif

#define cb_debug_object(OBJ)                                                                                           \
    do {                                                                                                               \
        VALUE debug_args[6] = {rb_funcall(OBJ, rb_intern("object_id"), 0), STR_NEW_CSTR(" "),                          \
                               rb_funcall(OBJ, rb_intern("class"), 0),     STR_NEW_CSTR(" "),                          \
                               rb_funcall(OBJ, rb_intern("inspect"), 0),   STR_NEW_CSTR("\n")};                        \
        rb_funcall2(rb_stderr, rb_intern("print"), 6, debug_args);                                                     \
    } while (0)

#define CB_FMT_MASK 0x3
#define CB_FMT_DOCUMENT 0x0
#define CB_FMT_MARSHAL 0x1
#define CB_FMT_PLAIN 0x2

/* Structs */
struct cb_bucket_st {
    lcb_t handle;
    lcb_type_t type;
    struct lcb_io_opt_st *io;
    VALUE connstr;
    VALUE username;
    VALUE password;
    VALUE engine;
    uint8_t connected; /* non-zero if instance has been connected */
    uint8_t running; /* non-zero if event loop is running */
    VALUE transcoder;
    uint32_t default_flags;
    time_t default_ttl;
    time_t default_observe_timeout;
    lcb_uint64_t default_arith_create; /* should the incr/decr create the key? if non-zero, will use arith_init */
    lcb_uint64_t default_arith_init;   /* default initial value for incr/decr */
    uint32_t timeout;
    size_t nbytes;         /* the number of bytes scheduled to be sent */
    VALUE exception;       /* error delivered by error_callback */
    VALUE environment;     /* sym_development or sym_production */
    st_table *object_space;
    char destroying;
    VALUE self; /* the pointer to bucket representation in ruby land */
};

struct cb_context_st {
    struct cb_bucket_st *bucket;
    VALUE proc;
    VALUE rv;
    VALUE exception;
    VALUE observe_options;
    VALUE transcoder;
    VALUE transcoder_opts;
    VALUE operation;
    VALUE headers_val;
    int headers_built;
    int arith;        /* incr: +1, decr: -1, other: 0 */
    int all_replicas; /* handle multiple responses from get_replica if non-zero */
    size_t nqueries;
};

struct cb_timer_st {
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

/* Modules */
extern VALUE cb_mCouchbase;
extern VALUE cb_mError;
extern VALUE cb_mTranscoder;
extern VALUE cb_mDocument;
extern VALUE cb_mPlain;
extern VALUE cb_mMarshal;
extern VALUE cb_mURI;
extern VALUE cb_mMultiJson;

/* Symbols */
extern ID cb_sym_add;
extern ID cb_sym_all;
extern ID cb_sym_append;
extern ID cb_sym_assemble_hash;
extern ID cb_sym_body;
extern ID cb_sym_bootstrap_transports;
extern ID cb_sym_bucket;
extern ID cb_sym_cas;
extern ID cb_sym_cccp;
extern ID cb_sym_chunked;
extern ID cb_sym_cluster;
extern ID cb_sym_connect;
extern ID cb_sym_content_type;
extern ID cb_sym_create;
extern ID cb_sym_decrement;
extern ID cb_sym_default;
extern ID cb_sym_default_arithmetic_init;
extern ID cb_sym_default_flags;
extern ID cb_sym_default_format;
extern ID cb_sym_default_observe_timeout;
extern ID cb_sym_default_ttl;
extern ID cb_sym_delete;
extern ID cb_sym_delta;
extern ID cb_sym_development;
extern ID cb_sym_document;
extern ID cb_sym_engine;
extern ID cb_sym_environment;
extern ID cb_sym_extended;
extern ID cb_sym_first;
extern ID cb_sym_flags;
extern ID cb_sym_forced;
extern ID cb_sym_format;
extern ID cb_sym_found;
extern ID cb_sym_get;
extern ID cb_sym_host;
extern ID cb_sym_hostname;
extern ID cb_sym_http;
extern ID cb_sym_increment;
extern ID cb_sym_initial;
extern ID cb_sym_iocp;
extern ID cb_sym_libev;
extern ID cb_sym_libevent;
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
extern ID cb_sym_replicated;
extern ID cb_sym_plain;
extern ID cb_sym_pool;
extern ID cb_sym_port;
extern ID cb_sym_post;
extern ID cb_sym_prepend;
extern ID cb_sym_production;
extern ID cb_sym_put;
extern ID cb_sym_replace;
extern ID cb_sym_replica;
extern ID cb_sym_rows;
extern ID cb_sym_meta;
extern ID cb_sym_select;
extern ID cb_sym_set;
extern ID cb_sym_stats;
extern ID cb_sym_timeout;
extern ID cb_sym_touch;
extern ID cb_sym_transcoder;
extern ID cb_sym_ttl;
extern ID cb_sym_type;
extern ID cb_sym_unlock;
extern ID cb_sym_username;
extern ID cb_sym_version;
extern ID cb_sym_view;
extern ID cb_sym_raw;
extern ID cb_sym_n1ql;
extern ID cb_sym_fts;
extern ID cb_sym_cbas;
extern ID cb_sym_chunks;
extern ID cb_sym_headers;
extern ID cb_sym_status;
extern ID cb_id_add_shutdown_hook;
extern ID cb_id_arity;
extern ID cb_id_call;
extern ID cb_id_create_timer;
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
extern ID cb_id_iv_meta;
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
extern ID cb_id_next_tick;
extern ID cb_id_observe_and_wait;
extern ID cb_id_parse;
extern ID cb_id_password;
extern ID cb_id_path;
extern ID cb_id_port;
extern ID cb_id_scheme;
extern ID cb_id_sprintf;
extern ID cb_id_to_s;
extern ID cb_id_user;
extern ID cb_id_verify_observe_options;
extern ID cb_sym_include_docs;
extern ID cb_sym_docs_concurrent_max;

/* Errors */
extern VALUE cb_eLibraryError;

extern VALUE cb_eBaseError;
extern VALUE cb_eValueFormatError;
extern VALUE cb_eHTTPError;
extern VALUE cb_eQuery;
/* LCB_SUCCESS = 0x00         */
/* LCB_AUTH_CONTINUE = 0x01   */
extern VALUE cb_eAuthError;             /* LCB_AUTH_ERROR = 0x02      */
extern VALUE cb_eDeltaBadvalError;      /* LCB_DELTA_BADVAL = 0x03    */
extern VALUE cb_eTooBigError;           /* LCB_E2BIG = 0x04           */
extern VALUE cb_eBusyError;             /* LCB_EBUSY = 0x05           */
extern VALUE cb_eInternalError;         /* LCB_EINTERNAL = 0x06       */
extern VALUE cb_eInvalidError;          /* LCB_EINVAL = 0x07          */
extern VALUE cb_eNoMemoryError;         /* LCB_ENOMEM = 0x08          */
extern VALUE cb_eRangeError;            /* LCB_ERANGE = 0x09          */
extern VALUE cb_eLibcouchbaseError;     /* LCB_ERROR = 0x0a           */
extern VALUE cb_eTmpFailError;          /* LCB_ETMPFAIL = 0x0b        */
extern VALUE cb_eKeyExistsError;        /* LCB_KEY_EEXISTS = 0x0c     */
extern VALUE cb_eNotFoundError;         /* LCB_KEY_ENOENT = 0x0d      */
extern VALUE cb_eDlopenFailedError;     /* LCB_DLOPEN_FAILED = 0x0e   */
extern VALUE cb_eDlsymFailedError;      /* LCB_DLSYM_FAILED = 0x0f    */
extern VALUE cb_eNetworkError;          /* LCB_NETWORK_ERROR = 0x10   */
extern VALUE cb_eNotMyVbucketError;     /* LCB_NOT_MY_VBUCKET = 0x11  */
extern VALUE cb_eNotStoredError;        /* LCB_NOT_STORED = 0x12      */
extern VALUE cb_eNotSupportedError;     /* LCB_NOT_SUPPORTED = 0x13   */
extern VALUE cb_eUnknownCommandError;   /* LCB_UNKNOWN_COMMAND = 0x14 */
extern VALUE cb_eUnknownHostError;      /* LCB_UNKNOWN_HOST = 0x15    */
extern VALUE cb_eProtocolError;         /* LCB_PROTOCOL_ERROR = 0x16  */
extern VALUE cb_eTimeoutError;          /* LCB_ETIMEDOUT = 0x17       */
extern VALUE cb_eConnectError;          /* LCB_CONNECT_ERROR = 0x18   */
extern VALUE cb_eBucketNotFoundError;   /* LCB_BUCKET_ENOENT = 0x19   */
extern VALUE cb_eClientNoMemoryError;   /* LCB_CLIENT_ENOMEM = 0x1a   */
extern VALUE cb_eClientTmpFailError;    /* LCB_CLIENT_ETMPFAIL = 0x1b */
extern VALUE cb_eBadHandleError;        /* LCB_EBADHANDLE = 0x1c      */
extern VALUE cb_eServerBug;             /* LCB_SERVER_BUG = 0x1d      */
extern VALUE cb_ePluginVersionMismatch; /* LCB_PLUGIN_VERSION_MISMATCH = 0x1e */
extern VALUE cb_eInvalidHostFormat;     /* LCB_INVALID_HOST_FORMAT = 0x1f     */
extern VALUE cb_eInvalidChar;           /* LCB_INVALID_CHAR = 0x20            */
extern VALUE cb_eDurabilityTooMany;     /* LCB_DURABILITY_ETOOMANY = 0x21 */
extern VALUE cb_eDuplicateCommands;     /* LCB_DUPLICATE_COMMANDS = 0x22 */
extern VALUE cb_eNoMatchingServer;      /* LCB_NO_MATCHING_SERVER = 0x23 */
extern VALUE cb_eBadEnvironment;        /* LCB_BAD_ENVIRONMENT = 0x24 */
extern VALUE cb_eBusy;                  /* LCB_BUSY = 0x25 */
extern VALUE cb_eInvalidUsername;       /* LCB_INVALID_USERNAME = 0x26 */

/* Default Strings */
extern VALUE cb_vStrDefault;
extern VALUE cb_vStrEmpty;
extern VALUE cb_vStrLocalhost;

typedef void (*mark_f)(void *, struct cb_bucket_st *);
VALUE cb_check_error(lcb_error_t rc, const char *msg, VALUE key);
VALUE cb_check_error_with_status(lcb_error_t rc, const char *msg, VALUE key, lcb_http_status_t status);
int cb_bucket_connected_bang(struct cb_bucket_st *bucket, VALUE operation);
void cb_gc_protect_ptr(struct cb_bucket_st *bucket, void *ptr, mark_f mark_func);
void cb_gc_unprotect_ptr(struct cb_bucket_st *bucket, void *ptr);
int cb_first_value_i(VALUE key, VALUE value, VALUE arg);
VALUE cb_encode_value(VALUE transcoder, VALUE val, uint32_t *flags, VALUE options);
VALUE cb_decode_value(VALUE transcoder, VALUE blob, uint32_t flags, VALUE options);

void cb_observe_callback(lcb_t handle, int cbtype, const lcb_RESPBASE *rb);
void cb_storage_callback(lcb_t handle, int cbtype, const lcb_RESPBASE *rb);
void cb_remove_callback(lcb_t handle, int cbtype, const lcb_RESPBASE *rb);
void cb_version_callback(lcb_t handle, int cbtype, const lcb_RESPBASE *rb);
void cb_stat_callback(lcb_t handle, int cbtype, const lcb_RESPBASE *rb);
void cb_arithmetic_callback(lcb_t handle, int cbtype, const lcb_RESPBASE *rb);
void cb_unlock_callback(lcb_t handle, int cbtype, const lcb_RESPBASE *rb);
void cb_touch_callback(lcb_t handle, int cbtype, const lcb_RESPBASE *rb);
void cb_get_callback(lcb_t handle, int cbtype, const lcb_RESPBASE *rb);
void cb_http_callback(lcb_t instance, int cbtype, const lcb_RESPBASE *r);

VALUE cb_get_transcoder(struct cb_bucket_st *bucket, VALUE override, int compat, VALUE opts);

struct cb_context_st *cb_context_alloc(struct cb_bucket_st *bucket);
struct cb_context_st *cb_context_alloc_common(struct cb_bucket_st *bucket, size_t nqueries);
void cb_context_free(struct cb_context_st *ctx);

VALUE cb_bucket_alloc(VALUE klass);
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
VALUE cb_bucket_query(int argc, VALUE *argv, VALUE self);
VALUE cb_bucket_version(VALUE self);
VALUE cb_bucket_disconnect(VALUE self);
VALUE cb_bucket_reconnect(int argc, VALUE *argv, VALUE self);
VALUE cb_bucket_observe(int argc, VALUE *argv, VALUE self);
VALUE cb_bucket_connected_p(VALUE self);
VALUE cb_bucket_transcoder_get(VALUE self);
VALUE cb_bucket_transcoder_set(VALUE self, VALUE val);
VALUE cb_bucket_default_flags_get(VALUE self);
VALUE cb_bucket_default_flags_set(VALUE self, VALUE val);
VALUE cb_bucket_default_format_get(VALUE self);
VALUE cb_bucket_default_format_set(VALUE self, VALUE val);
VALUE cb_bucket_timeout_get(VALUE self);
VALUE cb_bucket_timeout_set(VALUE self, VALUE val);
VALUE cb_bucket_bucket_get(VALUE self);
VALUE cb_bucket_connstr_get(VALUE self);
VALUE cb_bucket_environment_get(VALUE self);
VALUE cb_bucket_num_replicas_get(VALUE self);
VALUE cb_bucket_default_observe_timeout_get(VALUE self);
VALUE cb_bucket_default_observe_timeout_set(VALUE self, VALUE val);
VALUE cb_bucket_default_arithmetic_init_get(VALUE self);
VALUE cb_bucket_default_arithmetic_init_set(VALUE self, VALUE val);
VALUE cb_bucket___http_query(VALUE self, VALUE type, VALUE method, VALUE path, VALUE body, VALUE content_type, VALUE username, VALUE password, VALUE hostname);
VALUE cb_bucket___view_query(int argc, VALUE *argv, VALUE self);

VALUE cb_result_success_p(VALUE self);
VALUE cb_result_inspect(VALUE self);

/* plugin init functions */
LIBCOUCHBASE_API
lcb_error_t cb_create_ruby_mt_io_opts(int version, lcb_io_opt_t *io, void *arg);

/* shortcut functions */
VALUE cb_exc_new_at(VALUE klass, lcb_error_t code, const char *file, int line, const char *fmt, ...)
#ifdef __GNUC__
    __attribute__((format(printf, 5, 6)))
#endif
    ;
#define cb_exc_new(klass, code, fmt, ...) cb_exc_new_at(klass, code, __FILE__, __LINE__, fmt, __VA_ARGS__)
#define cb_exc_new2(klass, code, msg) cb_exc_new_at(klass, code, __FILE__, __LINE__, msg)
#define cb_exc_new_msg(klass, fmt, ...) cb_exc_new_at(klass, 0, __FILE__, __LINE__, fmt, __VA_ARGS__)

NORETURN(void cb_raise_at(VALUE klass, lcb_error_t code, const char *file, int line, const char *fmt, ...))
#ifdef __GNUC__
__attribute__((format(printf, 5, 6)))
#endif
;
#define cb_raise(klass, code, fmt, ...) cb_raise_at(klass, code, __FILE__, __LINE__, fmt, __VA_ARGS__)
#define cb_raise2(klass, code, msg) cb_raise_at(klass, code, __FILE__, __LINE__, msg)
#define cb_raise_msg(klass, fmt, ...) cb_raise_at(klass, 0, __FILE__, __LINE__, fmt, __VA_ARGS__)
#define cb_raise_msg2(klass, msg) cb_raise_at(klass, 0, __FILE__, __LINE__, msg)

void init_library_error();
void init_views();

#endif
