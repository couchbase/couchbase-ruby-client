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

#include <ruby.h>
#ifndef RUBY_ST_H
#include <st.h>
#endif

#include <libcouchbase/couchbase.h>
#include "couchbase_ext.h"

#ifdef HAVE_STDARG_PROTOTYPES
#include <stdarg.h>
#define va_init_list(a,b) va_start(a,b)
#else
#include <varargs.h>
#define va_init_list(a,b) va_start(a)
#endif

#define debug_object(OBJ) \
    rb_funcall(rb_stderr, rb_intern("print"), 1, rb_funcall(OBJ, rb_intern("object_id"), 0)); \
    rb_funcall(rb_stderr, rb_intern("print"), 1, rb_str_new2(" ")); \
    rb_funcall(rb_stderr, rb_intern("print"), 1, rb_funcall(OBJ, rb_intern("class"), 0)); \
    rb_funcall(rb_stderr, rb_intern("print"), 1, rb_str_new2(" ")); \
    rb_funcall(rb_stderr, rb_intern("puts"), 1, rb_funcall(OBJ, rb_intern("inspect"), 0));

#define FMT_MASK        0x3
#define FMT_DOCUMENT    0x0
#define FMT_MARSHAL     0x1
#define FMT_PLAIN       0x2

#define HEADER_SIZE     24

struct bucket_st
{
    libcouchbase_t handle;
    struct libcouchbase_io_opt_st *io;
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
};

struct couch_request_st;
struct context_st
{
    struct bucket_st* bucket;
    int extended;
    VALUE proc;
    void *rv;
    VALUE exception;
    VALUE force_format;
    struct couch_request_st *request;
    int quiet;
    int arithm;           /* incr: +1, decr: -1, other: 0 */
    size_t nqueries;
};

struct couch_request_st {
    struct bucket_st *bucket;
    VALUE bucket_obj;
    char *path;
    size_t npath;
    char *body;
    size_t nbody;
    int chunked;
    int extended;
    int running;
    int completed;
    libcouchbase_http_method_t method;
    libcouchbase_http_request_t request;
    struct context_st *ctx;
    VALUE on_body_callback;
};

struct key_traits_st
{
    VALUE keys_ary;
    size_t nkeys;
    char **keys;
    libcouchbase_size_t *lens;
    time_t *ttls;
    int extended;
    int explicit_ttl;
    int quiet;
    int mgat;
    int is_array;
    int assemble_hash;
    int lock;
    int replica;
    struct bucket_st *bucket;
    VALUE force_format;
};

struct timer_st
{
    struct bucket_st *bucket;
    int periodic;
    uint32_t usec;
    libcouchbase_timer_t timer;
    VALUE self;
    VALUE callback;
};

static VALUE mCouchbase, mError, mMultiJson, mURI, mMarshal, cBucket, cResult, cCouchRequest, cTimer;

static ID  sym_add,
           sym_append,
           sym_assemble_hash,
           sym_body,
           sym_bucket,
           sym_cas,
           sym_chunked,
           sym_couch_request,
           sym_create,
           sym_decrement,
           sym_default_flags,
           sym_default_format,
           sym_default_observe_timeout,
           sym_default_ttl,
           sym_delete,
           sym_delta,
           sym_development,
           sym_document,
           sym_environment,
           sym_extended,
           sym_flags,
           sym_flush,
           sym_format,
           sym_found,
           sym_get,
           sym_hostname,
           sym_increment,
           sym_initial,
           sym_key_prefix,
           sym_lock,
           sym_marshal,
           sym_method,
           sym_node_list,
           sym_not_found,
           sym_num_replicas,
           sym_observe,
           sym_password,
           sym_periodic,
           sym_persisted,
           sym_plain,
           sym_pool,
           sym_port,
           sym_post,
           sym_prepend,
           sym_production,
           sym_put,
           sym_quiet,
           sym_replace,
           sym_replica,
           sym_send_threshold,
           sym_set,
           sym_stats,
           sym_timeout,
           sym_touch,
           sym_ttl,
           sym_username,
           sym_version,
           id_arity,
           id_call,
           id_delete,
           id_dump,
           id_flatten_bang,
           id_has_key_p,
           id_host,
           id_iv_cas,
           id_iv_completed,
           id_iv_error,
           id_iv_flags,
           id_iv_from_master,
           id_iv_key,
           id_iv_node,
           id_iv_operation,
           id_iv_status,
           id_iv_status,
           id_iv_time_to_persist,
           id_iv_time_to_replicate,
           id_iv_value,
           id_dup,
           id_load,
           id_match,
           id_parse,
           id_password,
           id_path,
           id_port,
           id_scheme,
           id_to_s,
           id_user;

/* base error */
static VALUE eBaseError;
static VALUE eValueFormatError;

/* libcouchbase errors */
                                       /*LIBCOUCHBASE_SUCCESS = 0x00*/
                                       /*LIBCOUCHBASE_AUTH_CONTINUE = 0x01*/
static VALUE eAuthError;               /*LIBCOUCHBASE_AUTH_ERROR = 0x02*/
static VALUE eDeltaBadvalError;        /*LIBCOUCHBASE_DELTA_BADVAL = 0x03*/
static VALUE eTooBigError;             /*LIBCOUCHBASE_E2BIG = 0x04*/
static VALUE eBusyError;               /*LIBCOUCHBASE_EBUSY = 0x05*/
static VALUE eInternalError;           /*LIBCOUCHBASE_EINTERNAL = 0x06*/
static VALUE eInvalidError;            /*LIBCOUCHBASE_EINVAL = 0x07*/
static VALUE eNoMemoryError;           /*LIBCOUCHBASE_ENOMEM = 0x08*/
static VALUE eRangeError;              /*LIBCOUCHBASE_ERANGE = 0x09*/
static VALUE eLibcouchbaseError;       /*LIBCOUCHBASE_ERROR = 0x0a*/
static VALUE eTmpFailError;            /*LIBCOUCHBASE_ETMPFAIL = 0x0b*/
static VALUE eKeyExistsError;          /*LIBCOUCHBASE_KEY_EEXISTS = 0x0c*/
static VALUE eNotFoundError;           /*LIBCOUCHBASE_KEY_ENOENT = 0x0d*/
static VALUE eLibeventError;           /*LIBCOUCHBASE_LIBEVENT_ERROR = 0x0e*/
static VALUE eNetworkError;            /*LIBCOUCHBASE_NETWORK_ERROR = 0x0f*/
static VALUE eNotMyVbucketError;       /*LIBCOUCHBASE_NOT_MY_VBUCKET = 0x10*/
static VALUE eNotStoredError;          /*LIBCOUCHBASE_NOT_STORED = 0x11*/
static VALUE eNotSupportedError;       /*LIBCOUCHBASE_NOT_SUPPORTED = 0x12*/
static VALUE eUnknownCommandError;     /*LIBCOUCHBASE_UNKNOWN_COMMAND = 0x13*/
static VALUE eUnknownHostError;        /*LIBCOUCHBASE_UNKNOWN_HOST = 0x14*/
static VALUE eProtocolError;           /*LIBCOUCHBASE_PROTOCOL_ERROR = 0x15*/
static VALUE eTimeoutError;            /*LIBCOUCHBASE_ETIMEDOUT = 0x16*/
static VALUE eConnectError;            /*LIBCOUCHBASE_CONNECT_ERROR = 0x17*/
static VALUE eBucketNotFoundError;     /*LIBCOUCHBASE_BUCKET_ENOENT = 0x18*/
static VALUE eClientNoMemoryError;     /*LIBCOUCHBASE_CLIENT_ENOMEM = 0x19*/

static void maybe_do_loop(struct bucket_st *bucket);

    static void
cb_gc_protect(struct bucket_st *bucket, VALUE val)
{
    rb_hash_aset(bucket->object_space, val|1, val);
}

    static void
cb_gc_unprotect(struct bucket_st *bucket, VALUE val)
{
    rb_funcall(bucket->object_space, id_delete, 1, val|1);
}

    static VALUE
cb_proc_call(VALUE recv, int argc, ...)
{
    VALUE *argv;
    va_list ar;
    int arity;
    int ii;

    arity = FIX2INT(rb_funcall(recv, id_arity, 0));
    if (arity < 0) {
        arity = argc;
    }
    if (arity > 0) {
        va_init_list(ar, argc);
        argv = ALLOCA_N(VALUE, argc);
        for (ii = 0; ii < arity; ++ii) {
            if (ii < argc) {
                argv[ii] = va_arg(ar, VALUE);
            } else {
                argv[ii] = Qnil;
            }
        }
        va_end(ar);
    } else {
        argv = NULL;
    }
    return rb_funcall2(recv, id_call, arity, argv);
}

VALUE
cb_hash_delete(VALUE hash, VALUE key)
{
    return rb_funcall(hash, id_delete, 1, key);
}

/* Helper to convert return code from libcouchbase to meaningful exception.
 * Returns nil if the code considering successful and exception object
 * otherwise. Store given string to exceptions as message, and also
 * initialize +error+ attribute with given return code.  */
    static VALUE
cb_check_error_with_status(libcouchbase_error_t rc, const char *msg, VALUE key,
        libcouchbase_http_status_t status)
{
    VALUE klass, exc, str;
    char buf[300];

    if (rc == LIBCOUCHBASE_SUCCESS || rc == LIBCOUCHBASE_AUTH_CONTINUE) {
        return Qnil;
    }
    switch (rc) {
        case LIBCOUCHBASE_AUTH_ERROR:
            klass = eAuthError;
            break;
        case LIBCOUCHBASE_DELTA_BADVAL:
            klass = eDeltaBadvalError;
            break;
        case LIBCOUCHBASE_E2BIG:
            klass = eTooBigError;
            break;
        case LIBCOUCHBASE_EBUSY:
            klass = eBusyError;
            break;
        case LIBCOUCHBASE_EINTERNAL:
            klass = eInternalError;
            break;
        case LIBCOUCHBASE_EINVAL:
            klass = eInvalidError;
            break;
        case LIBCOUCHBASE_ENOMEM:
            klass = eNoMemoryError;
            break;
        case LIBCOUCHBASE_ERANGE:
            klass = eRangeError;
            break;
        case LIBCOUCHBASE_ETMPFAIL:
            klass = eTmpFailError;
            break;
        case LIBCOUCHBASE_KEY_EEXISTS:
            klass = eKeyExistsError;
            break;
        case LIBCOUCHBASE_KEY_ENOENT:
            klass = eNotFoundError;
            break;
        case LIBCOUCHBASE_LIBEVENT_ERROR:
            klass = eLibeventError;
            break;
        case LIBCOUCHBASE_NETWORK_ERROR:
            klass = eNetworkError;
            break;
        case LIBCOUCHBASE_NOT_MY_VBUCKET:
            klass = eNotMyVbucketError;
            break;
        case LIBCOUCHBASE_NOT_STORED:
            klass = eNotStoredError;
            break;
        case LIBCOUCHBASE_NOT_SUPPORTED:
            klass = eNotSupportedError;
            break;
        case LIBCOUCHBASE_UNKNOWN_COMMAND:
            klass = eUnknownCommandError;
            break;
        case LIBCOUCHBASE_UNKNOWN_HOST:
            klass = eUnknownHostError;
            break;
        case LIBCOUCHBASE_PROTOCOL_ERROR:
            klass = eProtocolError;
            break;
        case LIBCOUCHBASE_ETIMEDOUT:
            klass = eTimeoutError;
            break;
        case LIBCOUCHBASE_CONNECT_ERROR:
            klass = eConnectError;
            break;
        case LIBCOUCHBASE_BUCKET_ENOENT:
            klass = eBucketNotFoundError;
            break;
        case LIBCOUCHBASE_CLIENT_ENOMEM:
            klass = eClientNoMemoryError;
            break;
        case LIBCOUCHBASE_ERROR:
            /* fall through */
        default:
            klass = eLibcouchbaseError;
    }

    str = rb_str_buf_new2(msg ? msg : "");
    rb_str_buf_cat2(str, " (");
    if (key != Qnil) {
        snprintf(buf, 300, "key=\"%s\", ", RSTRING_PTR(key));
        rb_str_buf_cat2(str, buf);
    }
    if (status > 0) {
        const char *reason = NULL;
        snprintf(buf, 300, "status=\"%d\"", status);
        rb_str_buf_cat2(str, buf);
        switch (status) {
            case LIBCOUCHBASE_HTTP_STATUS_BAD_REQUEST:
                reason = " (Bad Request)";
                break;
            case LIBCOUCHBASE_HTTP_STATUS_UNAUTHORIZED:
                reason = " (Unauthorized)";
                break;
            case LIBCOUCHBASE_HTTP_STATUS_PAYMENT_REQUIRED:
                reason = " (Payment Required)";
                break;
            case LIBCOUCHBASE_HTTP_STATUS_FORBIDDEN:
                reason = " (Forbidden)";
                break;
            case LIBCOUCHBASE_HTTP_STATUS_NOT_FOUND:
                reason = " (Not Found)";
                break;
            case LIBCOUCHBASE_HTTP_STATUS_METHOD_NOT_ALLOWED:
                reason = " (Method Not Allowed)";
                break;
            case LIBCOUCHBASE_HTTP_STATUS_NOT_ACCEPTABLE:
                reason = " (Not Acceptable)";
                break;
            case LIBCOUCHBASE_HTTP_STATUS_PROXY_AUTHENTICATION_REQUIRED:
                reason = " (Proxy Authentication Required)";
                break;
            case LIBCOUCHBASE_HTTP_STATUS_REQUEST_TIMEOUT:
                reason = " (Request Timeout)";
                break;
            case LIBCOUCHBASE_HTTP_STATUS_CONFLICT:
                reason = " (Conflict)";
                break;
            case LIBCOUCHBASE_HTTP_STATUS_GONE:
                reason = " (Gone)";
                break;
            case LIBCOUCHBASE_HTTP_STATUS_LENGTH_REQUIRED:
                reason = " (Length Required)";
                break;
            case LIBCOUCHBASE_HTTP_STATUS_PRECONDITION_FAILED:
                reason = " (Precondition Failed)";
                break;
            case LIBCOUCHBASE_HTTP_STATUS_REQUEST_ENTITY_TOO_LARGE:
                reason = " (Request Entity Too Large)";
                break;
            case LIBCOUCHBASE_HTTP_STATUS_REQUEST_URI_TOO_LONG:
                reason = " (Request Uri Too Long)";
                break;
            case LIBCOUCHBASE_HTTP_STATUS_UNSUPPORTED_MEDIA_TYPE:
                reason = " (Unsupported Media Type)";
                break;
            case LIBCOUCHBASE_HTTP_STATUS_REQUESTED_RANGE_NOT_SATISFIABLE:
                reason = " (Requested Range Not Satisfiable)";
                break;
            case LIBCOUCHBASE_HTTP_STATUS_EXPECTATION_FAILED:
                reason = " (Expectation Failed)";
                break;
            case LIBCOUCHBASE_HTTP_STATUS_UNPROCESSABLE_ENTITY:
                reason = " (Unprocessable Entity)";
                break;
            case LIBCOUCHBASE_HTTP_STATUS_LOCKED:
                reason = " (Locked)";
                break;
            case LIBCOUCHBASE_HTTP_STATUS_FAILED_DEPENDENCY:
                reason = " (Failed Dependency)";
                break;
            case LIBCOUCHBASE_HTTP_STATUS_INTERNAL_SERVER_ERROR:
                reason = " (Internal Server Error)";
                break;
            case LIBCOUCHBASE_HTTP_STATUS_NOT_IMPLEMENTED:
                reason = " (Not Implemented)";
                break;
            case LIBCOUCHBASE_HTTP_STATUS_BAD_GATEWAY:
                reason = " (Bad Gateway)";
                break;
            case LIBCOUCHBASE_HTTP_STATUS_SERVICE_UNAVAILABLE:
                reason = " (Service Unavailable)";
                break;
            case LIBCOUCHBASE_HTTP_STATUS_GATEWAY_TIMEOUT:
                reason = " (Gateway Timeout)";
                break;
            case LIBCOUCHBASE_HTTP_STATUS_HTTP_VERSION_NOT_SUPPORTED:
                reason = " (Http Version Not Supported)";
                break;
            case LIBCOUCHBASE_HTTP_STATUS_INSUFFICIENT_STORAGE:
                reason = " (Insufficient Storage)";
                break;
            default:
                reason = "";
        }
        rb_str_buf_cat2(str, reason);
        rb_str_buf_cat2(str, ", ");

    }
    snprintf(buf, 300, "error=0x%02x)", rc);
    rb_str_buf_cat2(str, buf);
    exc = rb_exc_new3(klass, str);
    rb_ivar_set(exc, id_iv_error, INT2FIX(rc));
    rb_ivar_set(exc, id_iv_key, key);
    rb_ivar_set(exc, id_iv_cas, Qnil);
    rb_ivar_set(exc, id_iv_operation, Qnil);
    rb_ivar_set(exc, id_iv_status, status ? INT2FIX(status) : Qnil);
    return exc;
}

    static VALUE
cb_check_error(libcouchbase_error_t rc, const char *msg, VALUE key)
{
    return cb_check_error_with_status(rc, msg, key, 0);
}


    static inline uint32_t
flags_set_format(uint32_t flags, ID format)
{
    flags &= ~((uint32_t)FMT_MASK); /* clear format bits */

    if (format == sym_document) {
        return flags | FMT_DOCUMENT;
    } else if (format == sym_marshal) {
        return flags | FMT_MARSHAL;
    } else if (format == sym_plain) {
        return flags | FMT_PLAIN;
    }
    return flags; /* document is the default */
}

    static inline ID
flags_get_format(uint32_t flags)
{
    flags &= FMT_MASK; /* select format bits */

    switch (flags) {
        case FMT_DOCUMENT:
            return sym_document;
        case FMT_MARSHAL:
            return sym_marshal;
        case FMT_PLAIN:
            /* fall through */
        default:
            /* all other formats treated as plain */
            return sym_plain;
    }
}


    static VALUE
do_encode(VALUE *args)
{
    VALUE val = args[0];
    uint32_t flags = ((uint32_t)args[1] & FMT_MASK);

    switch (flags) {
        case FMT_DOCUMENT:
            return rb_funcall(mMultiJson, id_dump, 1, val);
        case FMT_MARSHAL:
            return rb_funcall(mMarshal, id_dump, 1, val);
        case FMT_PLAIN:
            /* fall through */
        default:
            /* all other formats treated as plain */
            return val;
    }
}

    static VALUE
do_decode(VALUE *args)
{
    VALUE blob = args[0];
    VALUE force_format = args[2];

    if (TYPE(force_format) == T_SYMBOL) {
        if (force_format == sym_document) {
            return rb_funcall(mMultiJson, id_load, 1, blob);
        } else if (force_format == sym_marshal) {
            return rb_funcall(mMarshal, id_load, 1, blob);
        } else { /* sym_plain and any other symbol */
            return blob;
        }
    } else {
        uint32_t flags = ((uint32_t)args[1] & FMT_MASK);

        switch (flags) {
            case FMT_DOCUMENT:
                return rb_funcall(mMultiJson, id_load, 1, blob);
            case FMT_MARSHAL:
                return rb_funcall(mMarshal, id_load, 1, blob);
            case FMT_PLAIN:
                /* fall through */
            default:
                /* all other formats treated as plain */
                return blob;
        }
    }
}

    static VALUE
coding_failed(void)
{
    return Qundef;
}

    static VALUE
encode_value(VALUE val, uint32_t flags)
{
    VALUE blob, args[2];

    args[0] = val;
    args[1] = (VALUE)flags;
    blob = rb_rescue(do_encode, (VALUE)args, coding_failed, 0);
    /* it must be bytestring after all */
    if (TYPE(blob) != T_STRING) {
        return Qundef;
    }
    return blob;
}

    static VALUE
decode_value(VALUE blob, uint32_t flags, VALUE force_format)
{
    VALUE val, args[3];

    /* first it must be bytestring */
    if (TYPE(blob) != T_STRING) {
        return Qundef;
    }
    args[0] = blob;
    args[1] = (VALUE)flags;
    args[2] = (VALUE)force_format;
    val = rb_rescue(do_decode, (VALUE)args, coding_failed, 0);
    return val;
}

    static void
strip_key_prefix(struct bucket_st *bucket, VALUE key)
{
    if (bucket->key_prefix) {
        rb_str_update(key, 0, RSTRING_LEN(bucket->key_prefix_val), rb_str_new2(""));
    }
}

    static VALUE
unify_key(struct bucket_st *bucket, VALUE key, int apply_prefix)
{
    VALUE ret;

    if (bucket->key_prefix && apply_prefix) {
        ret = rb_str_dup(bucket->key_prefix_val);
    } else {
        ret = rb_str_new2("");
    }
    switch (TYPE(key)) {
        case T_STRING:
            rb_str_concat(ret, key);
            break;
        case T_SYMBOL:
            rb_str_concat(ret, rb_str_new2(rb_id2name(SYM2ID(key))));
            break;
        default:    /* call #to_str or raise error */
            rb_str_concat(ret, StringValue(key));
            break;
    }
    return ret;
}

    static int
cb_extract_keys_i(VALUE key, VALUE value, VALUE arg)
{
    struct key_traits_st *traits = (struct key_traits_st *)arg;
    key = unify_key(traits->bucket, key, 1);
    rb_ary_push(traits->keys_ary, key);
    traits->keys[traits->nkeys] = RSTRING_PTR(key);
    traits->lens[traits->nkeys] = RSTRING_LEN(key);
    traits->ttls[traits->nkeys] = NUM2ULONG(value);
    traits->nkeys++;
    return ST_CONTINUE;
}

    static long
cb_args_scan_keys(long argc, VALUE argv, struct key_traits_st *traits)
{
    VALUE key, *keys_ptr, opts, ttl, ext;
    long nn = 0, ii;
    time_t exp;

    traits->keys_ary = rb_ary_new();
    traits->quiet = traits->bucket->quiet;
    traits->mgat = 0;
    traits->assemble_hash = 0;
    traits->lock = 0;
    traits->replica = 0;

    if (argc > 0) {
        /* keys with custom options */
        opts = RARRAY_PTR(argv)[argc-1];
        exp = traits->bucket->default_ttl;
        ext = Qfalse;
        if (argc > 1 && TYPE(opts) == T_HASH) {
            (void)rb_ary_pop(argv);
            traits->replica = RTEST(rb_hash_aref(opts, sym_replica));
            if (RTEST(rb_funcall(opts, id_has_key_p, 1, sym_quiet))) {
                traits->quiet = RTEST(rb_hash_aref(opts, sym_quiet));
            }
            traits->force_format = rb_hash_aref(opts, sym_format);
            if (traits->force_format != Qnil) {
                Check_Type(traits->force_format, T_SYMBOL);
            }
            ext = rb_hash_aref(opts, sym_extended);
            ttl = rb_hash_aref(opts, sym_ttl);
            if (ttl != Qnil) {
                traits->explicit_ttl = 1;
                exp = NUM2ULONG(ttl);
            }
            /* boolean or number of seconds to lock */
            ttl = rb_hash_aref(opts, sym_lock);
            if (ttl != Qnil) {
                traits->lock = RTEST(ttl);
                exp = 0;    /* use server default expiration */
                if (TYPE(ttl) == T_FIXNUM) {
                    exp = NUM2ULONG(ttl);
                }
            }
            traits->assemble_hash = RTEST(rb_hash_aref(opts, sym_assemble_hash));
        }
        nn = RARRAY_LEN(argv);
        if (nn == 1 && TYPE(RARRAY_PTR(argv)[0]) == T_ARRAY) {
            argv = RARRAY_PTR(argv)[0];
            nn = RARRAY_LEN(argv);
            traits->is_array = 1;
        }
        if (nn < 1) {
            rb_raise(rb_eArgError, "must be at least one key");
        }
        keys_ptr = RARRAY_PTR(argv);
        traits->extended = RTEST(ext) ? 1 : 0;
        if (nn == 1 && TYPE(keys_ptr[0]) == T_HASH) {
            /* hash of key-ttl pairs */
            nn = RHASH_SIZE(keys_ptr[0]);
            traits->keys = xcalloc(nn, sizeof(char *));
            traits->lens = xcalloc(nn, sizeof(size_t));
            traits->explicit_ttl = 1;
            traits->mgat = 1;
            traits->ttls = xcalloc(nn, sizeof(time_t));
            rb_hash_foreach(keys_ptr[0], cb_extract_keys_i, (VALUE)traits);
        } else {
            /* the list of keys */
            traits->nkeys = nn;
            traits->keys = xcalloc(nn, sizeof(char *));
            traits->lens = xcalloc(nn, sizeof(size_t));
            traits->ttls = xcalloc(nn, sizeof(time_t));
            for (ii = 0; ii < nn; ii++) {
                key = unify_key(traits->bucket, keys_ptr[ii], 1);
                rb_ary_push(traits->keys_ary, key);
                traits->keys[ii] = RSTRING_PTR(key);
                traits->lens[ii] = RSTRING_LEN(key);
                traits->ttls[ii] = exp;
            }
        }
    }

    return nn;
}

    static void
error_callback(libcouchbase_t handle, libcouchbase_error_t error, const char *errinfo)
{
    struct bucket_st *bucket = (struct bucket_st *)libcouchbase_get_cookie(handle);

    bucket->io->stop_event_loop(bucket->io);
    bucket->exception = cb_check_error(error, errinfo, Qnil);
}

    static void
storage_callback(libcouchbase_t handle, const void *cookie,
        libcouchbase_storage_t operation, libcouchbase_error_t error,
        const void *key, libcouchbase_size_t nkey, libcouchbase_cas_t cas)
{
    struct context_st *ctx = (struct context_st *)cookie;
    struct bucket_st *bucket = ctx->bucket;
    VALUE k, c, *rv = ctx->rv, exc, res;
    ID o;

    ctx->nqueries--;
    k = rb_str_new((const char*)key, nkey);
    strip_key_prefix(bucket, k);
    c = cas > 0 ? ULL2NUM(cas) : Qnil;
    switch(operation) {
        case LIBCOUCHBASE_ADD:
            o = sym_add;
            break;
        case LIBCOUCHBASE_REPLACE:
            o = sym_replace;
            break;
        case LIBCOUCHBASE_SET:
            o = sym_set;
            break;
        case LIBCOUCHBASE_APPEND:
            o = sym_append;
            break;
        case LIBCOUCHBASE_PREPEND:
            o = sym_prepend;
            break;
        default:
            o = Qnil;
    }
    exc = cb_check_error(error, "failed to store value", k);
    if (exc != Qnil) {
        rb_ivar_set(exc, id_iv_cas, c);
        rb_ivar_set(exc, id_iv_operation, o);
        if (NIL_P(ctx->exception)) {
            ctx->exception = exc;
            cb_gc_protect(bucket, ctx->exception);
        }
    }
    if (bucket->async) { /* asynchronous */
        if (ctx->proc != Qnil) {
            res = rb_class_new_instance(0, NULL, cResult);
            rb_ivar_set(res, id_iv_error, exc);
            rb_ivar_set(res, id_iv_key, k);
            rb_ivar_set(res, id_iv_operation, o);
            rb_ivar_set(res, id_iv_cas, c);
            cb_proc_call(ctx->proc, 1, res);
        }
    } else {             /* synchronous */
        *rv = c;
    }

    if (ctx->nqueries == 0) {
        cb_gc_unprotect(bucket, ctx->proc);
    }
    (void)handle;
}

    static void
delete_callback(libcouchbase_t handle, const void *cookie,
        libcouchbase_error_t error, const void *key,
        libcouchbase_size_t nkey)
{
    struct context_st *ctx = (struct context_st *)cookie;
    struct bucket_st *bucket = ctx->bucket;
    VALUE k, *rv = ctx->rv, exc = Qnil, res;

    ctx->nqueries--;
    k = rb_str_new((const char*)key, nkey);
    strip_key_prefix(bucket, k);
    if (error != LIBCOUCHBASE_KEY_ENOENT || !ctx->quiet) {
        exc = cb_check_error(error, "failed to remove value", k);
        if (exc != Qnil) {
            rb_ivar_set(exc, id_iv_operation, sym_delete);
            if (NIL_P(ctx->exception)) {
                ctx->exception = exc;
                cb_gc_protect(bucket, ctx->exception);
            }
        }
    }
    if (bucket->async) {    /* asynchronous */
        if (ctx->proc != Qnil) {
            res = rb_class_new_instance(0, NULL, cResult);
            rb_ivar_set(res, id_iv_error, exc);
            rb_ivar_set(res, id_iv_operation, sym_delete);
            rb_ivar_set(res, id_iv_key, k);
            cb_proc_call(ctx->proc, 1, res);
        }
    } else {                /* synchronous */
        *rv = (error == LIBCOUCHBASE_SUCCESS) ? Qtrue : Qfalse;
    }
    if (ctx->nqueries == 0) {
        cb_gc_unprotect(bucket, ctx->proc);
    }
    (void)handle;
}

    static void
get_callback(libcouchbase_t handle, const void *cookie,
        libcouchbase_error_t error, const void *key,
        libcouchbase_size_t nkey, const void *bytes,
        libcouchbase_size_t nbytes, libcouchbase_uint32_t flags,
        libcouchbase_cas_t cas)
{
    struct context_st *ctx = (struct context_st *)cookie;
    struct bucket_st *bucket = ctx->bucket;
    VALUE k, v, f, c, *rv = ctx->rv, exc = Qnil, res;

    ctx->nqueries--;
    k = rb_str_new((const char*)key, nkey);
    strip_key_prefix(bucket, k);
    if (error != LIBCOUCHBASE_KEY_ENOENT || !ctx->quiet) {
        exc = cb_check_error(error, "failed to get value", k);
        if (exc != Qnil) {
            rb_ivar_set(exc, id_iv_operation, sym_get);
            if (NIL_P(ctx->exception)) {
                ctx->exception = exc;
                cb_gc_protect(bucket, ctx->exception);
            }
        }
    }

    f = ULONG2NUM(flags);
    c = ULL2NUM(cas);
    v = Qnil;
    if (nbytes != 0) {
        v = decode_value(rb_str_new((const char*)bytes, nbytes), flags, ctx->force_format);
        if (v == Qundef) {
            if (ctx->exception != Qnil) {
                cb_gc_unprotect(bucket, ctx->exception);
            }
            ctx->exception = rb_exc_new2(eValueFormatError, "unable to convert value");
            rb_ivar_set(ctx->exception, id_iv_operation, sym_get);
            rb_ivar_set(ctx->exception, id_iv_key, k);
            cb_gc_protect(bucket, ctx->exception);
        }
    } else if (flags_get_format(flags) == sym_plain) {
        v = rb_str_new2("");
    }
    if (bucket->async) { /* asynchronous */
        if (ctx->proc != Qnil) {
            res = rb_class_new_instance(0, NULL, cResult);
            rb_ivar_set(res, id_iv_error, exc);
            rb_ivar_set(res, id_iv_operation, sym_get);
            rb_ivar_set(res, id_iv_key, k);
            rb_ivar_set(res, id_iv_value, v);
            rb_ivar_set(res, id_iv_flags, f);
            rb_ivar_set(res, id_iv_cas, c);
            cb_proc_call(ctx->proc, 1, res);
        }
    } else {                /* synchronous */
        if (NIL_P(exc) && error != LIBCOUCHBASE_KEY_ENOENT) {
            if (ctx->extended) {
                rb_hash_aset(*rv, k, rb_ary_new3(3, v, f, c));
            } else {
                rb_hash_aset(*rv, k, v);
            }
        }
    }

    if (ctx->nqueries == 0) {
        cb_gc_unprotect(bucket, ctx->proc);
    }
    (void)handle;
}

    static void
flush_callback(libcouchbase_t handle, const void* cookie,
        const char* authority, libcouchbase_error_t error)
{
    struct context_st *ctx = (struct context_st *)cookie;
    struct bucket_st *bucket = ctx->bucket;
    VALUE node, success = Qtrue, *rv = ctx->rv, exc, res;

    node = authority ? rb_str_new2(authority) : Qnil;
    exc = cb_check_error(error, "failed to flush bucket", node);
    if (exc != Qnil) {
        rb_ivar_set(exc, id_iv_operation, sym_flush);
        if (NIL_P(ctx->exception)) {
            ctx->exception = exc;
            cb_gc_protect(bucket, ctx->exception);
        }
        success = Qfalse;
    }

    if (authority) {
        if (bucket->async) {    /* asynchronous */
            if (ctx->proc != Qnil) {
                res = rb_class_new_instance(0, NULL, cResult);
                rb_ivar_set(res, id_iv_error, exc);
                rb_ivar_set(res, id_iv_operation, sym_flush);
                rb_ivar_set(res, id_iv_node, node);
                cb_proc_call(ctx->proc, 1, res);
            }
        } else {                /* synchronous */
            if (RTEST(*rv)) {
                /* rewrite status for positive values only */
                *rv = success;
            }
        }
    } else {
        ctx->nqueries--;
        cb_gc_unprotect(bucket, ctx->proc);
    }

    (void)handle;
}

    static void
version_callback(libcouchbase_t handle, const void *cookie,
        const char *authority, libcouchbase_error_t error,
        const char *bytes, libcouchbase_size_t nbytes)
{
    struct context_st *ctx = (struct context_st *)cookie;
    struct bucket_st *bucket = ctx->bucket;
    VALUE node, v, *rv = ctx->rv, exc, res;

    node = authority ? rb_str_new2(authority) : Qnil;
    exc = cb_check_error(error, "failed to get version", node);
    if (exc != Qnil) {
        rb_ivar_set(exc, id_iv_operation, sym_flush);
        if (NIL_P(ctx->exception)) {
            ctx->exception = exc;
            cb_gc_protect(bucket, ctx->exception);
        }
    }

    if (authority) {
        v = rb_str_new((const char*)bytes, nbytes);
        if (bucket->async) {    /* asynchronous */
            if (ctx->proc != Qnil) {
                res = rb_class_new_instance(0, NULL, cResult);
                rb_ivar_set(res, id_iv_error, exc);
                rb_ivar_set(res, id_iv_operation, sym_version);
                rb_ivar_set(res, id_iv_node, node);
                rb_ivar_set(res, id_iv_value, v);
                cb_proc_call(ctx->proc, 1, res);
            }
        } else {                /* synchronous */
            if (NIL_P(exc)) {
                rb_hash_aset(*rv, node, v);
            }
        }
    } else {
        ctx->nqueries--;
        cb_gc_unprotect(bucket, ctx->proc);
    }

    (void)handle;
}

    static void
stat_callback(libcouchbase_t handle, const void* cookie,
        const char* authority, libcouchbase_error_t error, const void* key,
        libcouchbase_size_t nkey, const void* bytes,
        libcouchbase_size_t nbytes)
{
    struct context_st *ctx = (struct context_st *)cookie;
    struct bucket_st *bucket = ctx->bucket;
    VALUE stats, node, k, v, *rv = ctx->rv, exc = Qnil, res;

    node = authority ? rb_str_new2(authority) : Qnil;
    exc = cb_check_error(error, "failed to fetch stats", node);
    if (exc != Qnil) {
        rb_ivar_set(exc, id_iv_operation, sym_stats);
        if (NIL_P(ctx->exception)) {
            ctx->exception = exc;
            cb_gc_protect(bucket, ctx->exception);
        }
    }
    if (authority) {
        k = rb_str_new((const char*)key, nkey);
        v = rb_str_new((const char*)bytes, nbytes);
        if (bucket->async) {    /* asynchronous */
            if (ctx->proc != Qnil) {
                res = rb_class_new_instance(0, NULL, cResult);
                rb_ivar_set(res, id_iv_error, exc);
                rb_ivar_set(res, id_iv_operation, sym_stats);
                rb_ivar_set(res, id_iv_node, node);
                rb_ivar_set(res, id_iv_key, k);
                rb_ivar_set(res, id_iv_value, v);
                cb_proc_call(ctx->proc, 1, res);
            }
        } else {                /* synchronous */
            if (NIL_P(exc)) {
                stats = rb_hash_aref(*rv, k);
                if (NIL_P(stats)) {
                    stats = rb_hash_new();
                    rb_hash_aset(*rv, k, stats);
                }
                rb_hash_aset(stats, node, v);
            }
        }
    } else {
        ctx->nqueries--;
        cb_gc_unprotect(bucket, ctx->proc);
    }
    (void)handle;
}

    static void
touch_callback(libcouchbase_t handle, const void *cookie,
        libcouchbase_error_t error, const void *key,
        libcouchbase_size_t nkey)
{
    struct context_st *ctx = (struct context_st *)cookie;
    struct bucket_st *bucket = ctx->bucket;
    VALUE k, success, *rv = ctx->rv, exc = Qnil, res;

    ctx->nqueries--;
    k = rb_str_new((const char*)key, nkey);
    strip_key_prefix(bucket, k);
    if (error != LIBCOUCHBASE_KEY_ENOENT || !ctx->quiet) {
        exc = cb_check_error(error, "failed to touch value", k);
        if (exc != Qnil) {
            rb_ivar_set(exc, id_iv_operation, sym_touch);
            if (NIL_P(ctx->exception)) {
                ctx->exception = exc;
                cb_gc_protect(bucket, ctx->exception);
            }
        }
    }

    if (bucket->async) {    /* asynchronous */
        if (ctx->proc != Qnil) {
            res = rb_class_new_instance(0, NULL, cResult);
            rb_ivar_set(res, id_iv_error, exc);
            rb_ivar_set(res, id_iv_operation, sym_touch);
            rb_ivar_set(res, id_iv_key, k);
            cb_proc_call(ctx->proc, 1, res);
        }
    } else {                /* synchronous */
        if (NIL_P(exc)) {
            success = (error == LIBCOUCHBASE_KEY_ENOENT) ? Qfalse : Qtrue;
            rb_hash_aset(*rv, k, success);
        }
    }
    if (ctx->nqueries == 0) {
        cb_gc_unprotect(bucket, ctx->proc);
    }
    (void)handle;
}

    static void
arithmetic_callback(libcouchbase_t handle, const void *cookie,
        libcouchbase_error_t error, const void *key,
        libcouchbase_size_t nkey, libcouchbase_uint64_t value,
        libcouchbase_cas_t cas)
{
    struct context_st *ctx = (struct context_st *)cookie;
    struct bucket_st *bucket = ctx->bucket;
    VALUE c, k, v, *rv = ctx->rv, exc, res;
    ID o;

    ctx->nqueries--;
    k = rb_str_new((const char*)key, nkey);
    strip_key_prefix(bucket, k);
    c = cas > 0 ? ULL2NUM(cas) : Qnil;
    o = ctx->arithm > 0 ? sym_increment : sym_decrement;
    exc = cb_check_error(error, "failed to perform arithmetic operation", k);
    if (exc != Qnil) {
        rb_ivar_set(exc, id_iv_cas, c);
        rb_ivar_set(exc, id_iv_operation, o);
        if (bucket->async) {
            if (bucket->on_error_proc != Qnil) {
                cb_proc_call(bucket->on_error_proc, 3, o, k, exc);
            } else {
                if (NIL_P(bucket->exception)) {
                    bucket->exception = exc;
                }
            }
        }
        if (NIL_P(ctx->exception)) {
            ctx->exception = exc;
            cb_gc_protect(bucket, ctx->exception);
        }
    }
    v = ULL2NUM(value);
    if (bucket->async) {    /* asynchronous */
        if (ctx->proc != Qnil) {
            res = rb_class_new_instance(0, NULL, cResult);
            rb_ivar_set(res, id_iv_error, exc);
            rb_ivar_set(res, id_iv_operation, o);
            rb_ivar_set(res, id_iv_key, k);
            rb_ivar_set(res, id_iv_value, v);
            rb_ivar_set(res, id_iv_cas, c);
            cb_proc_call(ctx->proc, 1, res);
        }
    } else {                /* synchronous */
        if (NIL_P(exc)) {
            if (ctx->extended) {
                *rv = rb_ary_new3(2, v, c);
            } else {
                *rv = v;
            }
        }
    }
    if (ctx->nqueries == 0) {
        cb_gc_unprotect(bucket, ctx->proc);
    }
    (void)handle;
}

    static void
couch_complete_callback(libcouchbase_http_request_t request,
        libcouchbase_t handle,
        const void *cookie,
        libcouchbase_error_t error,
        libcouchbase_http_status_t status,
        const char *path,
        libcouchbase_size_t npath,
        const void *bytes,
        libcouchbase_size_t nbytes)
{
    struct context_st *ctx = (struct context_st *)cookie;
    struct bucket_st *bucket = ctx->bucket;
    VALUE *rv = ctx->rv, k, v, res;

    ctx->request->completed = 1;
    k = rb_str_new((const char*)path, npath);
    ctx->exception = cb_check_error_with_status(error,
            "failed to execute couch request", k, status);
    if (ctx->exception != Qnil) {
        cb_gc_protect(bucket, ctx->exception);
    }
    v = nbytes ? rb_str_new((const char*)bytes, nbytes) : Qnil;
    if (ctx->proc != Qnil) {
        if (ctx->extended) {
            res = rb_class_new_instance(0, NULL, cResult);
            rb_ivar_set(res, id_iv_error, ctx->exception);
            rb_ivar_set(res, id_iv_operation, sym_couch_request);
            rb_ivar_set(res, id_iv_key, k);
            rb_ivar_set(res, id_iv_value, v);
            rb_ivar_set(res, id_iv_completed, Qtrue);
        } else {
            res = v;
        }
        cb_proc_call(ctx->proc, 1, res);
    }
    if (!bucket->async && ctx->exception == Qnil) {
        *rv = v;
    }
    (void)handle;
    (void)request;
}

    static void
couch_data_callback(libcouchbase_http_request_t request,
        libcouchbase_t handle,
        const void *cookie,
        libcouchbase_error_t error,
        libcouchbase_http_status_t status,
        const char *path,
        libcouchbase_size_t npath,
        const void *bytes,
        libcouchbase_size_t nbytes)
{
    struct context_st *ctx = (struct context_st *)cookie;
    struct bucket_st *bucket = ctx->bucket;
    VALUE k, v, res;

    k = rb_str_new((const char*)path, npath);
    ctx->exception = cb_check_error_with_status(error,
            "failed to execute couch request", k, status);
    v = nbytes ? rb_str_new((const char*)bytes, nbytes) : Qnil;
    if (ctx->exception != Qnil) {
        cb_gc_protect(bucket, ctx->exception);
        libcouchbase_cancel_http_request(request);
    }
    if (ctx->proc != Qnil) {
        if (ctx->extended) {
            res = rb_class_new_instance(0, NULL, cResult);
            rb_ivar_set(res, id_iv_error, ctx->exception);
            rb_ivar_set(res, id_iv_operation, sym_couch_request);
            rb_ivar_set(res, id_iv_key, k);
            rb_ivar_set(res, id_iv_value, v);
            rb_ivar_set(res, id_iv_completed, Qfalse);
        } else {
            res = v;
        }
        cb_proc_call(ctx->proc, 1, res);
    }
    (void)handle;
}

    static void
observe_callback(libcouchbase_t handle, const void *cookie,
        libcouchbase_error_t error, libcouchbase_observe_t status,
        const void *key, libcouchbase_size_t nkey, libcouchbase_cas_t cas,
        int from_master, libcouchbase_time_t ttp, libcouchbase_time_t ttr)
{
    struct context_st *ctx = (struct context_st *)cookie;
    struct bucket_st *bucket = ctx->bucket;
    VALUE k, res, *rv = ctx->rv;

    if (key) {
        k = rb_str_new((const char*)key, nkey);
        ctx->exception = cb_check_error_with_status(error,
                "failed to execute observe request", k, status);
        if (ctx->exception) {
            cb_gc_protect(bucket, ctx->exception);
        }
        res = rb_class_new_instance(0, NULL, cResult);
        rb_ivar_set(res, id_iv_completed, Qfalse);
        rb_ivar_set(res, id_iv_error, ctx->exception);
        rb_ivar_set(res, id_iv_operation, sym_observe);
        rb_ivar_set(res, id_iv_key, k);
        rb_ivar_set(res, id_iv_cas, ULL2NUM(cas));
        rb_ivar_set(res, id_iv_from_master, from_master ? Qtrue : Qfalse);
        rb_ivar_set(res, id_iv_time_to_persist, ULONG2NUM(ttp));
        rb_ivar_set(res, id_iv_time_to_replicate, ULONG2NUM(ttr));
        switch (status) {
            case LIBCOUCHBASE_OBSERVE_FOUND:
                rb_ivar_set(res, id_iv_status, sym_found);
                break;
            case LIBCOUCHBASE_OBSERVE_PERSISTED:
                rb_ivar_set(res, id_iv_status, sym_persisted);
                break;
            case LIBCOUCHBASE_OBSERVE_NOT_FOUND:
                rb_ivar_set(res, id_iv_status, sym_not_found);
                break;
            default:
                rb_ivar_set(res, id_iv_status, Qnil);
        }
        if (bucket->async) { /* asynchronous */
            if (ctx->proc != Qnil) {
                cb_proc_call(ctx->proc, 1, res);
            }
        } else {             /* synchronous */
            if (NIL_P(ctx->exception)) {
                VALUE stats = rb_hash_aref(*rv, k);
                if (NIL_P(stats)) {
                    stats = rb_ary_new();
                    rb_hash_aset(*rv, k, stats);
                }
                rb_ary_push(stats, res);
            }
        }
    } else {
        if (bucket->async && ctx->proc != Qnil) {
            res = rb_class_new_instance(0, NULL, cResult);
            rb_ivar_set(res, id_iv_completed, Qtrue);
            cb_proc_call(ctx->proc, 1, res);
        }
        ctx->nqueries--;
        cb_gc_unprotect(bucket, ctx->proc);
    }
    (void)handle;
}

    static int
cb_first_value_i(VALUE key, VALUE value, VALUE arg)
{
    VALUE *val = (VALUE *)arg;

    *val = value;
    (void)key;
    return ST_STOP;
}

    void
cb_bucket_free(void *ptr)
{
    struct bucket_st *bucket = ptr;

    if (bucket) {
        if (bucket->handle) {
            libcouchbase_destroy(bucket->handle);
        }
        xfree(bucket->authority);
        xfree(bucket->hostname);
        xfree(bucket->pool);
        xfree(bucket->bucket);
        xfree(bucket->username);
        xfree(bucket->password);
        xfree(bucket->key_prefix);
        xfree(bucket);
    }
}

    void
cb_bucket_mark(void *ptr)
{
    struct bucket_st *bucket = ptr;

    if (bucket) {
        rb_gc_mark(bucket->exception);
        rb_gc_mark(bucket->on_error_proc);
        rb_gc_mark(bucket->key_prefix_val);
        rb_gc_mark(bucket->object_space);
    }
}

    static void
do_scan_connection_options(struct bucket_st *bucket, int argc, VALUE *argv)
{
    VALUE uri, opts, arg;
    size_t len;

    if (rb_scan_args(argc, argv, "02", &uri, &opts) > 0) {
        if (TYPE(uri) == T_HASH && argc == 1) {
            opts = uri;
            uri = Qnil;
        }
        if (uri != Qnil) {
            const char path_re[] = "^(/pools/([A-Za-z0-9_.-]+)(/buckets/([A-Za-z0-9_.-]+))?)?";
            VALUE match, uri_obj, re;

            Check_Type(uri, T_STRING);
            uri_obj = rb_funcall(mURI, id_parse, 1, uri);

            arg = rb_funcall(uri_obj, id_scheme, 0);
            if (arg == Qnil || rb_str_cmp(arg, rb_str_new2("http"))) {
                rb_raise(rb_eArgError, "invalid URI: invalid scheme");
            }

            arg = rb_funcall(uri_obj, id_user, 0);
            if (arg != Qnil) {
                xfree(bucket->username);
                bucket->username = strdup(RSTRING_PTR(arg));
                if (bucket->username == NULL) {
                    rb_raise(eClientNoMemoryError, "failed to allocate memory for Bucket");
                }
            }

            arg = rb_funcall(uri_obj, id_password, 0);
            if (arg != Qnil) {
                xfree(bucket->password);
                bucket->password = strdup(RSTRING_PTR(arg));
                if (bucket->password == NULL) {
                    rb_raise(eClientNoMemoryError, "failed to allocate memory for Bucket");
                }
            }
            arg = rb_funcall(uri_obj, id_host, 0);
            if (arg != Qnil) {
                xfree(bucket->hostname);
                bucket->hostname = strdup(RSTRING_PTR(arg));
                if (bucket->hostname == NULL) {
                    rb_raise(eClientNoMemoryError, "failed to allocate memory for Bucket");
                }
            } else {
                rb_raise(rb_eArgError, "invalid URI: missing hostname");
            }

            arg = rb_funcall(uri_obj, id_port, 0);
            bucket->port = NIL_P(arg) ? 8091 : (uint16_t)NUM2UINT(arg);

            arg = rb_funcall(uri_obj, id_path, 0);
            re = rb_reg_new(path_re, sizeof(path_re) - 1, 0);
            match = rb_funcall(re, id_match, 1, arg);
            arg = rb_reg_nth_match(2, match);
            xfree(bucket->pool);
            bucket->pool = strdup(NIL_P(arg) ? "default" : RSTRING_PTR(arg));
            arg = rb_reg_nth_match(4, match);
            xfree(bucket->bucket);
            bucket->bucket = strdup(NIL_P(arg) ? "default" : RSTRING_PTR(arg));
        }
        if (TYPE(opts) == T_HASH) {
            arg = rb_hash_aref(opts, sym_node_list);
            if (arg != Qnil) {
                VALUE tt;
                xfree(bucket->node_list);
                Check_Type(arg, T_ARRAY);
                tt = rb_ary_join(arg, rb_str_new2(";"));
                bucket->node_list = strdup(StringValueCStr(tt));
            }
            arg = rb_hash_aref(opts, sym_hostname);
            if (arg != Qnil) {
                xfree(bucket->hostname);
                bucket->hostname = strdup(StringValueCStr(arg));
            }
            arg = rb_hash_aref(opts, sym_pool);
            if (arg != Qnil) {
                xfree(bucket->pool);
                bucket->pool = strdup(StringValueCStr(arg));
            }
            arg = rb_hash_aref(opts, sym_bucket);
            if (arg != Qnil) {
                xfree(bucket->bucket);
                bucket->bucket = strdup(StringValueCStr(arg));
            }
            arg = rb_hash_aref(opts, sym_username);
            if (arg != Qnil) {
                xfree(bucket->username);
                bucket->username = strdup(StringValueCStr(arg));
            }
            arg = rb_hash_aref(opts, sym_password);
            if (arg != Qnil) {
                xfree(bucket->password);
                bucket->password = strdup(StringValueCStr(arg));
            }
            arg = rb_hash_aref(opts, sym_port);
            if (arg != Qnil) {
                bucket->port = (uint16_t)NUM2UINT(arg);
            }
            if (RTEST(rb_funcall(opts, id_has_key_p, 1, sym_quiet))) {
                bucket->quiet = RTEST(rb_hash_aref(opts, sym_quiet));
            }
            arg = rb_hash_aref(opts, sym_timeout);
            if (arg != Qnil) {
                bucket->timeout = (uint32_t)NUM2ULONG(arg);
            }
            arg = rb_hash_aref(opts, sym_default_ttl);
            if (arg != Qnil) {
                bucket->default_ttl = (uint32_t)NUM2ULONG(arg);
            }
            arg = rb_hash_aref(opts, sym_default_observe_timeout);
            if (arg != Qnil) {
                bucket->default_observe_timeout = (uint32_t)NUM2ULONG(arg);
            }
            arg = rb_hash_aref(opts, sym_default_flags);
            if (arg != Qnil) {
                bucket->default_flags = (uint32_t)NUM2ULONG(arg);
            }
            arg = rb_hash_aref(opts, sym_default_format);
            if (arg != Qnil) {
                if (TYPE(arg) == T_FIXNUM) {
                    switch (FIX2INT(arg)) {
                        case FMT_DOCUMENT:
                            arg = sym_document;
                            break;
                        case FMT_MARSHAL:
                            arg = sym_marshal;
                            break;
                        case FMT_PLAIN:
                            arg = sym_plain;
                            break;
                    }
                }
                if (arg == sym_document || arg == sym_marshal || arg == sym_plain) {
                    bucket->default_format = arg;
                    bucket->default_flags = flags_set_format(bucket->default_flags, arg);
                }
            }
            arg = rb_hash_aref(opts, sym_environment);
            if (arg != Qnil) {
                if (arg == sym_production || arg == sym_development) {
                    bucket->environment = arg;
                }
            }
            arg = rb_hash_aref(opts, sym_key_prefix);
            if (arg != Qnil) {
                xfree(bucket->key_prefix);
                bucket->key_prefix = strdup(StringValueCStr(arg));
                bucket->key_prefix_val = rb_str_new2(bucket->key_prefix);
            }
        } else {
            opts = Qnil;
        }
    }
    if (bucket->password && bucket->username == NULL) {
        bucket->username = strdup(bucket->bucket);
    }
    len = strlen(bucket->hostname) + 10;
    if (bucket->default_observe_timeout < 2) {
        rb_raise(rb_eArgError, "default_observe_timeout is too low");
    }
    xfree(bucket->authority);
    bucket->authority = xcalloc(len, sizeof(char));
    if (bucket->authority == NULL) {
        rb_raise(eClientNoMemoryError, "failed to allocate memory for Bucket");
    }
    snprintf(bucket->authority, len, "%s:%u", bucket->hostname, bucket->port);
}

    static void
do_connect(struct bucket_st *bucket)
{
    libcouchbase_error_t err;

    if (bucket->handle) {
        libcouchbase_destroy(bucket->handle);
        bucket->handle = NULL;
        bucket->io = NULL;
    }
    bucket->io = libcouchbase_create_io_ops(LIBCOUCHBASE_IO_OPS_DEFAULT, NULL, &err);
    if (bucket->io == NULL && err != LIBCOUCHBASE_SUCCESS) {
        rb_exc_raise(cb_check_error(err, "failed to create IO instance", Qnil));
    }
    bucket->handle = libcouchbase_create(bucket->node_list ? bucket-> node_list : bucket->authority,
            bucket->username, bucket->password, bucket->bucket, bucket->io);
    if (bucket->handle == NULL) {
        rb_raise(eLibcouchbaseError, "failed to create libcouchbase instance");
    }
    libcouchbase_set_cookie(bucket->handle, bucket);
    (void)libcouchbase_set_error_callback(bucket->handle, error_callback);
    (void)libcouchbase_set_storage_callback(bucket->handle, storage_callback);
    (void)libcouchbase_set_get_callback(bucket->handle, get_callback);
    (void)libcouchbase_set_touch_callback(bucket->handle, touch_callback);
    (void)libcouchbase_set_remove_callback(bucket->handle, delete_callback);
    (void)libcouchbase_set_stat_callback(bucket->handle, stat_callback);
    (void)libcouchbase_set_flush_callback(bucket->handle, flush_callback);
    (void)libcouchbase_set_arithmetic_callback(bucket->handle, arithmetic_callback);
    (void)libcouchbase_set_version_callback(bucket->handle, version_callback);
    (void)libcouchbase_set_couch_complete_callback(bucket->handle, couch_complete_callback);
    (void)libcouchbase_set_couch_data_callback(bucket->handle, couch_data_callback);
    (void)libcouchbase_set_observe_callback(bucket->handle, observe_callback);

    if (bucket->timeout > 0) {
        libcouchbase_set_timeout(bucket->handle, bucket->timeout);
    } else {
        bucket->timeout = libcouchbase_get_timeout(bucket->handle);
    }
    err = libcouchbase_connect(bucket->handle);
    if (err != LIBCOUCHBASE_SUCCESS) {
        libcouchbase_destroy(bucket->handle);
        bucket->handle = NULL;
        bucket->io = NULL;
        rb_exc_raise(cb_check_error(err, "failed to connect libcouchbase instance to server", Qnil));
    }
    bucket->exception = Qnil;
    libcouchbase_wait(bucket->handle);
    if (bucket->exception != Qnil) {
        libcouchbase_destroy(bucket->handle);
        bucket->handle = NULL;
        bucket->io = NULL;
        rb_exc_raise(bucket->exception);
    }
}

    static VALUE
cb_bucket_alloc(VALUE klass)
{
    VALUE obj;
    struct bucket_st *bucket;

    /* allocate new bucket struct and set it to zero */
    obj = Data_Make_Struct(klass, struct bucket_st, cb_bucket_mark, cb_bucket_free,
            bucket);
    return obj;
}

/*
 * Initialize new Bucket.
 *
 * @since 1.0.0
 *
 * @overload initialize(url, options = {})
 *   Initialize bucket using URI of the cluster and options. It is possible
 *   to override some parts of URI using the options keys (e.g. :host or
 *   :port)
 *
 *   @param [String] url The full URL of management API of the cluster.
 *   @param [Hash] options The options for connection. See options definition
 *     below.
 *
 * @overload initialize(options = {})
 *   Initialize bucket using options only.
 *
 *   @param [Hash] options The options for operation for connection
 *   @option options [Array] :node_list (nil) the list of nodes to connect
 *     to. If specified it takes precedence over +:host+ option. The list
 *     must be array of strings in form of host names or host names with
 *     ports (in first case port 8091 will be used, see examples).
 *   @option options [String] :host ("localhost") the hostname or IP address
 *     of the node
 *   @option options [Fixnum] :port (8091) the port of the managemenent API
 *   @option options [String] :pool ("default") the pool name
 *   @option options [String] :bucket ("default") the bucket name
 *   @option options [Fixnum] :default_ttl (0) the TTL used by default during
 *     storing key-value pairs.
 *   @option options [Fixnum] :default_flags (0) the default flags.
 *   @option options [Symbol] :default_format (:document) the format, which
 *     will be used for values by default. Note that changing format will
 *     amend flags. (see {Bucket#default_format})
 *   @option options [String] :username (nil) the user name to connect to the
 *     cluster. Used to authenticate on management API. The username could
 *     be skipped for protected buckets, the bucket name will be used
 *     instead.
 *   @option options [String] :password (nil) the password of the user.
 *   @option options [true, false] :quiet (false) the flag controlling if raising
 *     exception when the client executes operations on unexising keys. If it
 *     is +true+ it will raise {Couchbase::Error::NotFound} exceptions. The
 *     default behaviour is to return +nil+ value silently (might be useful in
 *     Rails cache).
 *   @option options [Symbol] :environment (:production) the mode of the
 *     connection. Currently it influences only on design documents set. If
 *     the environment is +:development+, you will able to get design
 *     documents with 'dev_' prefix, otherwise (in +:production+ mode) the
 *     library will hide them from you.
 *   @option options [String] :key_prefix (nil) the prefix string which will
 *     be prepended to each key before sending out, and sripped before
 *     returning back to the application.
 *   @option options [Fixnum] :timeout (2500000) the timeout for IO
 *     operations (in microseconds)
 *
 * @example Initialize connection using default options
 *   Couchbase.new
 *
 * @example Select custom bucket
 *   Couchbase.new(:bucket => 'foo')
 *   Couchbase.new('http://localhost:8091/pools/default/buckets/foo')
 *
 * @example Connect to protected bucket
 *   Couchbase.new(:bucket => 'protected', :username => 'protected', :password => 'secret')
 *   Couchbase.new('http://localhost:8091/pools/default/buckets/protected',
 *                 :username => 'protected', :password => 'secret')
 *
 * @example Use list of nodes, in case some nodes might be dead
 *   Couchbase.new(:node_list => ['example.com:8091', 'example.org:8091', 'example.net'])
 *
 * @raise [Couchbase::Error::BucketNotFound] if there no such bucket to
 *   connect
 *
 * @raise [Couchbase::Error::Connect] if the socket wasn't accessible
 *   (doesn't accept connections or doesn't respond in time)
 *
 * @return [Bucket]
 */
    static VALUE
cb_bucket_init(int argc, VALUE *argv, VALUE self)
{
    struct bucket_st *bucket = DATA_PTR(self);

    bucket->exception = Qnil;
    bucket->hostname = strdup("localhost");
    bucket->port = 8091;
    bucket->pool = strdup("default");
    bucket->bucket = strdup("default");
    bucket->async = 0;
    bucket->quiet = 0;
    bucket->default_ttl = 0;
    bucket->default_flags = 0;
    bucket->default_format = sym_document;
    bucket->default_observe_timeout = 2500000;
    bucket->on_error_proc = Qnil;
    bucket->timeout = 0;
    bucket->environment = sym_production;
    bucket->key_prefix = NULL;
    bucket->key_prefix_val = Qnil;
    bucket->node_list = NULL;
    bucket->object_space = rb_hash_new();

    do_scan_connection_options(bucket, argc, argv);
    do_connect(bucket);

    return self;
}

/*
 * Initialize copy
 *
 * Initializes copy of the object, used by {Couchbase::Bucket#dup}
 *
 * @param orig [Couchbase::Bucket] the source for copy
 *
 * @return [Couchbase::Bucket]
 */
static VALUE
cb_bucket_init_copy(VALUE copy, VALUE orig)
{
    struct bucket_st *copy_b;
    struct bucket_st *orig_b;

    if (copy == orig)
        return copy;

    if (TYPE(orig) != T_DATA || TYPE(copy) != T_DATA ||
            RDATA(orig)->dfree != (RUBY_DATA_FUNC)cb_bucket_free) {
        rb_raise(rb_eTypeError, "wrong argument type");
    }

    copy_b = DATA_PTR(copy);
    orig_b = DATA_PTR(orig);

    copy_b->port = orig_b->port;
    copy_b->authority = strdup(orig_b->authority);
    copy_b->hostname = strdup(orig_b->hostname);
    copy_b->pool = strdup(orig_b->pool);
    copy_b->bucket = strdup(orig_b->bucket);
    if (orig_b->username) {
        copy_b->username = strdup(orig_b->username);
    }
    if (orig_b->password) {
        copy_b->password = strdup(orig_b->password);
    }
    if (orig_b->key_prefix) {
        copy_b->key_prefix = strdup(orig_b->key_prefix);
    }
    copy_b->async = orig_b->async;
    copy_b->quiet = orig_b->quiet;
    copy_b->default_format = orig_b->default_format;
    copy_b->default_flags = orig_b->default_flags;
    copy_b->default_ttl = orig_b->default_ttl;
    copy_b->environment = orig_b->environment;
    copy_b->timeout = orig_b->timeout;
    copy_b->exception = Qnil;
    if (orig_b->on_error_proc != Qnil) {
        copy_b->on_error_proc = rb_funcall(orig_b->on_error_proc, id_dup, 0);
    }
    if (orig_b->key_prefix_val != Qnil) {
        copy_b->key_prefix_val = rb_funcall(orig_b->key_prefix_val, id_dup, 0);
    }

    do_connect(copy_b);

    return copy;
}

/*
 * Reconnect the bucket
 *
 * @since 1.1.0
 *
 * Reconnect the bucket using initial configuration with optional
 * redefinition.
 *
 * @overload reconnect(url, options = {})
 *  see {Bucket#initialize Bucket#initialize(url, options)}
 *
 * @overload reconnect(options = {})
 *  see {Bucket#initialize Bucket#initialize(options)}
 *
 *  @example reconnect with current parameters
 *    c.reconnect
 *
 *  @example reconnect the instance to another bucket
 *    c.reconnect(:bucket => 'new')
 */
    static VALUE
cb_bucket_reconnect(int argc, VALUE *argv, VALUE self)
{
    struct bucket_st *bucket = DATA_PTR(self);

    do_scan_connection_options(bucket, argc, argv);
    do_connect(bucket);

    return self;
}

/* Document-method: connected?
 * Check whether the instance connected to the cluster.
 *
 * @since 1.1.0
 *
 * @return [true, false] +true+ if the instance connected to the cluster
 */
    static VALUE
cb_bucket_connected_p(VALUE self)
{
    struct bucket_st *bucket = DATA_PTR(self);
    return bucket->handle ? Qtrue : Qfalse;
}

/* Document-method: async?
 * Check whether the connection asynchronous.
 *
 * @since 1.0.0
 *
 * By default all operations are synchronous and block waiting for
 * results, but you can make them asynchronous and run event loop
 * explicitly. (see {Bucket#run})
 *
 * @example Return value of #get operation depending on async flag
 *   connection = Connection.new
 *   connection.async?      #=> false
 *
 *   connection.run do |conn|
 *     conn.async?          #=> true
 *   end
 *
 * @return [true, false] +true+ if the connection if asynchronous
 *
 * @see Bucket#run
 */
    static VALUE
cb_bucket_async_p(VALUE self)
{
    struct bucket_st *bucket = DATA_PTR(self);
    return bucket->async ? Qtrue : Qfalse;
}

    static VALUE
cb_bucket_quiet_get(VALUE self)
{
    struct bucket_st *bucket = DATA_PTR(self);
    return bucket->quiet ? Qtrue : Qfalse;
}

    static VALUE
cb_bucket_quiet_set(VALUE self, VALUE val)
{
    struct bucket_st *bucket = DATA_PTR(self);
    VALUE new;

    bucket->quiet = RTEST(val);
    new = bucket->quiet ? Qtrue : Qfalse;
    return new;
}

    static VALUE
cb_bucket_default_flags_get(VALUE self)
{
    struct bucket_st *bucket = DATA_PTR(self);
    return ULONG2NUM(bucket->default_flags);
}

    static VALUE
cb_bucket_default_flags_set(VALUE self, VALUE val)
{
    struct bucket_st *bucket = DATA_PTR(self);

    bucket->default_flags = (uint32_t)NUM2ULONG(val);
    bucket->default_format = flags_get_format(bucket->default_flags);
    return val;
}

    static VALUE
cb_bucket_default_format_get(VALUE self)
{
    struct bucket_st *bucket = DATA_PTR(self);
    return bucket->default_format;
}

    static VALUE
cb_bucket_default_format_set(VALUE self, VALUE val)
{
    struct bucket_st *bucket = DATA_PTR(self);

    if (TYPE(val) == T_FIXNUM) {
        switch (FIX2INT(val)) {
            case FMT_DOCUMENT:
                val = sym_document;
                break;
            case FMT_MARSHAL:
                val = sym_marshal;
                break;
            case FMT_PLAIN:
                val = sym_plain;
                break;
        }
    }
    if (val == sym_document || val == sym_marshal || val == sym_plain) {
        bucket->default_format = val;
        bucket->default_flags = flags_set_format(bucket->default_flags, val);
    }

    return val;
}

    static VALUE
cb_bucket_on_error_set(VALUE self, VALUE val)
{
    struct bucket_st *bucket = DATA_PTR(self);

    if (rb_respond_to(val, id_call)) {
        bucket->on_error_proc = val;
    } else {
        bucket->on_error_proc = Qnil;
    }

    return bucket->on_error_proc;
}

    static VALUE
cb_bucket_on_error_get(VALUE self)
{
    struct bucket_st *bucket = DATA_PTR(self);

    if (rb_block_given_p()) {
        return cb_bucket_on_error_set(self, rb_block_proc());
    } else {
        return bucket->on_error_proc;
    }
}

    static VALUE
cb_bucket_timeout_get(VALUE self)
{
    struct bucket_st *bucket = DATA_PTR(self);
    return ULONG2NUM(bucket->timeout);
}

    static VALUE
cb_bucket_timeout_set(VALUE self, VALUE val)
{
    struct bucket_st *bucket = DATA_PTR(self);
    VALUE tmval;

    bucket->timeout = (uint32_t)NUM2ULONG(val);
    libcouchbase_set_timeout(bucket->handle, bucket->timeout);
    tmval = ULONG2NUM(bucket->timeout);

    return tmval;
}

    static VALUE
cb_bucket_key_prefix_get(VALUE self)
{
    struct bucket_st *bucket = DATA_PTR(self);
    return bucket->key_prefix_val;
}

    static VALUE
cb_bucket_key_prefix_set(VALUE self, VALUE val)
{
    struct bucket_st *bucket = DATA_PTR(self);

    bucket->key_prefix = strdup(StringValueCStr(val));
    bucket->key_prefix_val = rb_str_new2(bucket->key_prefix);

    return bucket->key_prefix_val;
}

/* Document-method: hostname
 *
 * @since 1.0.0
 *
 * @return [String] the host name of the management interface (default: "localhost")
 */
    static VALUE
cb_bucket_hostname_get(VALUE self)
{
    struct bucket_st *bucket = DATA_PTR(self);
    if (bucket->handle) {
        xfree(bucket->hostname);
        bucket->hostname = strdup(libcouchbase_get_host(bucket->handle));
        if (bucket->hostname == NULL) {
            rb_raise(eClientNoMemoryError, "failed to allocate memory for Bucket");
        }
    }
    return rb_str_new2(bucket->hostname);
}

/* Document-method: port
 *
 * @since 1.0.0
 *
 * @return [Fixnum] the port number of the management interface (default: 8091)
 */
    static VALUE
cb_bucket_port_get(VALUE self)
{
    struct bucket_st *bucket = DATA_PTR(self);
    if (bucket->handle) {
        bucket->port = atoi(libcouchbase_get_port(bucket->handle));
    }
    return UINT2NUM(bucket->port);
}

/* Document-method: authority
 *
 * @since 1.0.0
 *
 * @return [String] host with port
 */
    static VALUE
cb_bucket_authority_get(VALUE self)
{
    struct bucket_st *bucket = DATA_PTR(self);
    size_t len;

    (void)cb_bucket_hostname_get(self);
    (void)cb_bucket_port_get(self);
    len = strlen(bucket->hostname) + 10;
    bucket->authority = xcalloc(len, sizeof(char));
    if (bucket->authority == NULL) {
        rb_raise(eClientNoMemoryError, "failed to allocate memory for Bucket");
    }
    snprintf(bucket->authority, len, "%s:%u", bucket->hostname, bucket->port);
    return rb_str_new2(bucket->authority);
}

/* Document-method: bucket
 *
 * @since 1.0.0
 *
 * @return [String] the bucket name
 */
    static VALUE
cb_bucket_bucket_get(VALUE self)
{
    struct bucket_st *bucket = DATA_PTR(self);
    return rb_str_new2(bucket->bucket);
}

/* Document-method: pool
 *
 * @since 1.0.0
 *
 * @return [String] the pool name (usually "default")
 */
    static VALUE
cb_bucket_pool_get(VALUE self)
{
    struct bucket_st *bucket = DATA_PTR(self);
    return rb_str_new2(bucket->pool);
}

/* Document-method: username
 *
 * @since 1.0.0
 *
 * @return [String] the username for protected buckets (usually matches
 *   the bucket name)
 */
    static VALUE
cb_bucket_username_get(VALUE self)
{
    struct bucket_st *bucket = DATA_PTR(self);
    return rb_str_new2(bucket->username);
}

/* Document-method: password
 *
 * @since 1.0.0
 *
 * @return [String] the password for protected buckets
 */
    static VALUE
cb_bucket_password_get(VALUE self)
{
    struct bucket_st *bucket = DATA_PTR(self);
    return rb_str_new2(bucket->password);
}

/* Document-method: environment
 *
 * @since 1.2.0
 *
 * @see Bucket#initialize
 *
 * @return [Symbol] the environment (+:development+ or +:production+)
 */
    static VALUE
cb_bucket_environment_get(VALUE self)
{
    struct bucket_st *bucket = DATA_PTR(self);
    return bucket->environment;
}
/* Document-method: num_replicas
 *
 * @since 1.2.0.dp6
 *
 * The numbers of the replicas for each node in the cluster
 *
 * @returns [Fixnum]
 */
    static VALUE
cb_bucket_num_replicas_get(VALUE self)
{
    struct bucket_st *bucket = DATA_PTR(self);
    int32_t nr = libcouchbase_get_num_replicas(bucket->handle);
    if (nr < 0) {
        return Qnil;
    } else {
        return INT2FIX(nr);
    }
}
/* Document-method: default_observe_timeout
 *
 * @since 1.2.0.dp6
 *
 * Get default timeout value for {Bucket#observe_and_wait} operation in
 * microseconds
 *
 * @returns [Fixnum]
 */
    static VALUE
cb_bucket_default_observe_timeout_get(VALUE self)
{
    struct bucket_st *bucket = DATA_PTR(self);
    return INT2FIX(bucket->default_observe_timeout);
}

/* Document-method: default_observe_timeout=
 *
 * @since 1.2.0.dp6
 *
 * Set default timeout value for {Bucket#observe_and_wait} operation in
 * microseconds
 *
 * @returns [Fixnum]
 */
    static VALUE
cb_bucket_default_observe_timeout_set(VALUE self, VALUE val)
{
    struct bucket_st *bucket = DATA_PTR(self);
    bucket->default_observe_timeout = FIX2INT(val);
    return val;
}
/* Document-method: url
 *
 * @since 1.0.0
 *
 * @return [String] the address of the cluster management interface
 */
    static VALUE
cb_bucket_url_get(VALUE self)
{
    struct bucket_st *bucket = DATA_PTR(self);
    VALUE str;

    (void)cb_bucket_authority_get(self);
    str = rb_str_buf_new2("http://");
    rb_str_buf_cat2(str, bucket->authority);
    rb_str_buf_cat2(str, "/pools/");
    rb_str_buf_cat2(str, bucket->pool);
    rb_str_buf_cat2(str, "/buckets/");
    rb_str_buf_cat2(str, bucket->bucket);
    rb_str_buf_cat2(str, "/");
    return str;
}

/*
 * Returns a string containing a human-readable representation of the
 * {Bucket}.
 *
 * @since 1.0.0
 *
 * @return [String]
 */
    static VALUE
cb_bucket_inspect(VALUE self)
{
    VALUE str;
    struct bucket_st *bucket = DATA_PTR(self);
    char buf[200];

    str = rb_str_buf_new2("#<");
    rb_str_buf_cat2(str, rb_obj_classname(self));
    snprintf(buf, 25, ":%p \"", (void *)self);
    (void)cb_bucket_authority_get(self);
    rb_str_buf_cat2(str, buf);
    rb_str_buf_cat2(str, "http://");
    rb_str_buf_cat2(str, bucket->authority);
    rb_str_buf_cat2(str, "/pools/");
    rb_str_buf_cat2(str, bucket->pool);
    rb_str_buf_cat2(str, "/buckets/");
    rb_str_buf_cat2(str, bucket->bucket);
    rb_str_buf_cat2(str, "/");
    snprintf(buf, 150, "\" default_format=:%s, default_flags=0x%x, quiet=%s, connected=%s, timeout=%u",
            rb_id2name(SYM2ID(bucket->default_format)),
            bucket->default_flags,
            bucket->quiet ? "true" : "false",
            bucket->handle ? "true" : "false",
            bucket->timeout);
    rb_str_buf_cat2(str, buf);
    if (bucket->key_prefix) {
        rb_str_buf_cat2(str, ", key_prefix=");
        rb_str_append(str, rb_inspect(bucket->key_prefix_val));
    }
    rb_str_buf_cat2(str, ">");

    return str;
}

/*
 * Delete the specified key
 *
 * @since 1.0.0
 *
 * @overload delete(key, options = {})
 *   @param key [String, Symbol] Key used to reference the value.
 *   @param options [Hash] Options for operation.
 *   @option options [true, false] :quiet (self.quiet) If set to +true+, the
 *     operation won't raise error for missing key, it will return +nil+.
 *     Otherwise it will raise error in synchronous mode. In asynchronous
 *     mode this option ignored.
 *   @option options [Fixnum] :cas The CAS value for an object. This value
 *     created on the server and is guaranteed to be unique for each value of
 *     a given key. This value is used to provide simple optimistic
 *     concurrency control when multiple clients or threads try to
 *     update/delete an item simultaneously.
 *
 *   @raise [Couchbase::Error::Connect] if connection closed (see {Bucket#reconnect})
 *   @raise [ArgumentError] when passing the block in synchronous mode
 *   @raise [Couchbase::Error::KeyExists] on CAS mismatch
 *   @raise [Couchbase::Error::NotFound] if key is missing in verbose mode
 *
 *   @example Delete the key in quiet mode (default)
 *     c.set("foo", "bar")
 *     c.delete("foo")        #=> true
 *     c.delete("foo")        #=> false
 *
 *   @example Delete the key verbosely
 *     c.set("foo", "bar")
 *     c.delete("foo", :quiet => false)   #=> true
 *     c.delete("foo", :quiet => true)    #=> nil (default behaviour)
 *     c.delete("foo", :quiet => false)   #=> will raise Couchbase::Error::NotFound
 *
 *   @example Delete the key with version check
 *     ver = c.set("foo", "bar")          #=> 5992859822302167040
 *     c.delete("foo", :cas => 123456)    #=> will raise Couchbase::Error::KeyExists
 *     c.delete("foo", :cas => ver)       #=> true
 */
    static VALUE
cb_bucket_delete(int argc, VALUE *argv, VALUE self)
{
    struct bucket_st *bucket = DATA_PTR(self);
    struct context_st *ctx;
    VALUE k, c, rv, proc, exc, opts;
    char *key;
    size_t nkey;
    libcouchbase_cas_t cas = 0;
    libcouchbase_error_t err;

    if (bucket->handle == NULL) {
        rb_raise(eConnectError, "closed connection");
    }
    rb_scan_args(argc, argv, "11&", &k, &opts, &proc);
    if (!bucket->async && proc != Qnil) {
        rb_raise(rb_eArgError, "synchronous mode doesn't support callbacks");
    }
    k = unify_key(bucket, k, 1);
    key = RSTRING_PTR(k);
    nkey = RSTRING_LEN(k);
    ctx = xcalloc(1, sizeof(struct context_st));
    ctx->quiet = bucket->quiet;
    if (ctx == NULL) {
        rb_raise(eClientNoMemoryError, "failed to allocate memory for context");
    }
    if (opts != Qnil) {
        if (TYPE(opts) == T_BIGNUM || TYPE(opts) == T_FIXNUM) {
            cas = NUM2ULL(opts);
        } else {
            Check_Type(opts, T_HASH);
            if ((c = rb_hash_aref(opts, sym_cas)) != Qnil) {
                cas = NUM2ULL(c);
            }
            if (RTEST(rb_funcall(opts, id_has_key_p, 1, sym_quiet))) {
                ctx->quiet = RTEST(rb_hash_aref(opts, sym_quiet));
            }
        }
    }
    ctx->proc = proc;
    cb_gc_protect(bucket, ctx->proc);
    rv = rb_ary_new();
    ctx->rv = &rv;
    ctx->bucket = bucket;
    ctx->exception = Qnil;
    ctx->nqueries = 1;
    err = libcouchbase_remove(bucket->handle, (const void *)ctx,
            (const void *)key, nkey, cas);
    exc = cb_check_error(err, "failed to schedule delete request", Qnil);
    if (exc != Qnil) {
        xfree(ctx);
        rb_exc_raise(exc);
    }
    bucket->nbytes += HEADER_SIZE + nkey;
    if (bucket->async) {
        maybe_do_loop(bucket);
        return Qnil;
    } else {
        if (ctx->nqueries > 0) {
            /* we have some operations pending */
            libcouchbase_wait(bucket->handle);
        }
        exc = ctx->exception;
        xfree(ctx);
        if (exc != Qnil) {
            cb_gc_unprotect(bucket, exc);
            rb_exc_raise(exc);
        }
        return rv;
    }
}

    static inline VALUE
cb_bucket_store(libcouchbase_storage_t cmd, int argc, VALUE *argv, VALUE self)
{
    struct bucket_st *bucket = DATA_PTR(self);
    struct context_st *ctx;
    VALUE k, v, arg, opts, rv, proc, exc, fmt;
    char *key, *bytes;
    size_t nkey, nbytes;
    uint32_t flags;
    time_t exp = 0;
    libcouchbase_cas_t cas = 0;
    libcouchbase_error_t err;

    if (bucket->handle == NULL) {
        rb_raise(eConnectError, "closed connection");
    }
    rb_scan_args(argc, argv, "21&", &k, &v, &opts, &proc);
    if (!bucket->async && proc != Qnil) {
        rb_raise(rb_eArgError, "synchronous mode doesn't support callbacks");
    }
    k = unify_key(bucket, k, 1);
    flags = bucket->default_flags;
    if (opts != Qnil) {
        Check_Type(opts, T_HASH);
        arg = rb_hash_aref(opts, sym_flags);
        if (arg != Qnil) {
            flags = (uint32_t)NUM2ULONG(arg);
        }
        arg = rb_hash_aref(opts, sym_ttl);
        if (arg != Qnil) {
            exp = NUM2ULONG(arg);
        }
        arg = rb_hash_aref(opts, sym_cas);
        if (arg != Qnil) {
            cas = NUM2ULL(arg);
        }
        fmt = rb_hash_aref(opts, sym_format);
        if (fmt != Qnil) { /* rewrite format bits */
            flags = flags_set_format(flags, fmt);
        }
    }
    key = RSTRING_PTR(k);
    nkey = RSTRING_LEN(k);
    v = encode_value(v, flags);
    if (v == Qundef) {
        rb_raise(eValueFormatError,
                "unable to convert value for key '%s'", key);
    }
    bytes = RSTRING_PTR(v);
    nbytes = RSTRING_LEN(v);
    ctx = xcalloc(1, sizeof(struct context_st));
    if (ctx == NULL) {
        rb_raise(eClientNoMemoryError, "failed to allocate memory for context");
    }
    rv = Qnil;
    ctx->rv = &rv;
    ctx->bucket = bucket;
    ctx->proc = proc;
    cb_gc_protect(bucket, ctx->proc);
    ctx->exception = Qnil;
    ctx->nqueries = 1;
    err = libcouchbase_store(bucket->handle, (const void *)ctx, cmd,
            (const void *)key, nkey, bytes, nbytes, flags, exp, cas);
    exc = cb_check_error(err, "failed to schedule set request", Qnil);
    if (exc != Qnil) {
        xfree(ctx);
        rb_exc_raise(exc);
    }
    bucket->nbytes += HEADER_SIZE + 8 + nkey + nbytes;
    if (bucket->async) {
        maybe_do_loop(bucket);
        return Qnil;
    } else {
        if (ctx->nqueries > 0) {
            /* we have some operations pending */
            libcouchbase_wait(bucket->handle);
        }
        exc = ctx->exception;
        xfree(ctx);
        if (exc != Qnil) {
            cb_gc_unprotect(bucket, exc);
            rb_exc_raise(exc);
        }
        if (bucket->exception != Qnil) {
            rb_exc_raise(bucket->exception);
        }
        return rv;
    }
}

    static inline VALUE
cb_bucket_arithmetic(int sign, int argc, VALUE *argv, VALUE self)
{
    struct bucket_st *bucket = DATA_PTR(self);
    struct context_st *ctx;
    VALUE k, d, arg, opts, rv, proc, exc;
    char *key;
    size_t nkey;
    time_t exp;
    uint64_t delta = 0, initial = 0;
    int create = 0;
    libcouchbase_error_t err;

    if (bucket->handle == NULL) {
        rb_raise(eConnectError, "closed connection");
    }
    rb_scan_args(argc, argv, "12&", &k, &d, &opts, &proc);
    if (!bucket->async && proc != Qnil) {
        rb_raise(rb_eArgError, "synchronous mode doesn't support callbacks");
    }
    k = unify_key(bucket, k, 1);
    ctx = xcalloc(1, sizeof(struct context_st));
    if (ctx == NULL) {
        rb_raise(eClientNoMemoryError, "failed to allocate memory for context");
    }
    if (argc == 2 && TYPE(d) == T_HASH) {
        opts = d;
        d = Qnil;
    }
    exp = bucket->default_ttl;
    if (opts != Qnil) {
        Check_Type(opts, T_HASH);
        create = RTEST(rb_hash_aref(opts, sym_create));
        ctx->extended = RTEST(rb_hash_aref(opts, sym_extended));
        arg = rb_hash_aref(opts, sym_ttl);
        if (arg != Qnil) {
            exp = NUM2ULONG(arg);
        }
        arg = rb_hash_aref(opts, sym_initial);
        if (arg != Qnil) {
            initial = NUM2ULL(arg);
            create = 1;
        }
        arg = rb_hash_aref(opts, sym_delta);
        if (NIL_P(d) && arg != Qnil) {
            Check_Type(arg, T_FIXNUM);
            d = arg;
        }
    }
    key = RSTRING_PTR(k);
    nkey = RSTRING_LEN(k);
    if (NIL_P(d)) {
        delta = 1 * sign;
    } else {
        delta = NUM2ULL(d) * sign;
    }
    rv = Qnil;
    ctx->rv = &rv;
    ctx->bucket = bucket;
    ctx->proc = proc;
    cb_gc_protect(bucket, ctx->proc);
    ctx->exception = Qnil;
    ctx->arithm = sign;
    ctx->nqueries = 1;
    err = libcouchbase_arithmetic(bucket->handle, (const void *)ctx,
            (const void *)key, nkey, delta, exp, create, initial);
    exc = cb_check_error(err, "failed to schedule arithmetic request", k);
    if (exc != Qnil) {
        xfree(ctx);
        rb_exc_raise(exc);
    }
    bucket->nbytes += HEADER_SIZE + 20 + nkey;
    if (bucket->async) {
        maybe_do_loop(bucket);
        return Qnil;
    } else {
        if (ctx->nqueries > 0) {
            /* we have some operations pending */
            libcouchbase_wait(bucket->handle);
        }
        exc = ctx->exception;
        xfree(ctx);
        if (exc != Qnil) {
            cb_gc_unprotect(bucket, exc);
            rb_exc_raise(exc);
        }
        return rv;
    }
}

/*
 * Increment the value of an existing numeric key
 *
 * @since 1.0.0
 *
 * The increment methods allow you to increase a given stored integer
 * value. These are the incremental equivalent of the decrement operations
 * and work on the same basis; updating the value of a key if it can be
 * parsed to an integer. The update operation occurs on the server and is
 * provided at the protocol level. This simplifies what would otherwise be a
 * two-stage get and set operation.
 *
 * @note that server values stored and transmitted as unsigned numbers,
 *   therefore if you try to store negative number and then increment or
 *   decrement it will cause overflow. (see "Integer overflow" example
 *   below)
 *
 * @overload incr(key, delta = 1, options = {})
 *   @param key [String, Symbol] Key used to reference the value.
 *   @param delta [Fixnum] Integer (up to 64 bits) value to increment
 *   @param options [Hash] Options for operation.
 *   @option options [true, false] :create (false) If set to +true+, it will
 *     initialize the key with zero value and zero flags (use +:initial+
 *     option to set another initial value). Note: it won't increment the
 *     missing value.
 *   @option options [Fixnum] :initial (0) Integer (up to 64 bits) value for
 *     missing key initialization. This option imply +:create+ option is
 *     +true+.
 *   @option options [Fixnum] :ttl (self.default_ttl) Expiry time for key.
 *     Values larger than 30*24*60*60 seconds (30 days) are interpreted as
 *     absolute times (from the epoch). This option ignored for existent
 *     keys.
 *   @option options [true, false] :extended (false) If set to +true+, the
 *     operation will return tuple +[value, cas]+, otherwise (by default) it
 *     returns just value.
 *
 *   @yieldparam ret [Result] the result of operation in asynchronous mode
 *     (valid attributes: +error+, +operation+, +key+, +value+, +cas+).
 *
 *   @return [Fixnum] the actual value of the key.
 *
 *   @raise [Couchbase::Error::NotFound] if key is missing and +:create+
 *     option isn't +true+.
 *
 *   @raise [Couchbase::Error::DeltaBadval] if the key contains non-numeric
 *     value
 *
 *   @raise [Couchbase::Error::Connect] if connection closed (see {Bucket#reconnect})
 *
 *   @raise [ArgumentError] when passing the block in synchronous mode
 *
 *   @example Increment key by one
 *     c.incr("foo")
 *
 *   @example Increment key by 50
 *     c.incr("foo", 50)
 *
 *   @example Increment key by one <b>OR</b> initialize with zero
 *     c.incr("foo", :create => true)   #=> will return old+1 or 0
 *
 *   @example Increment key by one <b>OR</b> initialize with three
 *     c.incr("foo", 50, :initial => 3) #=> will return old+50 or 3
 *
 *   @example Increment key and get its CAS value
 *     val, cas = c.incr("foo", :extended => true)
 *
 *   @example Integer overflow
 *     c.set("foo", -100)
 *     c.get("foo")           #=> -100
 *     c.incr("foo")          #=> 18446744073709551517
 *
 *   @example Asynchronous invocation
 *     c.run do
 *       c.incr("foo") do |ret|
 *         ret.operation   #=> :increment
 *         ret.success?    #=> true
 *         ret.key         #=> "foo"
 *         ret.value
 *         ret.cas
 *       end
 *     end
 *
 */
static VALUE
cb_bucket_incr(int argc, VALUE *argv, VALUE self)
{
    return cb_bucket_arithmetic(+1, argc, argv, self);
}

/*
 * Decrement the value of an existing numeric key
 *
 * @since 1.0.0
 *
 * The decrement methods reduce the value of a given key if the
 * corresponding value can be parsed to an integer value. These operations
 * are provided at a protocol level to eliminate the need to get, update,
 * and reset a simple integer value in the database. It supports the use of
 * an explicit offset value that will be used to reduce the stored value in
 * the database.
 *
 * @note that server values stored and transmitted as unsigned numbers,
 *   therefore if you try to decrement negative or zero key, you will always
 *   get zero.
 *
 * @overload decr(key, delta = 1, options = {})
 *   @param key [String, Symbol] Key used to reference the value.
 *   @param delta [Fixnum] Integer (up to 64 bits) value to decrement
 *   @param options [Hash] Options for operation.
 *   @option options [true, false] :create (false) If set to +true+, it will
 *     initialize the key with zero value and zero flags (use +:initial+
 *     option to set another initial value). Note: it won't decrement the
 *     missing value.
 *   @option options [Fixnum] :initial (0) Integer (up to 64 bits) value for
 *     missing key initialization. This option imply +:create+ option is
 *     +true+.
 *   @option options [Fixnum] :ttl (self.default_ttl) Expiry time for key.
 *     Values larger than 30*24*60*60 seconds (30 days) are interpreted as
 *     absolute times (from the epoch). This option ignored for existent
 *     keys.
 *   @option options [true, false] :extended (false) If set to +true+, the
 *     operation will return tuple +[value, cas]+, otherwise (by default) it
 *     returns just value.
 *
 *   @yieldparam ret [Result] the result of operation in asynchronous mode
 *     (valid attributes: +error+, +operation+, +key+, +value+, +cas+).
 *
 *   @return [Fixnum] the actual value of the key.
 *
 *   @raise [Couchbase::Error::NotFound] if key is missing and +:create+
 *     option isn't +true+.
 *
 *   @raise [Couchbase::Error::DeltaBadval] if the key contains non-numeric
 *     value
 *
 *   @raise [Couchbase::Error::Connect] if connection closed (see {Bucket#reconnect})
 *
 *   @raise [ArgumentError] when passing the block in synchronous mode
 *
 *   @example Decrement key by one
 *     c.decr("foo")
 *
 *   @example Decrement key by 50
 *     c.decr("foo", 50)
 *
 *   @example Decrement key by one <b>OR</b> initialize with zero
 *     c.decr("foo", :create => true)   #=> will return old-1 or 0
 *
 *   @example Decrement key by one <b>OR</b> initialize with three
 *     c.decr("foo", 50, :initial => 3) #=> will return old-50 or 3
 *
 *   @example Decrement key and get its CAS value
 *     val, cas = c.decr("foo", :extended => true)
 *
 *   @example Decrementing zero
 *     c.set("foo", 0)
 *     c.decrement("foo", 100500)   #=> 0
 *
 *   @example Decrementing negative value
 *     c.set("foo", -100)
 *     c.decrement("foo", 100500)   #=> 0
 *
 *   @example Asynchronous invocation
 *     c.run do
 *       c.decr("foo") do |ret|
 *         ret.operation   #=> :decrement
 *         ret.success?    #=> true
 *         ret.key         #=> "foo"
 *         ret.value
 *         ret.cas
 *       end
 *     end
 *
 */
static VALUE
cb_bucket_decr(int argc, VALUE *argv, VALUE self)
{
    return cb_bucket_arithmetic(-1, argc, argv, self);
}

/*
 * Observe key state
 *
 * @since 1.2.0.dp6
 *
 * @overload observe(*keys, options = {})
 *   @param keys [String, Symbol, Array] One or several keys to fetch
 *   @param options [Hash] Options for operation.
 *
 *   @yieldparam ret [Result] the result of operation in asynchronous mode
 *     (valid attributes: +error+, +status+, +operation+, +key+, +cas+,
 *     +from_master+, +time_to_persist+, +time_to_replicate+).
 *
 *   @return [Hash<String, Array<Result>>, Array<Result>] the state of the
 *     keys on all nodes. If the +keys+ argument was String or Symbol, this
 *     method will return just array of results (result per each node),
 *     otherwise it will return hash map.
 *
 *   @example Observe single key
 *     c.observe("foo")
 *     #=> [#<Couchbase::Result:0x00000001650df0 ....>, ...]
 *
 *   @example Observe multiple keys
 *     keys = ["foo", "bar"]
 *     stats = c.observe(keys)
 *     stats.size   #=> 2
 *     stats["foo"] #=> #<Couchbase::Result:0x00000001650df0 ....>
 */
    static VALUE
cb_bucket_observe(int argc, VALUE *argv, VALUE self)
{
    struct bucket_st *bucket = DATA_PTR(self);
    struct context_st *ctx;
    VALUE args, rv, proc, exc;
    size_t ii, ll = 0, nn;
    libcouchbase_error_t err = LIBCOUCHBASE_SUCCESS;
    struct key_traits_st *traits;
    int is_array;

    if (bucket->handle == NULL) {
        rb_raise(eConnectError, "closed connection");
    }
    rb_scan_args(argc, argv, "0*&", &args, &proc);
    if (!bucket->async && proc != Qnil) {
        rb_raise(rb_eArgError, "synchronous mode doesn't support callbacks");
    }
    traits = xcalloc(1, sizeof(struct key_traits_st));
    traits->bucket = bucket;
    is_array = traits->is_array;
    nn = cb_args_scan_keys(RARRAY_LEN(args), args, traits);
    ctx = xcalloc(1, sizeof(struct context_st));
    if (ctx == NULL) {
        rb_raise(eClientNoMemoryError, "failed to allocate memory for context");
    }
    ctx->proc = proc;
    cb_gc_protect(bucket, ctx->proc);
    ctx->bucket = bucket;
    rv = rb_hash_new();
    ctx->rv = &rv;
    ctx->exception = Qnil;
    ctx->nqueries = traits->nkeys;
    err = libcouchbase_observe(bucket->handle, (const void *)ctx,
            traits->nkeys, (const void * const *)traits->keys, traits->lens);
    if (err == LIBCOUCHBASE_SUCCESS) {
        for (ii = 0; ii < traits->nkeys; ++ii) {
            ll += traits->lens[ii];
        }
    }
    xfree(traits->keys);
    xfree(traits->lens);
    xfree(traits->ttls);
    xfree(traits);
    exc = cb_check_error(err, "failed to schedule observe request", Qnil);
    if (exc != Qnil) {
        xfree(ctx);
        rb_exc_raise(exc);
    }
    bucket->nbytes += HEADER_SIZE + 4 + ll;
    if (bucket->async) {
        maybe_do_loop(bucket);
        return Qnil;
    } else {
        if (ctx->nqueries > 0) {
            /* we have some operations pending */
            libcouchbase_wait(bucket->handle);
        }
        exc = ctx->exception;
        xfree(ctx);
        if (exc != Qnil) {
            cb_gc_unprotect(bucket, exc);
            rb_exc_raise(exc);
        }
        if (bucket->exception != Qnil) {
            rb_exc_raise(bucket->exception);
        }
        if (nn > 1 || is_array) {
            return rv;  /* return as a hash {key => {}, ...} */
        } else {
            VALUE vv = Qnil;
            rb_hash_foreach(rv, cb_first_value_i, (VALUE)&vv);
            return vv;  /* return first value */
        }
    }
}

/*
 * Obtain an object stored in Couchbase by given key.
 *
 * @since 1.0.0
 *
 * @see http://couchbase.com/docs/couchbase-manual-2.0/couchbase-architecture-apis-memcached-protocol-additions.html#couchbase-architecture-apis-memcached-protocol-additions-getl
 *
 * @overload get(*keys, options = {})
 *   @param keys [String, Symbol, Array] One or several keys to fetch
 *   @param options [Hash] Options for operation.
 *   @option options [true, false] :extended (false) If set to +true+, the
 *     operation will return tuple +[value, flags, cas]+, otherwise (by
 *     default) it returns just value.
 *   @option options [Fixnum] :ttl (self.default_ttl) Expiry time for key.
 *     Values larger than 30*24*60*60 seconds (30 days) are interpreted as
 *     absolute times (from the epoch).
 *   @option options [true, false] :quiet (self.quiet) If set to +true+, the
 *     operation won't raise error for missing key, it will return +nil+.
 *     Otherwise it will raise error in synchronous mode. In asynchronous
 *     mode this option ignored.
 *   @option options [Symbol] :format (nil) Explicitly choose the decoder
 *     for this key (+:plain+, +:document+, +:marshal+). See
 *     {Bucket#default_format}.
 *   @option options [Fixnum, Boolean] :lock Lock the keys for time span.
 *     If this parameter is +true+ the key(s) will be locked for default
 *     timeout. Also you can use number to setup your own timeout in
 *     seconds. If it will be lower that zero or exceed the maximum, the
 *     server will use default value. You can determine actual default and
 *     maximum values calling {Bucket#stats} without arguments and
 *     inspecting keys  "ep_getl_default_timeout" and "ep_getl_max_timeout"
 *     correspondingly. See overloaded hash syntax to specify custom timeout
 *     per each key.
 *   @option options [true, false] :assemble_hash (false) Assemble Hash for
 *     results. Hash assembled automatically if +:extended+ option is true
 *     or in case of "get and touch" multimple keys.
 *   @option options [true, false] :replica (false) Read key from replica
 *     node. Options +:ttl+ and +:lock+ are not compatible with +:replica+.
 *
 *   @yieldparam ret [Result] the result of operation in asynchronous mode
 *     (valid attributes: +error+, +operation+, +key+, +value+, +flags+,
 *     +cas+).
 *
 *   @return [Object, Array, Hash] the value(s) (or tuples in extended mode)
 *     assiciated with the key.
 *
 *   @raise [Couchbase::Error::NotFound] if the key is missing in the
 *     bucket.
 *
 *   @raise [Couchbase::Error::Connect] if connection closed (see {Bucket#reconnect})
 *
 *   @raise [ArgumentError] when passing the block in synchronous mode
 *
 *   @example Get single value in quite mode (the default)
 *     c.get("foo")     #=> the associated value or nil
 *
 *   @example Use alternative hash-like syntax
 *     c["foo"]         #=> the associated value or nil
 *
 *   @example Get single value in verbose mode
 *     c.get("missing-foo", :quiet => false)  #=> raises Couchbase::NotFound
 *     c.get("missing-foo", :quiet => true)   #=> returns nil
 *
 *   @example Get and touch single value. The key won't be accessible after 10 seconds
 *     c.get("foo", :ttl => 10)
 *
 *   @example Extended get
 *     val, flags, cas = c.get("foo", :extended => true)
 *
 *   @example Get multiple keys
 *     c.get("foo", "bar", "baz")   #=> [val1, val2, val3]
 *
 *   @example Get multiple keys with assembing result into the Hash
 *     c.get("foo", "bar", "baz", :assemble_hash => true)
 *     #=> {"foo" => val1, "bar" => val2, "baz" => val3}
 *
 *   @example Extended get multiple keys
 *     c.get("foo", "bar", :extended => true)
 *     #=> {"foo" => [val1, flags1, cas1], "bar" => [val2, flags2, cas2]}
 *
 *   @example Asynchronous get
 *     c.run do
 *       c.get("foo", "bar", "baz") do |res|
 *         ret.operation   #=> :get
 *         ret.success?    #=> true
 *         ret.key         #=> "foo", "bar" or "baz" in separate calls
 *         ret.value
 *         ret.flags
 *         ret.cas
 *       end
 *     end
 *
 *   @example Get and lock key using default timeout
 *     c.get("foo", :lock => true)
 *
 *   @example Determine lock timeout parameters
 *     c.stats.values_at("ep_getl_default_timeout", "ep_getl_max_timeout")
 *     #=> [{"127.0.0.1:11210"=>"15"}, {"127.0.0.1:11210"=>"30"}]
 *
 *   @example Get and lock key using custom timeout
 *     c.get("foo", :lock => 3)
 *
 *   @example Get and lock multiple keys using custom timeout
 *     c.get("foo", "bar", :lock => 3)
 *
 * @overload get(keys, options = {})
 *   When the method receive hash map, it will behave like it receive list
 *   of keys (+keys.keys+), but also touch each key setting expiry time to
 *   the corresponding value. But unlike usual get this command always
 *   return hash map +{key => value}+ or +{key => [value, flags, cas]}+.
 *
 *   @param keys [Hash] Map key-ttl
 *   @param options [Hash] Options for operation. (see options definition
 *     above)
 *
 *   @return [Hash] the values (or tuples in extended mode) assiciated with
 *     the keys.
 *
 *   @example Get and touch multiple keys
 *     c.get("foo" => 10, "bar" => 20)   #=> {"foo" => val1, "bar" => val2}
 *
 *   @example Extended get and touch multiple keys
 *     c.get({"foo" => 10, "bar" => 20}, :extended => true)
 *     #=> {"foo" => [val1, flags1, cas1], "bar" => [val2, flags2, cas2]}
 *
 *   @example Get and lock multiple keys for chosen period in seconds
 *     c.get("foo" => 10, "bar" => 20, :lock => true)
 *     #=> {"foo" => val1, "bar" => val2}
 */
    static VALUE
cb_bucket_get(int argc, VALUE *argv, VALUE self)
{
    struct bucket_st *bucket = DATA_PTR(self);
    struct context_st *ctx;
    VALUE args, rv, proc, exc, keys;
    size_t nn, ii, ll = 0;
    libcouchbase_error_t err = LIBCOUCHBASE_SUCCESS;
    struct key_traits_st *traits;
    int extended, mgat, is_array, assemble_hash;

    if (bucket->handle == NULL) {
        rb_raise(eConnectError, "closed connection");
    }
    rb_scan_args(argc, argv, "0*&", &args, &proc);
    if (!bucket->async && proc != Qnil) {
        rb_raise(rb_eArgError, "synchronous mode doesn't support callbacks");
    }
    traits = xcalloc(1, sizeof(struct key_traits_st));
    traits->bucket = bucket;
    nn = cb_args_scan_keys(RARRAY_LEN(args), args, traits);
    ctx = xcalloc(1, sizeof(struct context_st));
    if (ctx == NULL) {
        rb_raise(eClientNoMemoryError, "failed to allocate memory for context");
    }
    mgat = traits->mgat;
    assemble_hash = traits->assemble_hash;
    keys = traits->keys_ary;
    is_array = traits->is_array;
    ctx->proc = proc;
    cb_gc_protect(bucket, ctx->proc);
    ctx->bucket = bucket;
    ctx->extended = traits->extended;
    ctx->quiet = traits->quiet;
    ctx->force_format = traits->force_format;
    rv = rb_hash_new();
    ctx->rv = &rv;
    ctx->exception = Qnil;
    ctx->nqueries = traits->nkeys;
    if (traits->lock) {
        for (ii = 0; ii < traits->nkeys; ++ii) {
            err = libcouchbase_getl(bucket->handle, (const void *)ctx,
                    (const void *)traits->keys[ii], traits->lens[ii],
                    traits->ttls + ii);
            if (err != LIBCOUCHBASE_SUCCESS) {
                break;
            }
        }
    } else if (traits->replica) {
        err = libcouchbase_get_replica(bucket->handle, (const void *)ctx,
                traits->nkeys, (const void * const *)traits->keys,
                traits->lens);
    } else {
        err = libcouchbase_mget(bucket->handle, (const void *)ctx,
                traits->nkeys, (const void * const *)traits->keys,
                traits->lens, (traits->explicit_ttl) ? traits->ttls : NULL);
    }
    if (err == LIBCOUCHBASE_SUCCESS) {
        for (ii = 0; ii < traits->nkeys; ++ii) {
            ll += traits->lens[ii];
        }
    }
    xfree(traits->keys);
    xfree(traits->lens);
    xfree(traits->ttls);
    xfree(traits);
    exc = cb_check_error(err, "failed to schedule get request", Qnil);
    if (exc != Qnil) {
        xfree(ctx);
        rb_exc_raise(exc);
    }
    bucket->nbytes += HEADER_SIZE + 4 + ll;
    if (bucket->async) {
        maybe_do_loop(bucket);
        return Qnil;
    } else {
        if (ctx->nqueries > 0) {
            /* we have some operations pending */
            libcouchbase_wait(bucket->handle);
        }
        exc = ctx->exception;
        extended = ctx->extended;
        xfree(ctx);
        if (exc != Qnil) {
            cb_gc_unprotect(bucket, exc);
            rb_exc_raise(exc);
        }
        if (bucket->exception != Qnil) {
            rb_exc_raise(bucket->exception);
        }
        if (assemble_hash || mgat || (extended && (nn > 1 || is_array))) {
            return rv;  /* return as a hash {key => [value, flags, cas], ...} */
        }
        if (nn > 1 || is_array) {
            VALUE *keys_ptr, ret;
            ret = rb_ary_new();
            keys_ptr = RARRAY_PTR(keys);
            for (ii = 0; ii < nn; ii++) {
                rb_ary_push(ret, rb_hash_aref(rv, keys_ptr[ii]));
            }
            return ret;  /* return as an array [value1, value2, ...] */
        } else {
            VALUE vv = Qnil;
            rb_hash_foreach(rv, cb_first_value_i, (VALUE)&vv);
            return vv;
        }
    }
}

/*
 * Update the expiry time of an item
 *
 * @since 1.0.0
 *
 * The +touch+ method allow you to update the expiration time on a given
 * key. This can be useful for situations where you want to prevent an item
 * from expiring without resetting the associated value. For example, for a
 * session database you might want to keep the session alive in the database
 * each time the user accesses a web page without explicitly updating the
 * session value, keeping the user's session active and available.
 *
 * @overload touch(key, options = {})
 *   @param key [String, Symbol] Key used to reference the value.
 *   @param options [Hash] Options for operation.
 *   @option options [Fixnum] :ttl (self.default_ttl) Expiry time for key.
 *     Values larger than 30*24*60*60 seconds (30 days) are interpreted as
 *     absolute times (from the epoch).
 *
 *   @yieldparam ret [Result] the result of operation in asynchronous mode
 *     (valid attributes: +error+, +operation+, +key+).
 *
 *   @return [true, false] +true+ if the operation was successful and +false+
 *     otherwise.
 *
 *   @raise [Couchbase::Error::Connect] if connection closed (see {Bucket#reconnect})
 *
 *   @raise [ArgumentError] when passing the block in synchronous mode
 *
 *   @example Touch value using +default_ttl+
 *     c.touch("foo")
 *
 *   @example Touch value using custom TTL (10 seconds)
 *     c.touch("foo", :ttl => 10)
 *
 * @overload touch(keys)
 *   @param keys [Hash] The Hash where keys represent the keys in the
 *     database, values -- the expiry times for corresponding key. See
 *     description of +:ttl+ argument above for more information about TTL
 *     values.
 *
 *   @yieldparam ret [Result] the result of operation for each key in
 *     asynchronous mode (valid attributes: +error+, +operation+, +key+).
 *
 *   @return [Hash] Mapping keys to result of touch operation (+true+ if the
 *     operation was successful and +false+ otherwise)
 *
 *   @example Touch several values
 *     c.touch("foo" => 10, :bar => 20) #=> {"foo" => true, "bar" => true}
 *
 *   @example Touch several values in async mode
 *     c.run do
 *       c.touch("foo" => 10, :bar => 20) do |ret|
 *          ret.operation   #=> :touch
 *          ret.success?    #=> true
 *          ret.key         #=> "foo" and "bar" in separate calls
 *       end
 *     end
 *
 *   @example Touch single value
 *     c.touch("foo" => 10)             #=> true
 *
 */
   static VALUE
cb_bucket_touch(int argc, VALUE *argv, VALUE self)
{
    struct bucket_st *bucket = DATA_PTR(self);
    struct context_st *ctx;
    VALUE args, rv, proc, exc;
    size_t nn, ii, ll;
    libcouchbase_error_t err;
    struct key_traits_st *traits;

    if (bucket->handle == NULL) {
        rb_raise(eConnectError, "closed connection");
    }
    rb_scan_args(argc, argv, "0*&", &args, &proc);
    if (!bucket->async && proc != Qnil) {
        rb_raise(rb_eArgError, "synchronous mode doesn't support callbacks");
    }
    rb_funcall(args, id_flatten_bang, 0);
    traits = xcalloc(1, sizeof(struct key_traits_st));
    traits->bucket = bucket;
    nn = cb_args_scan_keys(RARRAY_LEN(args), args, traits);
    ctx = xcalloc(1, sizeof(struct context_st));
    if (ctx == NULL) {
        rb_raise(eClientNoMemoryError, "failed to allocate memory for context");
    }
    ctx->proc = proc;
    cb_gc_protect(bucket, ctx->proc);
    ctx->bucket = bucket;
    rv = rb_hash_new();
    ctx->rv = &rv;
    ctx->exception = Qnil;
    ctx->nqueries = traits->nkeys;
    err = libcouchbase_mtouch(bucket->handle, (const void *)ctx,
            traits->nkeys, (const void * const *)traits->keys,
            traits->lens, traits->ttls);
    for (ii = 0, ll = 0; ii < traits->nkeys; ++ii) {
        ll += traits->lens[ii];
    }
    xfree(traits->keys);
    xfree(traits->lens);
    xfree(traits);
    exc = cb_check_error(err, "failed to schedule touch request", Qnil);
    if (exc != Qnil) {
        xfree(ctx);
        rb_exc_raise(exc);
    }
    bucket->nbytes += HEADER_SIZE + 4 + ll;
    if (bucket->async) {
        maybe_do_loop(bucket);
        return Qnil;
    } else {
        if (ctx->nqueries > 0) {
            /* we have some operations pending */
            libcouchbase_wait(bucket->handle);
        }
        exc = ctx->exception;
        xfree(ctx);
        if (exc != Qnil) {
            cb_gc_unprotect(bucket, exc);
            rb_exc_raise(exc);
        }
        if (bucket->exception != Qnil) {
            rb_exc_raise(bucket->exception);
        }
        if (nn > 1) {
            return rv;  /* return as a hash {key => true, ...} */
        } else {
            VALUE vv = Qnil;
            rb_hash_foreach(rv, cb_first_value_i, (VALUE)&vv);
            return vv;
        }
    }
}

/*
 * Deletes all values from a server
 *
 * @since 1.0.0
 *
 * @overload flush
 *   @yieldparam [Result] ret the object with +error+, +node+ and +operation+
 *     attributes.
 *
 *   @return [true, false] +true+ on success
 *
 *   @raise [Couchbase::Error::Connect] if connection closed (see {Bucket#reconnect})
 *   @raise [ArgumentError] when passing the block in synchronous mode
 *
 *   @example Simple flush the bucket
 *     c.flush    #=> true
 *
 *   @example Asynchronous flush
 *     c.run do
 *       c.flush do |ret|
 *         ret.operation   #=> :flush
 *         ret.success?    #=> true
 *         ret.node        #=> "localhost:11211"
 *       end
 *     end
 */
    static VALUE
cb_bucket_flush(VALUE self)
{
    struct bucket_st *bucket = DATA_PTR(self);
    struct context_st *ctx;
    VALUE rv, exc;
    libcouchbase_error_t err;

    if (bucket->handle == NULL) {
        rb_raise(eConnectError, "closed connection");
    }
    if (!bucket->async && rb_block_given_p()) {
        rb_raise(rb_eArgError, "synchronous mode doesn't support callbacks");
    }
    ctx = xcalloc(1, sizeof(struct context_st));
    if (ctx == NULL) {
        rb_raise(eClientNoMemoryError, "failed to allocate memory for context");
    }
    rv = Qtrue;	/* optimistic by default */
    ctx->rv = &rv;
    ctx->bucket = bucket;
    ctx->exception = Qnil;
    if (rb_block_given_p()) {
        ctx->proc = rb_block_proc();
    } else {
        ctx->proc = Qnil;
    }
    ctx->nqueries = 1;
    cb_gc_protect(bucket, ctx->proc);
    err = libcouchbase_flush(bucket->handle, (const void *)ctx);
    exc = cb_check_error(err, "failed to schedule flush request", Qnil);
    if (exc != Qnil) {
        xfree(ctx);
        rb_exc_raise(exc);
    }
    bucket->nbytes += HEADER_SIZE;
    if (bucket->async) {
        maybe_do_loop(bucket);
        return Qnil;
    } else {
        if (ctx->nqueries > 0) {
            /* we have some operations pending */
            libcouchbase_wait(bucket->handle);
        }
        exc = ctx->exception;
        xfree(ctx);
        if (exc != Qnil) {
            cb_gc_unprotect(bucket, exc);
            rb_exc_raise(exc);
        }
        return rv;
    }
}

/*
 * Returns versions of the server for each node in the cluster
 *
 * @since 1.1.0
 *
 * @overload version
 *   @yieldparam [Result] ret the object with +error+, +node+, +operation+
 *     and +value+ attributes.
 *
 *   @return [Hash] node-version pairs
 *
 *   @raise [Couchbase::Error::Connect] if connection closed (see {Bucket#reconnect})
 *   @raise [ArgumentError] when passing the block in synchronous mode
 *
 *   @example Synchronous version request
 *     c.version            #=> will render version
 *
 *   @example Asynchronous version request
 *     c.run do
 *       c.version do |ret|
 *         ret.operation    #=> :version
 *         ret.success?     #=> true
 *         ret.node         #=> "localhost:11211"
 *         ret.value        #=> will render version
 *       end
 *     end
 */
    static VALUE
cb_bucket_version(VALUE self)
{
    struct bucket_st *bucket = DATA_PTR(self);
    struct context_st *ctx;
    VALUE rv, exc;
    libcouchbase_error_t err;

    if (bucket->handle == NULL) {
        rb_raise(eConnectError, "closed connection");
    }
    if (!bucket->async && rb_block_given_p()) {
        rb_raise(rb_eArgError, "synchronous mode doesn't support callbacks");
    }
    ctx = xcalloc(1, sizeof(struct context_st));
    if (ctx == NULL) {
        rb_raise(eClientNoMemoryError, "failed to allocate memory for context");
    }
    rv = rb_hash_new();
    ctx->rv = &rv;
    ctx->bucket = bucket;
    ctx->exception = Qnil;
    if (rb_block_given_p()) {
        ctx->proc = rb_block_proc();
    } else {
        ctx->proc = Qnil;
    }
    cb_gc_protect(bucket, ctx->proc);
    ctx->nqueries = 1;
    err = libcouchbase_server_versions(bucket->handle, (const void *)ctx);
    exc = cb_check_error(err, "failed to schedule version request", Qnil);
    if (exc != Qnil) {
        xfree(ctx);
        rb_exc_raise(exc);
    }
    bucket->nbytes += HEADER_SIZE;
    if (bucket->async) {
        maybe_do_loop(bucket);
        return Qnil;
    } else {
        if (ctx->nqueries > 0) {
            /* we have some operations pending */
            libcouchbase_wait(bucket->handle);
        }
        exc = ctx->exception;
        xfree(ctx);
        if (exc != Qnil) {
            cb_gc_unprotect(bucket, exc);
            rb_exc_raise(exc);
        }
        return rv;
    }
}

/*
 * Request server statistics.
 *
 * @since 1.0.0
 *
 * Fetches stats from each node in cluster. Without a key specified the
 * server will respond with a "default" set of statistical information. In
 * asynchronous mode each statistic is returned in separate call where the
 * Result object yielded (+#key+ contains the name of the statistical item
 * and the +#value+ contains the value, the +#node+ will indicate the server
 * address). In synchronous mode it returns the hash of stats keys and
 * node-value pairs as a value.
 *
 * @overload stats(arg = nil)
 *   @param [String] arg argument to STATS query
 *   @yieldparam [Result] ret the object with +node+, +key+ and +value+
 *     attributes.
 *
 *   @example Found how many items in the bucket
 *     total = 0
 *     c.stats["total_items"].each do |key, value|
 *       total += value.to_i
 *     end
 *
 *   @example Found total items number asynchronously
 *     total = 0
 *     c.run do
 *       c.stats do |ret|
 *         if ret.key == "total_items"
 *           total += ret.value.to_i
 *         end
 *       end
 *     end
 *
 *   @example Get memory stats (works on couchbase buckets)
 *     c.stats(:memory)   #=> {"mem_used"=>{...}, ...}
 *
 *   @return [Hash] where keys are stat keys, values are host-value pairs
 *
 *   @raise [Couchbase::Error::Connect] if connection closed (see {Bucket#reconnect})
 *   @raise [ArgumentError] when passing the block in synchronous mode
 */
    static VALUE
cb_bucket_stats(int argc, VALUE *argv, VALUE self)
{
    struct bucket_st *bucket = DATA_PTR(self);
    struct context_st *ctx;
    VALUE rv, exc, arg, proc;
    char *key;
    size_t nkey;
    libcouchbase_error_t err;

    if (bucket->handle == NULL) {
        rb_raise(eConnectError, "closed connection");
    }
    rb_scan_args(argc, argv, "01&", &arg, &proc);
    if (!bucket->async && proc != Qnil) {
        rb_raise(rb_eArgError, "synchronous mode doesn't support callbacks");
    }

    ctx = xcalloc(1, sizeof(struct context_st));
    if (ctx == NULL) {
        rb_raise(eClientNoMemoryError, "failed to allocate memory for context");
    }
    rv = rb_hash_new();
    ctx->rv = &rv;
    ctx->bucket = bucket;
    ctx->proc = proc;
    cb_gc_protect(bucket, ctx->proc);
    ctx->exception = Qnil;
    if (arg != Qnil) {
        arg = unify_key(bucket, arg, 0);
        key = RSTRING_PTR(arg);
        nkey = RSTRING_LEN(arg);
    } else {
        key = NULL;
        nkey = 0;
    }
    ctx->nqueries = 1;
    err = libcouchbase_server_stats(bucket->handle, (const void *)ctx,
            key, nkey);
    exc = cb_check_error(err, "failed to schedule stat request", Qnil);
    if (exc != Qnil) {
        xfree(ctx);
        rb_exc_raise(exc);
    }
    if (bucket->async) {
        bucket->nbytes += HEADER_SIZE + nkey;
        maybe_do_loop(bucket);
        return Qnil;
    } else {
        if (ctx->nqueries > 0) {
            /* we have some operations pending */
            libcouchbase_wait(bucket->handle);
        }
        exc = ctx->exception;
        xfree(ctx);
        if (exc != Qnil) {
            cb_gc_unprotect(bucket, exc);
            rb_exc_raise(exc);
        }
        if (bucket->exception != Qnil) {
            rb_exc_raise(bucket->exception);
        }
        return rv;
    }

    return Qnil;
}

    static void
do_loop(struct bucket_st *bucket)
{
    libcouchbase_wait(bucket->handle);
    bucket->nbytes = 0;
}

    static void
maybe_do_loop(struct bucket_st *bucket)
{
    if (bucket->threshold != 0 && bucket->nbytes > bucket->threshold) {
        do_loop(bucket);
    }
}

    static VALUE
do_run(VALUE *args)
{
    VALUE self = args[0], opts = args[1], proc = args[2], exc;
    struct bucket_st *bucket = DATA_PTR(self);

    if (bucket->handle == NULL) {
        rb_raise(eConnectError, "closed connection");
    }
    if (bucket->async) {
        rb_raise(eInvalidError, "nested #run");
    }
    bucket->threshold = 0;
    if (opts != Qnil) {
        VALUE arg;
        Check_Type(opts, T_HASH);
        arg = rb_hash_aref(opts, sym_send_threshold);
        if (arg != Qnil) {
            bucket->threshold = (uint32_t)NUM2ULONG(arg);
        }
    }
    bucket->async = 1;
    cb_proc_call(proc, 1, self);
    do_loop(bucket);
    if (bucket->exception != Qnil) {
        exc = bucket->exception;
        bucket->exception = Qnil;
        rb_exc_raise(exc);
    }
    return Qnil;
}

    static VALUE
ensure_run(VALUE *args)
{
    VALUE self = args[0];
    struct bucket_st *bucket = DATA_PTR(self);

    bucket->async = 0;
    return Qnil;
}

/*
 * Run the event loop.
 *
 * @since 1.0.0
 *
 * @param [Hash] options The options for operation for connection
 * @option options [Fixnum] :send_threshold (0) if the internal command
 *   buffer will exceeds this value, then the library will start network
 *   interaction and block the current thread until all scheduled commands
 *   will be completed.
 *
 * @yieldparam [Bucket] bucket the bucket instance
 *
 * @example Use block to run the loop
 *   c = Couchbase.new
 *   c.run do
 *     c.get("foo") {|ret| puts ret.value}
 *   end
 *
 * @example Use lambda to run the loop
 *   c = Couchbase.new
 *   operations = lambda do |c|
 *     c.get("foo") {|ret| puts ret.value}
 *   end
 *   c.run(&operations)
 *
 * @example Use threshold to send out commands automatically
 *   c = Couchbase.connect
 *   sent = 0
 *   c.run(:send_threshold => 8192) do  # 8Kb
 *     c.set("foo1", "x" * 100) {|r| sent += 1}
 *     # 128 bytes buffered, sent is 0 now
 *     c.set("foo2", "x" * 10000) {|r| sent += 1}
 *     # 10028 bytes added, sent is 2 now
 *     c.set("foo3", "x" * 100) {|r| sent += 1}
 *   end
 *   # all commands were executed and sent is 3 now
 *
 * @return [nil]
 *
 * @raise [Couchbase::Error::Connect] if connection closed (see {Bucket#reconnect})
 */
    static VALUE
cb_bucket_run(int argc, VALUE *argv, VALUE self)
{
    VALUE args[3];

    rb_need_block();
    args[0] = self;
    rb_scan_args(argc, argv, "01&", &args[1], &args[2]);
    rb_ensure(do_run, (VALUE)args, ensure_run, (VALUE)args);
    return Qnil;
}

/*
 * Stop the event loop.
 *
 * @since 1.2.0
 *
 * @example Breakout the event loop when 5th request is completed
 *   c = Couchbase.connect
 *   c.run do
 *     10.times do |ii|
 *       c.get("foo") do |ret|
 *         puts ii
 *         c.stop if ii == 5
 *       end
 *     end
 *   end
 *
 * @return [nil]
 */
    static VALUE
cb_bucket_stop(VALUE self)
{
    struct bucket_st *bucket = DATA_PTR(self);
    bucket->io->stop_event_loop(bucket->io);
    return Qnil;
}

/*
 * Unconditionally store the object in the Couchbase
 *
 * @since 1.0.0
 *
 * @overload set(key, value, options = {})
 *
 *   @param key [String, Symbol] Key used to reference the value.
 *   @param value [Object] Value to be stored
 *   @param options [Hash] Options for operation.
 *   @option options [Fixnum] :ttl (self.default_ttl) Expiry time for key.
 *     Values larger than 30*24*60*60 seconds (30 days) are interpreted as
 *     absolute times (from the epoch).
 *   @option options [Fixnum] :flags (self.default_flags) Flags for storage
 *     options. Flags are ignored by the server but preserved for use by the
 *     client. For more info see {Bucket#default_flags}.
 *   @option options [Symbol] :format (self.default_format) The
 *     representation for storing the value in the bucket. For more info see
 *     {Bucket#default_format}.
 *   @option options [Fixnum] :cas The CAS value for an object. This value
 *     created on the server and is guaranteed to be unique for each value of
 *     a given key. This value is used to provide simple optimistic
 *     concurrency control when multiple clients or threads try to update an
 *     item simultaneously.
 *
 *   @yieldparam ret [Result] the result of operation in asynchronous mode
 *     (valid attributes: +error+, +operation+, +key+).
 *
 *   @return [Fixnum] The CAS value of the object.
 *
 *   @raise [Couchbase::Error::Connect] if connection closed (see {Bucket#reconnect}).
 *   @raise [Couchbase::Error::KeyExists] if the key already exists on the
 *     server.
 *   @raise [Couchbase::Error::ValueFormat] if the value cannot be serialized
 *     with chosen encoder, e.g. if you try to store the Hash in +:plain+
 *     mode.
 *   @raise [ArgumentError] when passing the block in synchronous mode
 *
 *   @example Store the key which will be expired in 2 seconds using relative TTL.
 *     c.set("foo", "bar", :ttl => 2)
 *
 *   @example Store the key which will be expired in 2 seconds using absolute TTL.
 *     c.set("foo", "bar", :ttl => Time.now.to_i + 2)
 *
 *   @example Force JSON document format for value
 *     c.set("foo", {"bar" => "baz}, :format => :document)
 *
 *   @example Use hash-like syntax to store the value
 *     c.set["foo"] = {"bar" => "baz}
 *
 *   @example Use extended hash-like syntax
 *     c["foo", {:flags => 0x1000, :format => :plain}] = "bar"
 *     c["foo", :flags => 0x1000] = "bar"  # for ruby 1.9.x only
 *
 *   @example Set application specific flags (note that it will be OR-ed with format flags)
 *     c.set("foo", "bar", :flags => 0x1000)
 *
 *   @example Perform optimistic locking by specifying last known CAS version
 *     c.set("foo", "bar", :cas => 8835713818674332672)
 *
 *   @example Perform asynchronous call
 *     c.run do
 *       c.set("foo", "bar") do |ret|
 *         ret.operation   #=> :set
 *         ret.success?    #=> true
 *         ret.key         #=> "foo"
 *         ret.cas
 *       end
 *     end
 */
    static VALUE
cb_bucket_set(int argc, VALUE *argv, VALUE self)
{
    return cb_bucket_store(LIBCOUCHBASE_SET, argc, argv, self);
}

/*
 * Add the item to the database, but fail if the object exists already
 *
 * @since 1.0.0
 *
 * @overload add(key, value, options = {})
 *
 *   @param key [String, Symbol] Key used to reference the value.
 *   @param value [Object] Value to be stored
 *   @param options [Hash] Options for operation.
 *   @option options [Fixnum] :ttl (self.default_ttl) Expiry time for key.
 *     Values larger than 30*24*60*60 seconds (30 days) are interpreted as
 *     absolute times (from the epoch).
 *   @option options [Fixnum] :flags (self.default_flags) Flags for storage
 *     options. Flags are ignored by the server but preserved for use by the
 *     client. For more info see {Bucket#default_flags}.
 *   @option options [Symbol] :format (self.default_format) The
 *     representation for storing the value in the bucket. For more info see
 *     {Bucket#default_format}.
 *   @option options [Fixnum] :cas The CAS value for an object. This value
 *     created on the server and is guaranteed to be unique for each value of
 *     a given key. This value is used to provide simple optimistic
 *     concurrency control when multiple clients or threads try to update an
 *     item simultaneously.
 *
 *   @yieldparam ret [Result] the result of operation in asynchronous mode
 *     (valid attributes: +error+, +operation+, +key+).
 *
 *   @return [Fixnum] The CAS value of the object.
 *
 *   @raise [Couchbase::Error::Connect] if connection closed (see {Bucket#reconnect})
 *   @raise [Couchbase::Error::KeyExists] if the key already exists on the
 *     server
 *   @raise [Couchbase::Error::ValueFormat] if the value cannot be serialized
 *     with chosen encoder, e.g. if you try to store the Hash in +:plain+
 *     mode.
 *   @raise [ArgumentError] when passing the block in synchronous mode
 *
 *   @example Add the same key twice
 *     c.add("foo", "bar")  #=> stored successully
 *     c.add("foo", "baz")  #=> will raise Couchbase::Error::KeyExists: failed to store value (key="foo", error=0x0c)
 */
    static VALUE
cb_bucket_add(int argc, VALUE *argv, VALUE self)
{
    return cb_bucket_store(LIBCOUCHBASE_ADD, argc, argv, self);
}

/*
 * Replace the existing object in the database
 *
 * @since 1.0.0
 *
 * @overload replace(key, value, options = {})
 *   @param key [String, Symbol] Key used to reference the value.
 *   @param value [Object] Value to be stored
 *   @param options [Hash] Options for operation.
 *   @option options [Fixnum] :ttl (self.default_ttl) Expiry time for key.
 *     Values larger than 30*24*60*60 seconds (30 days) are interpreted as
 *     absolute times (from the epoch).
 *   @option options [Fixnum] :flags (self.default_flags) Flags for storage
 *     options. Flags are ignored by the server but preserved for use by the
 *     client. For more info see {Bucket#default_flags}.
 *   @option options [Symbol] :format (self.default_format) The
 *     representation for storing the value in the bucket. For more info see
 *     {Bucket#default_format}.
 *   @option options [Fixnum] :cas The CAS value for an object. This value
 *     created on the server and is guaranteed to be unique for each value of
 *     a given key. This value is used to provide simple optimistic
 *     concurrency control when multiple clients or threads try to update an
 *     item simultaneously.
 *
 *   @return [Fixnum] The CAS value of the object.
 *
 *   @raise [Couchbase::Error::Connect] if connection closed (see {Bucket#reconnect})
 *   @raise [Couchbase::Error::NotFound] if the key doesn't exists
 *   @raise [Couchbase::Error::KeyExists] on CAS mismatch
 *   @raise [ArgumentError] when passing the block in synchronous mode
 *
 *   @example Replacing missing key
 *     c.replace("foo", "baz")  #=> will raise Couchbase::Error::NotFound: failed to store value (key="foo", error=0x0d)
 */
    static VALUE
cb_bucket_replace(int argc, VALUE *argv, VALUE self)
{
    return cb_bucket_store(LIBCOUCHBASE_REPLACE, argc, argv, self);
}

/*
 * Append this object to the existing object
 *
 * @since 1.0.0
 *
 * @note This operation is kind of data-aware from server point of view.
 *   This mean that the server treats value as binary stream and just
 *   perform concatenation, therefore it won't work with +:marshal+ and
 *   +:document+ formats, because of lack of knowledge how to merge values
 *   in these formats. See {Bucket#cas} for workaround.
 *
 * @overload append(key, value, options = {})
 *   @param key [String, Symbol] Key used to reference the value.
 *   @param value [Object] Value to be stored
 *   @param options [Hash] Options for operation.
 *   @option options [Fixnum] :cas The CAS value for an object. This value
 *     created on the server and is guaranteed to be unique for each value of
 *     a given key. This value is used to provide simple optimistic
 *     concurrency control when multiple clients or threads try to update an
 *     item simultaneously.
 *   @option options [Symbol] :format (self.default_format) The
 *     representation for storing the value in the bucket. For more info see
 *     {Bucket#default_format}.
 *
 *   @return [Fixnum] The CAS value of the object.
 *
 *   @raise [Couchbase::Error::Connect] if connection closed (see {Bucket#reconnect})
 *   @raise [Couchbase::Error::KeyExists] on CAS mismatch
 *   @raise [Couchbase::Error::NotStored] if the key doesn't exist
 *   @raise [ArgumentError] when passing the block in synchronous mode
 *
 *   @example Simple append
 *     c.set("foo", "aaa")
 *     c.append("foo", "bbb")
 *     c.get("foo")           #=> "aaabbb"
 *
 *   @example Implementing sets using append
 *     def set_add(key, *values)
 *       encoded = values.flatten.map{|v| "+#{v} "}.join
 *       append(key, encoded)
 *     end
 *
 *     def set_remove(key, *values)
 *       encoded = values.flatten.map{|v| "-#{v} "}.join
 *       append(key, encoded)
 *     end
 *
 *     def set_get(key)
 *       encoded = get(key)
 *       ret = Set.new
 *       encoded.split(' ').each do |v|
 *         op, val = v[0], v[1..-1]
 *         case op
 *         when "-"
 *           ret.delete(val)
 *         when "+"
 *           ret.add(val)
 *         end
 *       end
 *       ret
 *     end
 *
 *   @example Using optimistic locking. The operation will fail on CAS mismatch
 *     ver = c.set("foo", "aaa")
 *     c.append("foo", "bbb", :cas => ver)
 */
    static VALUE
cb_bucket_append(int argc, VALUE *argv, VALUE self)
{
    return cb_bucket_store(LIBCOUCHBASE_APPEND, argc, argv, self);
}

/*
 * Prepend this object to the existing object
 *
 * @since 1.0.0
 *
 * @note This operation is kind of data-aware from server point of view.
 *   This mean that the server treats value as binary stream and just
 *   perform concatenation, therefore it won't work with +:marshal+ and
 *   +:document+ formats, because of lack of knowledge how to merge values
 *   in these formats. See {Bucket#cas} for workaround.
 *
 * @overload prepend(key, value, options = {})
 *   @param key [String, Symbol] Key used to reference the value.
 *   @param value [Object] Value to be stored
 *   @param options [Hash] Options for operation.
 *   @option options [Fixnum] :cas The CAS value for an object. This value
 *     created on the server and is guaranteed to be unique for each value of
 *     a given key. This value is used to provide simple optimistic
 *     concurrency control when multiple clients or threads try to update an
 *     item simultaneously.
 *   @option options [Symbol] :format (self.default_format) The
 *     representation for storing the value in the bucket. For more info see
 *     {Bucket#default_format}.
 *
 *   @raise [Couchbase::Error::Connect] if connection closed (see {Bucket#reconnect})
 *   @raise [Couchbase::Error::KeyExists] on CAS mismatch
 *   @raise [Couchbase::Error::NotStored] if the key doesn't exist
 *   @raise [ArgumentError] when passing the block in synchronous mode
 *
 *   @example Simple prepend example
 *     c.set("foo", "aaa")
 *     c.prepend("foo", "bbb")
 *     c.get("foo")           #=> "bbbaaa"
 *
 *   @example Using explicit format option
 *     c.default_format       #=> :document
 *     c.set("foo", {"y" => "z"})
 *     c.prepend("foo", '[', :format => :plain)
 *     c.append("foo", ', {"z": "y"}]', :format => :plain)
 *     c.get("foo")           #=> [{"y"=>"z"}, {"z"=>"y"}]
 *
 *   @example Using optimistic locking. The operation will fail on CAS mismatch
 *     ver = c.set("foo", "aaa")
 *     c.prepend("foo", "bbb", :cas => ver)
 */
    static VALUE
cb_bucket_prepend(int argc, VALUE *argv, VALUE self)
{
    return cb_bucket_store(LIBCOUCHBASE_PREPEND, argc, argv, self);
}

    static VALUE
cb_bucket_aset(int argc, VALUE *argv, VALUE self)
{
    VALUE temp;

    if (argc == 3) {
        /* swap opts and value, because value goes last for []= */
        temp = argv[2];
        argv[2] = argv[1];
        argv[1] = temp;
    }
    return cb_bucket_set(argc, argv, self);
}

/*
 * Close the connection to the cluster
 *
 * @since 1.1.0
 *
 * @return [true]
 *
 * @raise [Couchbase::Error::Connect] if connection closed (see {Bucket#reconnect})
 */
    static VALUE
cb_bucket_disconnect(VALUE self)
{
    struct bucket_st *bucket = DATA_PTR(self);

    if (bucket->handle) {
        libcouchbase_destroy(bucket->handle);
        bucket->handle = NULL;
        bucket->io = NULL;
        return Qtrue;
    } else {
        rb_raise(eConnectError, "closed connection");
    }
}

/*
 * Check if result of operation was successful.
 *
 * @since 1.0.0
 *
 * @return [true, false] +false+ if there is an +error+ object attached,
 *   +false+ otherwise.
 */
    static VALUE
cb_result_success_p(VALUE self)
{
    return RTEST(rb_ivar_get(self, id_iv_error)) ? Qfalse : Qtrue;
}

/*
 * Returns a string containing a human-readable representation of the Result.
 *
 * @since 1.0.0
 *
 * @return [String]
 */
    static VALUE
cb_result_inspect(VALUE self)
{
    VALUE str, attr, error;
    char buf[100];

    str = rb_str_buf_new2("#<");
    rb_str_buf_cat2(str, rb_obj_classname(self));
    snprintf(buf, 100, ":%p", (void *)self);
    rb_str_buf_cat2(str, buf);

    attr = rb_ivar_get(self, id_iv_error);
    if (RTEST(attr)) {
        error = rb_ivar_get(attr, id_iv_error);
    } else {
        error = INT2FIX(0);
    }
    rb_str_buf_cat2(str, " error=0x");
    rb_str_append(str, rb_funcall(error, id_to_s, 1, INT2FIX(16)));

    attr = rb_ivar_get(self, id_iv_key);
    if (RTEST(attr)) {
        rb_str_buf_cat2(str, " key=");
        rb_str_append(str, rb_inspect(attr));
    }

    attr = rb_ivar_get(self, id_iv_status);
    if (RTEST(attr)) {
        rb_str_buf_cat2(str, " status=");
        rb_str_append(str, rb_inspect(attr));
    }

    attr = rb_ivar_get(self, id_iv_cas);
    if (RTEST(attr)) {
        rb_str_buf_cat2(str, " cas=");
        rb_str_append(str, rb_inspect(attr));
    }

    attr = rb_ivar_get(self, id_iv_flags);
    if (RTEST(attr)) {
        rb_str_buf_cat2(str, " flags=0x");
        rb_str_append(str, rb_funcall(attr, id_to_s, 1, INT2FIX(16)));
    }

    attr = rb_ivar_get(self, id_iv_node);
    if (RTEST(attr)) {
        rb_str_buf_cat2(str, " node=");
        rb_str_append(str, rb_inspect(attr));
    }

    attr = rb_ivar_get(self, id_iv_from_master);
    if (attr != Qnil) {
        rb_str_buf_cat2(str, " from_master=");
        rb_str_append(str, rb_inspect(attr));
    }

    attr = rb_ivar_get(self, id_iv_time_to_persist);
    if (RTEST(attr)) {
        rb_str_buf_cat2(str, " time_to_persist=");
        rb_str_append(str, rb_inspect(attr));
    }

    attr = rb_ivar_get(self, id_iv_time_to_replicate);
    if (RTEST(attr)) {
        rb_str_buf_cat2(str, " time_to_replicate=");
        rb_str_append(str, rb_inspect(attr));
    }

    rb_str_buf_cat2(str, ">");

    return str;
}

    void
cb_timer_free(void *ptr)
{
    xfree(ptr);
}

    void
cb_timer_mark(void *ptr)
{
    struct timer_st *timer = ptr;
    if (timer) {
        rb_gc_mark(timer->callback);
    }
}

    static VALUE
cb_timer_alloc(VALUE klass)
{
    VALUE obj;
    struct timer_st *timer;

    /* allocate new bucket struct and set it to zero */
    obj = Data_Make_Struct(klass, struct timer_st, cb_timer_mark,
            cb_timer_free, timer);
    return obj;
}

/*
 * Returns a string containing a human-readable representation of the
 * Timer.
 *
 * @since 1.2.0.dp6
 *
 * @return [String]
 */
    static VALUE
cb_timer_inspect(VALUE self)
{
    VALUE str;
    struct timer_st *tm = DATA_PTR(self);
    char buf[200];

    str = rb_str_buf_new2("#<");
    rb_str_buf_cat2(str, rb_obj_classname(self));
    snprintf(buf, 20, ":%p", (void *)self);
    rb_str_buf_cat2(str, buf);
    snprintf(buf, 100, " timeout:%u periodic:%s>",
            tm->usec, tm->periodic ? "true" : "false");
    rb_str_buf_cat2(str, buf);

    return str;
}

/*
 * Cancel the timer.
 *
 * @since 1.2.0.dp6
 *
 * This operation makes sense for periodic timers or if one need to cancel
 * regular timer before it will be triggered.
 *
 * @example Cancel periodic timer
 *   n = 1
 *   c.run do
 *     tm = c.create_periodic_timer(500000) do
 *       c.incr("foo") do
 *         if n == 5
 *           tm.cancel
 *         else
 *           n += 1
 *         end
 *       end
 *     end
 *   end
 *
 * @return [String]
 */
    static VALUE
cb_timer_cancel(VALUE self)
{
    struct timer_st *tm = DATA_PTR(self);
    libcouchbase_timer_destroy(tm->bucket->handle, tm->timer);
    return self;
}

    static VALUE
trigger_timer(VALUE timer)
{
    struct timer_st *tm = DATA_PTR(timer);
    return cb_proc_call(tm->callback, 1, timer);
}

    static void
timer_callback(libcouchbase_timer_t timer, libcouchbase_t instance,
        const void *cookie)
{
    struct timer_st *tm = (struct timer_st *)cookie;
    int error = 0;

    rb_protect(trigger_timer, tm->self, &error);
    if (error) {
        libcouchbase_timer_destroy(instance, timer);
    }
    (void)cookie;
}

/*
 * Initialize new Timer
 *
 * @since 1.2.0
 *
 * The timers could used to trigger reccuring events or implement timeouts.
 * The library will call given block after time interval pass.
 *
 * @param bucket [Bucket] the connection object
 * @param interval [Fixnum] the interval in microseconds
 * @param options [Hash]
 * @option options [Boolean] :periodic (false) set it to +true+ if the timer
 *   should be triggered until it will be canceled.
 *
 * @yieldparam [Timer] timer the current timer
 *
 * @example Create regular timer for 0.5 second
 *   c.run do
 *     Couchbase::Timer.new(c, 500000) do
 *       puts "ding-dong"
 *     end
 *   end
 *
 * @example Create periodic timer
 *   n = 10
 *   c.run do
 *     Couchbase::Timer.new(c, 500000, :periodic => true) do |tm|
 *       puts "#{n}"
 *       n -= 1
 *       tm.cancel if n.zero?
 *     end
 *   end
 *
 *
 * @return [Couchbase::Timer]
 */
    static VALUE
cb_timer_init(int argc, VALUE *argv, VALUE self)
{
    struct timer_st *tm = DATA_PTR(self);
    VALUE bucket, opts, timeout, exc, cb;
    libcouchbase_error_t err;

    rb_need_block();
    rb_scan_args(argc, argv, "21&", &bucket, &timeout, &opts, &cb);

    if (CLASS_OF(bucket) != cBucket) {
        rb_raise(rb_eTypeError, "wrong argument type (expected Couchbase::Bucket)");
    }
    tm->self = self;
    tm->callback = cb;
    tm->usec = NUM2ULONG(timeout);
    tm->bucket = DATA_PTR(bucket);
    if (opts != Qnil) {
        Check_Type(opts, T_HASH);
        tm->periodic = RTEST(rb_hash_aref(opts, sym_periodic));
    }
    tm->timer = libcouchbase_timer_create(tm->bucket->handle, tm, tm->usec,
            tm->periodic, timer_callback, &err);
    exc = cb_check_error(err, "failed to attach the timer", Qnil);
    if (exc != Qnil) {
        rb_exc_raise(exc);
    }

    return self;
}

    void
cb_couch_request_free(void *ptr)
{
    struct couch_request_st *request = ptr;
    if (request) {
        request->running = 0;
        if (TYPE(request->bucket_obj) == T_DATA && !request->completed) {
            libcouchbase_cancel_http_request(request->request);
        }
        xfree(request->path);
        xfree(request->body);
        xfree(request);
    }
}

    void
cb_couch_request_mark(void *ptr)
{
    struct couch_request_st *request = ptr;
    if (request) {
        rb_gc_mark(request->on_body_callback);
    }
}

    static VALUE
cb_couch_request_alloc(VALUE klass)
{
    VALUE obj;
    struct couch_request_st *request;

    /* allocate new bucket struct and set it to zero */
    obj = Data_Make_Struct(klass, struct couch_request_st, cb_couch_request_mark,
            cb_couch_request_free, request);
    return obj;
}

/*
 * Returns a string containing a human-readable representation of the
 * CouchRequest.
 *
 * @since 1.2.0
 *
 * @return [String]
 */
    static VALUE
cb_couch_request_inspect(VALUE self)
{
    VALUE str;
    struct couch_request_st *req = DATA_PTR(self);
    char buf[200];

    str = rb_str_buf_new2("#<");
    rb_str_buf_cat2(str, rb_obj_classname(self));
    snprintf(buf, 20, ":%p \"", (void *)self);
    rb_str_buf_cat2(str, buf);
    rb_str_buf_cat2(str, req->path);
    snprintf(buf, 100, "\" chunked:%s>", req->chunked ? "true" : "false");
    rb_str_buf_cat2(str, buf);

    return str;
}

/*
 * Initialize new CouchRequest
 *
 * @since 1.2.0
 *
 * @return [Bucket::CouchRequest]
 */
    static VALUE
cb_couch_request_init(int argc, VALUE *argv, VALUE self)
{
    struct couch_request_st *request = DATA_PTR(self);
    VALUE bucket, path, opts, on_body, mm, pp, body;
    rb_scan_args(argc, argv, "22", &bucket, &pp, &opts, &on_body);

    if (NIL_P(on_body) && rb_block_given_p()) {
        on_body = rb_block_proc();
    }
    path = StringValue(pp);	/* convert path to string */
    request->path = strdup(RSTRING_PTR(path));
    request->npath = RSTRING_LEN(path);
    request->on_body_callback = on_body;
    if (CLASS_OF(bucket) != cBucket) {
        rb_raise(rb_eTypeError, "wrong argument type (expected Couchbase::Bucket)");
    }
    request->bucket = DATA_PTR(bucket);
    request->bucket_obj = bucket;
    request->method = LIBCOUCHBASE_HTTP_METHOD_GET;
    request->extended = Qfalse;
    request->chunked = Qfalse;
    if (opts != Qnil) {
        Check_Type(opts, T_HASH);
        request->extended = RTEST(rb_hash_aref(opts, sym_extended));
        request->chunked = RTEST(rb_hash_aref(opts, sym_chunked));
        if ((mm = rb_hash_aref(opts, sym_method)) != Qnil) {
            if (mm == sym_get) {
                request->method = LIBCOUCHBASE_HTTP_METHOD_GET;
            } else if (mm == sym_post) {
                request->method = LIBCOUCHBASE_HTTP_METHOD_POST;
            } else if (mm == sym_put) {
                request->method = LIBCOUCHBASE_HTTP_METHOD_PUT;
            } else if (mm == sym_delete) {
                request->method = LIBCOUCHBASE_HTTP_METHOD_DELETE;
            } else {
                rb_raise(rb_eArgError, "unsupported HTTP method");
            }
        }
        if ((body = rb_hash_aref(opts, sym_body)) != Qnil) {
            Check_Type(body, T_STRING);
            request->body = strdup(RSTRING_PTR(body));
            request->nbody = RSTRING_LEN(body);
        }
    }

    return self;
}

/*
 * Set +on_body+ callback
 *
 * @since 1.2.0
 */
    static VALUE
cb_couch_request_on_body(VALUE self)
{
    struct couch_request_st *request = DATA_PTR(self);
    VALUE old = request->on_body_callback;
    if (rb_block_given_p()) {
        request->on_body_callback = rb_block_proc();
    }
    return old;
}

/*
 * Execute {Bucket::CouchRequest}
 *
 * @since 1.2.0
 */
    static VALUE
cb_couch_request_perform(VALUE self)
{
    struct couch_request_st *req = DATA_PTR(self);
    struct context_st *ctx;
    VALUE rv, exc;
    libcouchbase_error_t err;
    struct bucket_st *bucket;

    ctx = xcalloc(1, sizeof(struct context_st));
    if (ctx == NULL) {
        rb_raise(eClientNoMemoryError, "failed to allocate memory");
    }
    rv = Qnil;
    ctx->rv = &rv;
    ctx->bucket = bucket = req->bucket;
    ctx->proc = rb_block_given_p() ? rb_block_proc() : req->on_body_callback;
    ctx->extended = req->extended;
    ctx->request = req;

    req->request = libcouchbase_make_couch_request(bucket->handle, (const void *)ctx,
            req->path, req->npath, req->body, req->nbody, req->method,
            req->chunked, &err);
    exc = cb_check_error(err, "failed to schedule document request",
            rb_str_new(req->path, req->npath));
    if (exc != Qnil) {
        xfree(ctx);
        rb_exc_raise(exc);
    }
    req->running = 1;
    req->ctx = ctx;
    if (bucket->async) {
        return Qnil;
    } else {
        libcouchbase_wait(bucket->handle);
        if (req->completed) {
            exc = ctx->exception;
            xfree(ctx);
            if (exc != Qnil) {
                cb_gc_unprotect(bucket, exc);
                rb_exc_raise(exc);
            }
            return rv;
        } else {
            return Qnil;
        }
    }
    return Qnil;
}

    static VALUE
cb_couch_request_pause(VALUE self)
{
    struct couch_request_st *req = DATA_PTR(self);
    req->bucket->io->stop_event_loop(req->bucket->io);
    return Qnil;
}

    static VALUE
cb_couch_request_continue(VALUE self)
{
    VALUE exc, *rv;
    struct couch_request_st *req = DATA_PTR(self);

    if (req->running) {
        libcouchbase_wait(req->bucket->handle);
        if (req->completed) {
            exc = req->ctx->exception;
            rv = req->ctx->rv;
            xfree(req->ctx);
            if (exc != Qnil) {
                cb_gc_unprotect(req->bucket, exc);
                rb_exc_raise(exc);
            }
            return *rv;
        }
    } else {
        cb_couch_request_perform(self);
    }
    return Qnil;
}

/* Document-method: path
 *
 * @since 1.2.0
 *
 * @return [String] the requested path
 */
    static VALUE
cb_couch_request_path_get(VALUE self)
{
    struct couch_request_st *req = DATA_PTR(self);
    return rb_str_new2(req->path);
}

/* Document-method: chunked
 *
 * @since 1.2.0
 *
 * @return [Boolean] +false+ if library should collect whole response before
 *   yielding, +true+ if the client is ready to handle response in chunks.
 */
    static VALUE
cb_couch_request_chunked_get(VALUE self)
{
    struct couch_request_st *req = DATA_PTR(self);
    return RTEST(req->chunked);
}

/* Document-method: extended
 *
 * @since 1.2.0
 *
 * @return [Boolean] if +false+ the callbacks should receive just the data,
 *   and {Couchbase::Result} instance otherwise.
 */
    static VALUE
cb_couch_request_extended_get(VALUE self)
{
    struct couch_request_st *req = DATA_PTR(self);
    return RTEST(req->extended);
}

/* Document-method: make_couch_request(path, options = {})
 *
 * @since 1.2.0
 *
 * @param path [String]
 * @param options [Hash]
 * @option options [Boolean] :extended (false) set it to +true+ if the
 *   {Couchbase::Result} object needed. The response chunk will be
 *   accessible through +#value+ attribute.
 * @yieldparam [String,Couchbase::Result] res the response chunk if the
 *   :extended option is +false+ and result object otherwise
 *
 * @return [Couchbase::Bucket::CouchRequest]
 */
    static VALUE
cb_bucket_make_couch_request(int argc, VALUE *argv, VALUE self)
{
    VALUE args[4]; /* bucket, path, options, block */

    args[0] = self;
    rb_scan_args(argc, argv, "11&", &args[1], &args[2], &args[3]);

    return rb_class_new_instance(4, args, cCouchRequest);
}

/* Ruby Extension initializer */
    void
Init_couchbase_ext(void)
{
    mMultiJson = rb_const_get(rb_cObject, rb_intern("MultiJson"));
    mURI = rb_const_get(rb_cObject, rb_intern("URI"));
    mMarshal = rb_const_get(rb_cObject, rb_intern("Marshal"));
    mCouchbase = rb_define_module("Couchbase");

    mError = rb_define_module_under(mCouchbase, "Error");
    /* Document-class: Couchbase::Error::Base
     * The base error class
     *
     * @since 1.0.0
     */
    eBaseError = rb_define_class_under(mError, "Base", rb_eStandardError);
    /* Document-class: Couchbase::Error::Auth
     * Authentication error
     *
     * @since 1.0.0
     */
    eAuthError = rb_define_class_under(mError, "Auth", eBaseError);
    /* Document-class: Couchbase::Error::BucketNotFound
     * The given bucket not found in the cluster
     *
     * @since 1.0.0
     */
    eBucketNotFoundError = rb_define_class_under(mError, "BucketNotFound", eBaseError);
    /* Document-class: Couchbase::Error::Busy
     * The cluster is too busy now. Try again later
     *
     * @since 1.0.0
     */
    eBusyError = rb_define_class_under(mError, "Busy", eBaseError);
    /* Document-class: Couchbase::Error::DeltaBadval
     * The given value is not a number
     *
     * @since 1.0.0
     */
    eDeltaBadvalError = rb_define_class_under(mError, "DeltaBadval", eBaseError);
    /* Document-class: Couchbase::Error::Internal
     * Internal error
     *
     * @since 1.0.0
     */
    eInternalError = rb_define_class_under(mError, "Internal", eBaseError);
    /* Document-class: Couchbase::Error::Invalid
     * Invalid arguments
     *
     * @since 1.0.0
     */
    eInvalidError = rb_define_class_under(mError, "Invalid", eBaseError);
    /* Document-class: Couchbase::Error::KeyExists
     * Key already exists
     *
     * @since 1.0.0
     */
    eKeyExistsError = rb_define_class_under(mError, "KeyExists", eBaseError);
    /* Document-class: Couchbase::Error::Libcouchbase
     * Generic error
     *
     * @since 1.0.0
     */
    eLibcouchbaseError = rb_define_class_under(mError, "Libcouchbase", eBaseError);
    /* Document-class: Couchbase::Error::Libevent
     * Problem using libevent
     *
     * @since 1.0.0
     */
    eLibeventError = rb_define_class_under(mError, "Libevent", eBaseError);
    /* Document-class: Couchbase::Error::Network
     * Network error
     *
     * @since 1.0.0
     */
    eNetworkError = rb_define_class_under(mError, "Network", eBaseError);
    /* Document-class: Couchbase::Error::NoMemory
     * Out of memory error (on Server)
     *
     * @since 1.0.0
     */
    eNoMemoryError = rb_define_class_under(mError, "NoMemory", eBaseError);
    /* Document-class: Couchbase::Error::ClientNoMemory
     * Out of memory error (on Client)
     *
     * @since 1.2.0.dp6
     */
    eClientNoMemoryError = rb_define_class_under(mError, "ClientNoMemory", eBaseError);
    /* Document-class: Couchbase::Error::NotFound
     * No such key
     *
     * @since 1.0.0
     */
    eNotFoundError = rb_define_class_under(mError, "NotFound", eBaseError);
    /* Document-class: Couchbase::Error::NotMyVbucket
     * The vbucket is not located on this server
     *
     * @since 1.0.0
     */
    eNotMyVbucketError = rb_define_class_under(mError, "NotMyVbucket", eBaseError);
    /* Document-class: Couchbase::Error::NotStored
     * Not stored
     *
     * @since 1.0.0
     */
    eNotStoredError = rb_define_class_under(mError, "NotStored", eBaseError);
    /* Document-class: Couchbase::Error::NotSupported
     * Not supported
     *
     * @since 1.0.0
     */
    eNotSupportedError = rb_define_class_under(mError, "NotSupported", eBaseError);
    /* Document-class: Couchbase::Error::Range
     * Invalid range
     *
     * @since 1.0.0
     */
    eRangeError = rb_define_class_under(mError, "Range", eBaseError);
    /* Document-class: Couchbase::Error::TemporaryFail
     * Temporary failure. Try again later
     *
     * @since 1.0.0
     */
    eTmpFailError = rb_define_class_under(mError, "TemporaryFail", eBaseError);
    /* Document-class: Couchbase::Error::TooBig
     * Object too big
     *
     * @since 1.0.0
     */
    eTooBigError = rb_define_class_under(mError, "TooBig", eBaseError);
    /* Document-class: Couchbase::Error::UnknownCommand
     * Unknown command
     *
     * @since 1.0.0
     */
    eUnknownCommandError = rb_define_class_under(mError, "UnknownCommand", eBaseError);
    /* Document-class: Couchbase::Error::UnknownHost
     * Unknown host
     *
     * @since 1.0.0
     */
    eUnknownHostError = rb_define_class_under(mError, "UnknownHost", eBaseError);
    /* Document-class: Couchbase::Error::ValueFormat
     * Failed to decode or encode value
     *
     * @since 1.0.0
     */
    eValueFormatError = rb_define_class_under(mError, "ValueFormat", eBaseError);
    /* Document-class: Couchbase::Error::Protocol
     * Protocol error
     *
     * @since 1.0.0
     */
    eProtocolError = rb_define_class_under(mError, "Protocol", eBaseError);
    /* Document-class: Couchbase::Error::Timeout
     * Timeout error
     *
     * @since 1.1.0
     */
    eTimeoutError = rb_define_class_under(mError, "Timeout", eBaseError);
    /* Document-class: Couchbase::Error::Connect
     * Connect error
     *
     * @since 1.1.0
     */
    eConnectError = rb_define_class_under(mError, "Connect", eBaseError);

    /* Document-method: error
     *
     * The underlying libcouchbase library could return one of the following
     * error codes. The ruby client will wrap these errors into appropriate
     * exception class, derived from {Couchbase::Error::Base}.
     *
     * 0x00 :: LIBCOUCHBASE_SUCCESS (Success)
     * 0x01 :: LIBCOUCHBASE_AUTH_CONTINUE (Continue authentication)
     * 0x02 :: LIBCOUCHBASE_AUTH_ERROR (Authentication error)
     * 0x03 :: LIBCOUCHBASE_DELTA_BADVAL (Not a number)
     * 0x04 :: LIBCOUCHBASE_E2BIG (Object too big)
     * 0x05 :: LIBCOUCHBASE_EBUSY (Too busy. Try again later)
     * 0x06 :: LIBCOUCHBASE_EINTERNAL (Internal error)
     * 0x07 :: LIBCOUCHBASE_EINVAL (Invalid arguments)
     * 0x08 :: LIBCOUCHBASE_ENOMEM (Out of memory)
     * 0x09 :: LIBCOUCHBASE_ERANGE (Invalid range)
     * 0x0a :: LIBCOUCHBASE_ERROR (Generic error)
     * 0x0b :: LIBCOUCHBASE_ETMPFAIL (Temporary failure. Try again later)
     * 0x0c :: LIBCOUCHBASE_KEY_EEXISTS (Key exists (with a different CAS value))
     * 0x0d :: LIBCOUCHBASE_KEY_ENOENT (No such key)
     * 0x0e :: LIBCOUCHBASE_LIBEVENT_ERROR (Problem using libevent)
     * 0x0f :: LIBCOUCHBASE_NETWORK_ERROR (Network error)
     * 0x10 :: LIBCOUCHBASE_NOT_MY_VBUCKET (The vbucket is not located on this server)
     * 0x11 :: LIBCOUCHBASE_NOT_STORED (Not stored)
     * 0x12 :: LIBCOUCHBASE_NOT_SUPPORTED (Not supported)
     * 0x13 :: LIBCOUCHBASE_UNKNOWN_COMMAND (Unknown command)
     * 0x14 :: LIBCOUCHBASE_UNKNOWN_HOST (Unknown host)
     * 0x15 :: LIBCOUCHBASE_PROTOCOL_ERROR (Protocol error)
     * 0x16 :: LIBCOUCHBASE_ETIMEDOUT (Operation timed out)
     * 0x17 :: LIBCOUCHBASE_CONNECT_ERROR (Connection failure)
     * 0x18 :: LIBCOUCHBASE_BUCKET_ENOENT (No such bucket)
     * 0x18 :: LIBCOUCHBASE_CLIENT_ENOMEM (Out of memory on the client)
     *
     * @since 1.0.0
     *
     * @return [Fixnum] the error code from libcouchbase
     */
    rb_define_attr(eBaseError, "error", 1, 0);
    id_iv_error = rb_intern("@error");
    /* Document-method: key
     *
     * @since 1.0.0
     *
     * @return [String] the key which generated error */
    rb_define_attr(eBaseError, "key", 1, 0);
    id_iv_key = rb_intern("@key");
    /* Document-method: cas
     *
     * @since 1.0.0
     *
     * @return [Fixnum] the version of the key (+nil+ unless accessible) */
    rb_define_attr(eBaseError, "cas", 1, 0);
    id_iv_cas = rb_intern("@cas");
    /* Document-method: operation
     *
     * @since 1.0.0
     *
     * @return [Symbol] the operation (+nil+ unless accessible) */
    rb_define_attr(eBaseError, "operation", 1, 0);
    id_iv_operation = rb_intern("@operation");

    /* Document-class: Couchbase::Result
     *
     * The object which yielded to asynchronous callbacks
     *
     * @since 1.0.0
     */
    cResult = rb_define_class_under(mCouchbase, "Result", rb_cObject);
    rb_define_method(cResult, "inspect", cb_result_inspect, 0);
    rb_define_method(cResult, "success?", cb_result_success_p, 0);
    /* Document-method: operation
     *
     * @since 1.0.0
     *
     * @return [Symbol]
     */
    rb_define_attr(cResult, "operation", 1, 0);
    /* Document-method: error
     *
     * @since 1.0.0
     *
     * @return [Couchbase::Error::Base]
     */
    rb_define_attr(cResult, "error", 1, 0);
    /* Document-method: key
     *
     * @since 1.0.0
     *
     * @return [String]
     */
    rb_define_attr(cResult, "key", 1, 0);
    id_iv_key = rb_intern("@key");
    /* Document-method: value
     *
     * @since 1.0.0
     *
     * @return [String]
     */
    rb_define_attr(cResult, "value", 1, 0);
    id_iv_value = rb_intern("@value");
    /* Document-method: cas
     *
     * @since 1.0.0
     *
     * @return [Fixnum]
     */
    rb_define_attr(cResult, "cas", 1, 0);
    id_iv_cas = rb_intern("@cas");
    /* Document-method: flags
     *
     * @since 1.0.0
     *
     * @return [Fixnum]
     */
    rb_define_attr(cResult, "flags", 1, 0);
    id_iv_flags = rb_intern("@flags");
    /* Document-method: node
     *
     * @since 1.0.0
     *
     * @return [String]
     */
    rb_define_attr(cResult, "node", 1, 0);
    id_iv_node = rb_intern("@node");
    /* Document-method: completed
     * In {Bucket::CouchRequest} operations used to mark the final call
     * @return [Boolean] */
    rb_define_attr(cResult, "completed", 1, 0);
    rb_define_alias(cResult, "completed?", "completed");
    id_iv_completed = rb_intern("@completed");
    /* Document-method: status
     *
     * @since 1.2.0.dp6
     *
     * @see Bucket#observe
     *
     * Status of the key. Possible values:
     * +:found+ :: Key found in cache, but not yet persisted
     * +:persisted+ :: Key found and persisted
     * +:not_found+ :: Key not found
     *
     * @return [Symbol]
     */
    rb_define_attr(cResult, "status", 1, 0);
    id_iv_status = rb_intern("@status");
    /* Document-method: from_master
     *
     * @since 1.2.0.dp6
     *
     * @see Bucket#observe
     *
     * True if key stored on master
     * @return [Boolean]
     */
    rb_define_attr(cResult, "from_master", 1, 0);
    rb_define_alias(cResult, "from_master?", "from_master");
    id_iv_from_master = rb_intern("@from_master");
    /* Document-method: time_to_persist
     *
     * @since 1.2.0.dp6
     *
     * @see Bucket#observe
     *
     * Average time needed to persist key on the disk (zero if unavailable)
     * @return [Fixnum]
     */
    rb_define_attr(cResult, "time_to_persist", 1, 0);
    rb_define_alias(cResult, "ttp", "time_to_persist");
    id_iv_time_to_persist = rb_intern("@time_to_persist");
    /* Document-method: time_to_persist
     *
     * @since 1.2.0.dp6
     *
     * @see Bucket#observe
     *
     * Average time needed to replicate key on the disk (zero if unavailable)
     * @return [Fixnum]
     */
    rb_define_attr(cResult, "time_to_replicate", 1, 0);
    rb_define_alias(cResult, "ttr", "time_to_replicate");
    id_iv_time_to_replicate = rb_intern("@time_to_replicate");

    /* Document-class: Couchbase::Bucket
     *
     * This class in charge of all stuff connected to communication with
     * Couchbase.
     *
     * @since 1.0.0
     */
    cBucket = rb_define_class_under(mCouchbase, "Bucket", rb_cObject);

    /* 0x03: Bitmask for flag bits responsible for format */
    rb_define_const(cBucket, "FMT_MASK", INT2FIX(FMT_MASK));
    /* 0x00: Document format. The (default) format supports most of ruby
     * types which could be mapped to JSON data (hashes, arrays, strings,
     * numbers). Future version will be able to run map/reduce queries on
     * the values in the document form (hashes). */
    rb_define_const(cBucket, "FMT_DOCUMENT", INT2FIX(FMT_DOCUMENT));
    /* 0x01:  Marshal format. The format which supports transparent
     * serialization of ruby objects with standard <tt>Marshal.dump</tt> and
     * <tt>Marhal.load</tt> methods. */
    rb_define_const(cBucket, "FMT_MARSHAL", INT2FIX(FMT_MARSHAL));
    /* 0x02:  Plain format. The format which force client don't apply any
     * conversions to the value, but it should be passed as String. It
     * could be useful for building custom algorithms or formats. For
     * example implement set:
     * http://dustin.github.com/2011/02/17/memcached-set.html */
    rb_define_const(cBucket, "FMT_PLAIN", INT2FIX(FMT_PLAIN));

    rb_define_alloc_func(cBucket, cb_bucket_alloc);
    rb_define_method(cBucket, "initialize", cb_bucket_init, -1);
    rb_define_method(cBucket, "initialize_copy", cb_bucket_init_copy, 1);
    rb_define_method(cBucket, "inspect", cb_bucket_inspect, 0);

    rb_define_method(cBucket, "add", cb_bucket_add, -1);
    rb_define_method(cBucket, "append", cb_bucket_append, -1);
    rb_define_method(cBucket, "prepend", cb_bucket_prepend, -1);
    rb_define_method(cBucket, "replace", cb_bucket_replace, -1);
    rb_define_method(cBucket, "set", cb_bucket_set, -1);
    rb_define_method(cBucket, "get", cb_bucket_get, -1);
    rb_define_method(cBucket, "run", cb_bucket_run, -1);
    rb_define_method(cBucket, "stop", cb_bucket_stop, 0);
    rb_define_method(cBucket, "touch", cb_bucket_touch, -1);
    rb_define_method(cBucket, "delete", cb_bucket_delete, -1);
    rb_define_method(cBucket, "stats", cb_bucket_stats, -1);
    rb_define_method(cBucket, "flush", cb_bucket_flush, 0);
    rb_define_method(cBucket, "version", cb_bucket_version, 0);
    rb_define_method(cBucket, "incr", cb_bucket_incr, -1);
    rb_define_method(cBucket, "decr", cb_bucket_decr, -1);
    rb_define_method(cBucket, "disconnect", cb_bucket_disconnect, 0);
    rb_define_method(cBucket, "reconnect", cb_bucket_reconnect, -1);
    rb_define_method(cBucket, "make_couch_request", cb_bucket_make_couch_request, -1);
    rb_define_method(cBucket, "observe", cb_bucket_observe, -1);

    rb_define_alias(cBucket, "decrement", "decr");
    rb_define_alias(cBucket, "increment", "incr");

    rb_define_alias(cBucket, "[]", "get");
    rb_define_alias(cBucket, "[]=", "set");
    rb_define_method(cBucket, "[]=", cb_bucket_aset, -1);

    rb_define_method(cBucket, "connected?", cb_bucket_connected_p, 0);
    rb_define_method(cBucket, "async?", cb_bucket_async_p, 0);

    /* Document-method: quiet
     * Flag specifying behaviour for operations on missing keys
     *
     * @since 1.0.0
     *
     * If it is +true+, the operations will silently return +nil+ or +false+
     * instead of raising {Couchbase::Error::NotFound}.
     *
     * @example Hiding cache miss (considering "miss" key is not stored)
     *   connection.quiet = true
     *   connection.get("miss")     #=> nil
     *
     * @example Raising errors on miss (considering "miss" key is not stored)
     *   connection.quiet = false
     *   connection.get("miss")     #=> will raise Couchbase::Error::NotFound
     *
     * @return [true, false] */
    /* rb_define_attr(cBucket, "quiet", 1, 1); */
    rb_define_method(cBucket, "quiet", cb_bucket_quiet_get, 0);
    rb_define_method(cBucket, "quiet=", cb_bucket_quiet_set, 1);
    rb_define_alias(cBucket, "quiet?", "quiet");

    /* Document-method: default_flags
     * Default flags for new values.
     *
     * @since 1.0.0
     *
     * The library reserves last two lower bits to store the format of the
     * value. The can be masked via FMT_MASK constant.
     *
     * @example Selecting format bits
     *   connection.default_flags & Couchbase::Bucket::FMT_MASK
     *
     * @example Set user defined bits
     *   connection.default_flags |= 0x6660
     *
     * @note Amending format bit will also change #default_format value
     *
     * @return [Fixnum] the effective flags */
    /* rb_define_attr(cBucket, "default_flags", 1, 1); */
    rb_define_method(cBucket, "default_flags", cb_bucket_default_flags_get, 0);
    rb_define_method(cBucket, "default_flags=", cb_bucket_default_flags_set, 1);

    /* Document-method: default_format
     * Default format for new values.
     *
     * @since 1.0.0
     *
     * @see http://couchbase.com/docs/couchbase-manual-2.0/couchbase-views-datastore.html
     *
     * It uses flags field to store the format. It accepts either the Symbol
     * (+:document+, +:marshal+, +:plain+) or Fixnum (use constants
     * FMT_DOCUMENT, FMT_MARSHAL, FMT_PLAIN) and silently ignores all
     * other value.
     *
     * Here is some notes regarding how to choose the format:
     *
     * * <tt>:document</tt> (default) format supports most of ruby types
     *   which could be mapped to JSON data (hashes, arrays, strings,
     *   numbers). Future version will be able to run map/reduce queries on
     *   the values in the document form (hashes).
     *
     * * <tt>:plain</tt> format if you no need any conversions to be applied
     *   to your data, but your data should be passed as String. It could be
     *   useful for building custom algorithms or formats. For example
     *   implement set: http://dustin.github.com/2011/02/17/memcached-set.html
     *
     * * <tt>:marshal</tt> format if you'd like to transparently serialize
     *   your ruby object with standard <tt>Marshal.dump</tt> and
     *   <tt>Marhal.load</tt> methods.
     *
     * @example Selecting plain format using symbol
     *   connection.format = :document
     *
     * @example Selecting plain format using Fixnum constant
     *   connection.format = Couchbase::Bucket::FMT_PLAIN
     *
     * @note Amending default_format will also change #default_flags value
     *
     * @return [Symbol] the effective format */
    /* rb_define_attr(cBucket, "default_format", 1, 1); */
    rb_define_method(cBucket, "default_format", cb_bucket_default_format_get, 0);
    rb_define_method(cBucket, "default_format=", cb_bucket_default_format_set, 1);

    /* Document-method: timeout
     *
     * @since 1.1.0
     *
     * @return [Fixnum] The timeout for the operations in microseconds. The
     *   client will raise {Couchbase::Error::Timeout} exception for all
     *   commands which weren't completed in given timeslot. */
    /* rb_define_attr(cBucket, "timeout", 1, 1); */
    rb_define_method(cBucket, "timeout", cb_bucket_timeout_get, 0);
    rb_define_method(cBucket, "timeout=", cb_bucket_timeout_set, 1);

    /* Document-method: key_prefix
     *
     * @since 1.2.0.dp5
     *
     * @return [String] The library will prepend +key_prefix+ to each key to
     *   provide simple namespacing. */
    /* rb_define_attr(cBucket, "key_prefix", 1, 1); */
    rb_define_method(cBucket, "key_prefix", cb_bucket_key_prefix_get, 0);
    rb_define_method(cBucket, "key_prefix=", cb_bucket_key_prefix_set, 1);

    /* Document-method: on_error
     * Error callback for asynchronous mode.
     *
     * @since 1.0.0
     *
     * This callback is using to deliver exceptions in asynchronous mode.
     *
     * @yieldparam [Symbol] op The operation caused the error
     * @yieldparam [String] key The key which cause the error or +nil+
     * @yieldparam [Exception] exc The exception instance
     *
     * @example Using lambda syntax
     *   connection = Couchbase.new(:async => true)
     *   connection.on_error = lambda {|op, key, exc| ... }
     *   connection.run do |conn|
     *     conn.set("foo", "bar")
     *   end
     *
     * @example Using block syntax
     *   connection = Couchbase.new(:async => true)
     *   connection.on_error {|op, key, exc| ... }
     *   ...
     *
     * @return [Proc] the effective callback */
    /* rb_define_attr(cBucket, "on_error", 1, 1); */
    rb_define_method(cBucket, "on_error", cb_bucket_on_error_get, 0);
    rb_define_method(cBucket, "on_error=", cb_bucket_on_error_set, 1);

    /* Document-method: url
     *
     * The config url for this connection.
     *
     * Generally it is the bootstrap URL, but it could be different after
     * cluster upgrade. This url is used to fetch the cluster
     * configuration.
     *
     * @since 1.0.0
     */
    /* rb_define_attr(cBucket, "url", 1, 0); */
    rb_define_method(cBucket, "url", cb_bucket_url_get, 0);
    /* Document-method: hostname
     *
     * The hostname of the current node
     *
     * @see Bucket#url
     *
     * @since 1.0.0
     */
    /* rb_define_attr(cBucket, "hostname", 1, 0); */
    rb_define_method(cBucket, "hostname", cb_bucket_hostname_get, 0);
    /* Document-method: port
     *
     * The port of the current node
     *
     * @see Bucket#url
     *
     * @since 1.0.0
     */
    /* rb_define_attr(cBucket, "port", 1, 0); */
    rb_define_method(cBucket, "port", cb_bucket_port_get, 0);
    /* Document-method: authority
     *
     * The authority ("hostname:port") of the current node
     *
     * @see Bucket#url
     *
     * @since 1.0.0
     */
    /* rb_define_attr(cBucket, "authority", 1, 0); */
    rb_define_method(cBucket, "authority", cb_bucket_authority_get, 0);
    /* Document-method: bucket
     *
     * The bucket name of the current connection
     *
     * @see Bucket#url
     *
     * @since 1.0.0
     */
    /* rb_define_attr(cBucket, "bucket", 1, 0); */
    rb_define_method(cBucket, "bucket", cb_bucket_bucket_get, 0);
    rb_define_alias(cBucket, "name", "bucket");
    /* Document-method: pool
     *
     * The pool name of the current connection
     *
     * @see Bucket#url
     *
     * @since 1.0.0
     */
    /* rb_define_attr(cBucket, "pool", 1, 0); */
    rb_define_method(cBucket, "pool", cb_bucket_pool_get, 0);
    /* Document-method: username
     *
     * The user name used to connect to the cluster
     *
     * @see Bucket#url
     *
     * @since 1.0.0
     */
    /* rb_define_attr(cBucket, "username", 1, 0); */
    rb_define_method(cBucket, "username", cb_bucket_username_get, 0);
    /* Document-method: password
     *
     * The password used to connect to the cluster
     *
     * @since 1.0.0
     */
    /* rb_define_attr(cBucket, "password", 1, 0); */
    rb_define_method(cBucket, "password", cb_bucket_password_get, 0);
    /* Document-method: environment
     *
     * The environment of the connection (+:development+ or +:production+)
     *
     * @since 1.2.0
     *
     * @returns [Symbol]
     */
    /* rb_define_attr(cBucket, "environment", 1, 0); */
    rb_define_method(cBucket, "environment", cb_bucket_environment_get, 0);
    /* Document-method: num_replicas
     *
     * @since 1.2.0.dp6
     *
     * The numbers of the replicas for each node in the cluster
     *
     * @returns [Fixnum]
     */
    /* rb_define_attr(cBucket, "num_replicas", 1, 0); */
    rb_define_method(cBucket, "num_replicas", cb_bucket_num_replicas_get, 0);
    /* Document-method: default_observe_timeout
     *
     * @since 1.2.0.dp6
     *
     * The default timeout value for {Bucket#observe_and_wait} operation in
     * microseconds
     *
     * @returns [Fixnum]
     */
    /* rb_define_attr(cBucket, "default_observe_timeout", 1, 1); */
    rb_define_method(cBucket, "default_observe_timeout", cb_bucket_default_observe_timeout_get, 0);
    rb_define_method(cBucket, "default_observe_timeout=", cb_bucket_default_observe_timeout_set, 1);

    cCouchRequest = rb_define_class_under(cBucket, "CouchRequest", rb_cObject);
    rb_define_alloc_func(cCouchRequest, cb_couch_request_alloc);

    rb_define_method(cCouchRequest, "initialize", cb_couch_request_init, -1);
    rb_define_method(cCouchRequest, "inspect", cb_couch_request_inspect, 0);
    rb_define_method(cCouchRequest, "on_body", cb_couch_request_on_body, 0);
    rb_define_method(cCouchRequest, "perform", cb_couch_request_perform, 0);
    rb_define_method(cCouchRequest, "pause", cb_couch_request_pause, 0);
    rb_define_method(cCouchRequest, "continue", cb_couch_request_continue, 0);

    /* rb_define_attr(cCouchRequest, "path", 1, 0); */
    rb_define_method(cCouchRequest, "path", cb_couch_request_path_get, 0);
    /* rb_define_attr(cCouchRequest, "extended", 1, 0); */
    rb_define_method(cCouchRequest, "extended", cb_couch_request_extended_get, 0);
    rb_define_alias(cCouchRequest, "extended?", "extended");
    /* rb_define_attr(cCouchRequest, "chunked", 1, 0); */
    rb_define_method(cCouchRequest, "chunked", cb_couch_request_chunked_get, 0);
    rb_define_alias(cCouchRequest, "chunked?", "chunked");

    cTimer = rb_define_class_under(mCouchbase, "Timer", rb_cObject);
    rb_define_alloc_func(cTimer, cb_timer_alloc);
    rb_define_method(cTimer, "initialize", cb_timer_init, -1);
    rb_define_method(cTimer, "inspect", cb_timer_inspect, 0);
    rb_define_method(cTimer, "cancel", cb_timer_cancel, 0);

    /* Define symbols */
    id_arity = rb_intern("arity");
    id_call = rb_intern("call");
    id_delete = rb_intern("delete");
    id_dump = rb_intern("dump");
    id_dup = rb_intern("dup");
    id_flatten_bang = rb_intern("flatten!");
    id_has_key_p = rb_intern("has_key?");
    id_host = rb_intern("host");
    id_load = rb_intern("load");
    id_match = rb_intern("match");
    id_parse = rb_intern("parse");
    id_password = rb_intern("password");
    id_path = rb_intern("path");
    id_port = rb_intern("port");
    id_scheme = rb_intern("scheme");
    id_to_s = rb_intern("to_s");
    id_user = rb_intern("user");

    sym_add = ID2SYM(rb_intern("add"));
    sym_append = ID2SYM(rb_intern("append"));
    sym_assemble_hash = ID2SYM(rb_intern("assemble_hash"));
    sym_body = ID2SYM(rb_intern("body"));
    sym_bucket = ID2SYM(rb_intern("bucket"));
    sym_cas = ID2SYM(rb_intern("cas"));
    sym_chunked = ID2SYM(rb_intern("chunked"));
    sym_couch_request = ID2SYM(rb_intern("couch_request"));
    sym_create = ID2SYM(rb_intern("create"));
    sym_decrement = ID2SYM(rb_intern("decrement"));
    sym_default_flags = ID2SYM(rb_intern("default_flags"));
    sym_default_format = ID2SYM(rb_intern("default_format"));
    sym_default_ttl = ID2SYM(rb_intern("default_ttl"));
    sym_delete = ID2SYM(rb_intern("delete"));
    sym_delta = ID2SYM(rb_intern("delta"));
    sym_development = ID2SYM(rb_intern("development"));
    sym_document = ID2SYM(rb_intern("document"));
    sym_environment = ID2SYM(rb_intern("environment"));
    sym_extended = ID2SYM(rb_intern("extended"));
    sym_flags = ID2SYM(rb_intern("flags"));
    sym_flush = ID2SYM(rb_intern("flush"));
    sym_format = ID2SYM(rb_intern("format"));
    sym_found = ID2SYM(rb_intern("found"));
    sym_get = ID2SYM(rb_intern("get"));
    sym_hostname = ID2SYM(rb_intern("hostname"));
    sym_increment = ID2SYM(rb_intern("increment"));
    sym_initial = ID2SYM(rb_intern("initial"));
    sym_key_prefix = ID2SYM(rb_intern("key_prefix"));
    sym_lock = ID2SYM(rb_intern("lock"));
    sym_marshal = ID2SYM(rb_intern("marshal"));
    sym_method = ID2SYM(rb_intern("method"));
    sym_node_list = ID2SYM(rb_intern("node_list"));
    sym_not_found = ID2SYM(rb_intern("not_found"));
    sym_num_replicas = ID2SYM(rb_intern("num_replicas"));
    sym_observe = ID2SYM(rb_intern("observe"));
    sym_password = ID2SYM(rb_intern("password"));
    sym_periodic = ID2SYM(rb_intern("periodic"));
    sym_persisted = ID2SYM(rb_intern("persisted"));
    sym_plain = ID2SYM(rb_intern("plain"));
    sym_pool = ID2SYM(rb_intern("pool"));
    sym_port = ID2SYM(rb_intern("port"));
    sym_post = ID2SYM(rb_intern("post"));
    sym_prepend = ID2SYM(rb_intern("prepend"));
    sym_production = ID2SYM(rb_intern("production"));
    sym_put = ID2SYM(rb_intern("put"));
    sym_quiet = ID2SYM(rb_intern("quiet"));
    sym_replace = ID2SYM(rb_intern("replace"));
    sym_replica = ID2SYM(rb_intern("replica"));
    sym_send_threshold = ID2SYM(rb_intern("send_threshold"));
    sym_set = ID2SYM(rb_intern("set"));
    sym_stats = ID2SYM(rb_intern("stats"));
    sym_timeout = ID2SYM(rb_intern("timeout"));
    sym_touch = ID2SYM(rb_intern("touch"));
    sym_ttl = ID2SYM(rb_intern("ttl"));
    sym_username = ID2SYM(rb_intern("username"));
    sym_version = ID2SYM(rb_intern("version"));
}
