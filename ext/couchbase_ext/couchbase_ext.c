/* vim: ft=c et ts=8 sts=4 sw=4 cino=
 *
 *   Copyright 2011 Couchbase, Inc.
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
#include <event.h>
#include "couchbase_config.h"

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

typedef struct
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
    long seqno;
    VALUE default_format;    /* should update +default_flags+ on change */
    uint32_t default_flags;
    uint32_t default_ttl;
    VALUE exception;        /* error delivered by error_callback */
    VALUE on_error_proc;    /* is using to deliver errors in async mode */
} bucket_t;

typedef struct
{
    bucket_t* bucket;
    int extended;
    VALUE proc;
    void *rv;
    VALUE exception;
    int quiet;
    int arithm;           /* incr: +1, decr: -1, other: 0 */
} context_t;

struct key_traits
{
    VALUE keys_ary;
    size_t nkeys;
    char **keys;
    size_t *lens;
    time_t *ttls;
    int extended;
    int explicit_ttl;
    int quiet;
};

static VALUE mCouchbase, mError, mJSON, mMarshal, cBucket;
static VALUE object_space;

static ID  sym_async,
           sym_add,
           sym_append,
           sym_bucket,
           sym_cas,
           sym_create,
           sym_decrement,
           sym_default_flags,
           sym_default_format,
           sym_delete,
           sym_document,
           sym_extended,
           sym_flags,
           sym_flush,
           sym_format,
           sym_get,
           sym_hostname,
           sym_increment,
           sym_initial,
           sym_marshal,
           sym_password,
           sym_plain,
           sym_pool,
           sym_port,
           sym_prepend,
           sym_quiet,
           sym_replace,
           sym_set,
           sym_stats,
           sym_touch,
           sym_ttl,
           sym_username,
           id_arity,
           id_call,
           id_dump,
           id_flatten_bang,
           id_has_key_p,
           id_load,
           id_iv_async,
           id_iv_authority,
           id_iv_bucket,
           id_iv_cas,
           id_iv_default_flags,
           id_iv_default_format,
           id_iv_error,
           id_iv_hostname,
           id_iv_key,
           id_iv_on_error,
           id_iv_operation,
           id_iv_password,
           id_iv_pool,
           id_iv_port,
           id_iv_quiet,
           id_iv_url,
           id_iv_username;

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

    static VALUE
cb_proc_call(VALUE recv, int argc, ...)
{
    VALUE *argv;
    va_list ar;
    int arity;
    int ii;

    arity = FIX2INT(rb_funcall(recv, id_arity, 0));
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
        arity = 0;
        argv = NULL;
    }
    return rb_funcall2(recv, id_call, arity, argv);
}

/* Helper to conver return code from libcouchbase to meaningful exception.
 * Returns nil if the code considering successful and exception object
 * otherwise. Store given string to exceptions as message, and also
 * initialize +error+ attribute with given return code.  */
    static VALUE
cb_check_error(libcouchbase_error_t rc, const char *msg, VALUE key)
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
        case LIBCOUCHBASE_ERROR:
            /* fall through */
        default:
            klass = eLibcouchbaseError;
    }

    str = rb_str_buf_new2(msg ? msg : "");
    rb_str_buf_cat2(str, " (");
    if (key != Qnil) {
        snprintf(buf, 300, "key: '%s', ", RSTRING_PTR(key));
        rb_str_buf_cat2(str, buf);
    }
    snprintf(buf, 300, "error: %d)", rc);
    rb_str_buf_cat2(str, buf);
    exc = rb_exc_new3(klass, str);
    rb_ivar_set(exc, id_iv_error, INT2FIX(rc));
    rb_ivar_set(exc, id_iv_key, key);
    rb_ivar_set(exc, id_iv_cas, Qnil);
    rb_ivar_set(exc, id_iv_operation, Qnil);
    return exc;
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
            return rb_funcall(mJSON, id_dump, 1, val);
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
    uint32_t flags = ((uint32_t)args[1] & FMT_MASK);

    switch (flags) {
        case FMT_DOCUMENT:
            return rb_funcall(mJSON, id_load, 1, blob);
        case FMT_MARSHAL:
            return rb_funcall(mMarshal, id_load, 1, blob);
        case FMT_PLAIN:
            /* fall through */
        default:
            /* all other formats treated as plain */
            return blob;
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
decode_value(VALUE blob, uint32_t flags)
{
    VALUE val, args[2];

    /* first it must be bytestring */
    if (TYPE(blob) != T_STRING) {
        return Qundef;
    }
    args[0] = blob;
    args[1] = (VALUE)flags;
    val = rb_rescue(do_decode, (VALUE)args, coding_failed, 0);
    return val;
}

    static VALUE
unify_key(VALUE key)
{
    switch (TYPE(key)) {
        case T_STRING:
            return key;
        case T_SYMBOL:
            return rb_str_new2(rb_id2name(SYM2ID(key)));
        default:    /* call #to_str or raise error */
            return StringValue(key);
    }
}

    static int
cb_extract_keys_i(VALUE key, VALUE value, VALUE arg)
{
    struct key_traits *traits = (struct key_traits *)arg;
    key = unify_key(key);
    rb_ary_push(traits->keys_ary, key);
    traits->keys[traits->nkeys] = RSTRING_PTR(key);
    traits->lens[traits->nkeys] = RSTRING_LEN(key);
    traits->ttls[traits->nkeys] = NUM2ULONG(value);
    traits->nkeys++;
    return ST_CONTINUE;
}

    static long
cb_args_scan_keys(long argc, VALUE argv, bucket_t *bucket, struct key_traits *traits)
{
    VALUE arg, key, *keys_ptr, opts, ttl, ext;
    long nn = 0, ii;
    time_t exp;

    traits->keys_ary = rb_ary_new();
    traits->quiet = bucket->quiet;
    if (argc == 1) {
        arg = RARRAY_PTR(argv)[0];
        switch (TYPE(arg)) {
            case T_HASH:
                /* hash of key-ttl pairs */
                nn = RHASH_SIZE(arg);
                traits->keys = calloc(nn, sizeof(char *));
                traits->lens = calloc(nn, sizeof(size_t));
                traits->explicit_ttl = 1;
                traits->ttls = calloc(nn, sizeof(time_t));
                rb_hash_foreach(arg, cb_extract_keys_i, (VALUE)traits);
                break;
            case T_STRING:
            case T_SYMBOL:
                /* single key with default expiration */
                nn = traits->nkeys = 1;
                traits->keys = calloc(nn, sizeof(char *));
                traits->lens = calloc(nn, sizeof(size_t));
                traits->ttls = calloc(nn, sizeof(time_t));
                key = unify_key(arg);
                rb_ary_push(traits->keys_ary, key);
                traits->keys[0] = RSTRING_PTR(key);
                traits->lens[0] = RSTRING_LEN(key);
                traits->ttls[0] = bucket->default_ttl;
                break;
        }
    } else if (argc > 1) {
        /* keys with custom options */
        opts = RARRAY_PTR(argv)[argc-1];
        exp = bucket->default_ttl;
        ext = Qfalse;
        if (TYPE(opts) == T_HASH) {
            (void)rb_ary_pop(argv);
            if (RTEST(rb_funcall(opts, id_has_key_p, 1, sym_quiet))) {
                traits->quiet = RTEST(rb_hash_aref(opts, sym_quiet));
            }
            ext = rb_hash_aref(opts, sym_extended);
            ttl = rb_hash_aref(opts, sym_ttl);
            if (ttl != Qnil) {
                traits->explicit_ttl = 1;
                exp = NUM2ULONG(ttl);
            }
            nn = RARRAY_LEN(argv);
        } else {
            nn = argc;
        }
        if (nn < 1) {
            rb_raise(rb_eArgError, "must be at least one key");
        }
        traits->nkeys = nn;
        traits->extended = RTEST(ext) ? 1 : 0;
        traits->keys = calloc(nn, sizeof(char *));
        traits->lens = calloc(nn, sizeof(size_t));
        traits->ttls = calloc(nn, sizeof(time_t));
        keys_ptr = RARRAY_PTR(argv);
        for (ii = 0; ii < nn; ii++) {
            key = unify_key(keys_ptr[ii]);
            rb_ary_push(traits->keys_ary, key);
            traits->keys[ii] = RSTRING_PTR(key);
            traits->lens[ii] = RSTRING_LEN(key);
            traits->ttls[ii] = exp;
        }
    }

    return nn;
}

    static void
error_callback(libcouchbase_t handle, libcouchbase_error_t error, const char *errinfo)
{
    bucket_t *bucket = (bucket_t *)libcouchbase_get_cookie(handle);

    bucket->io->stop_event_loop(bucket->io);
    bucket->exception = cb_check_error(error, errinfo, Qnil);
}

    static void
storage_callback(libcouchbase_t handle, const void *cookie,
        libcouchbase_storage_t operation, libcouchbase_error_t error,
        const void *key, size_t nkey, uint64_t cas)
{
    context_t *ctx = (context_t *)cookie;
    bucket_t *bucket = ctx->bucket;
    VALUE k, c, *rv = ctx->rv, exc;
    ID o;

    bucket->seqno--;

    k = rb_str_new((const char*)key, nkey);
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
        }
    } else {
        if (!bucket->async) {
            *rv = c;
        }
        if (ctx->proc != Qnil) {
            cb_proc_call(ctx->proc, 3, c, k, o);
        }
    }

    if (bucket->seqno == 0) {
        bucket->io->stop_event_loop(bucket->io);
        rb_hash_delete(object_space, ctx->proc|1);
    }
    (void)handle;
}

    static void
delete_callback(libcouchbase_t handle, const void *cookie,
        libcouchbase_error_t error, const void *key, size_t nkey)
{
    context_t *ctx = (context_t *)cookie;
    bucket_t *bucket = ctx->bucket;
    VALUE k, success, *rv = ctx->rv, exc = Qnil;

    bucket->seqno--;

    k = rb_str_new((const char*)key, nkey);
    if (error != LIBCOUCHBASE_KEY_ENOENT || !ctx->quiet) {
        exc = cb_check_error(error, "failed to remove value", k);
        if (exc != Qnil) {
            if (bucket->async) {
                if (bucket->on_error_proc != Qnil) {
                    cb_proc_call(bucket->on_error_proc, 3, sym_delete, k, exc);
                } else {
                    if (NIL_P(bucket->exception)) {
                        bucket->exception = exc;
                    }
                }
            }
            if (NIL_P(ctx->exception)) {
                ctx->exception = exc;
            }
        }
    }
    if (NIL_P(exc)) {
        success = (error == LIBCOUCHBASE_KEY_ENOENT) ? Qfalse : Qtrue;
        if (!bucket->async) {
            *rv = success;
        }
        if (ctx->proc != Qnil) {
            cb_proc_call(ctx->proc, 2, k, success);
        }
    }
    if (bucket->seqno == 0) {
        bucket->io->stop_event_loop(bucket->io);
        rb_hash_delete(object_space, ctx->proc|1);
    }
    (void)handle;
}

    static void
get_callback(libcouchbase_t handle, const void *cookie,
        libcouchbase_error_t error, const void *key, size_t nkey,
        const void *bytes, size_t nbytes, uint32_t flags, uint64_t cas)
{
    context_t *ctx = (context_t *)cookie;
    bucket_t *bucket = ctx->bucket;
    VALUE k, v, f, c, *rv = ctx->rv, exc = Qnil;

    bucket->seqno--;

    k = rb_str_new((const char*)key, nkey);

    if (error != LIBCOUCHBASE_KEY_ENOENT || !ctx->quiet) {
        exc = cb_check_error(error, "failed to get value", k);
        if (exc != Qnil) {
            rb_ivar_set(exc, id_iv_operation, sym_get);
            if (bucket->async) {
                if (bucket->on_error_proc != Qnil) {
                    cb_proc_call(bucket->on_error_proc, 3, sym_get, k, exc);
                } else {
                    if (NIL_P(bucket->exception)) {
                        bucket->exception = exc;
                    }
                }
            }
            if (NIL_P(ctx->exception)) {
                ctx->exception = exc;
            }
        }
    }

    if (NIL_P(exc)) {
        if (nbytes != 0) {
            v = decode_value(rb_str_new((const char*)bytes, nbytes), flags);
            if (v == Qundef) {
                ctx->exception = rb_exc_new2(eValueFormatError, "unable to convert value");
            } else {
                if (ctx->extended) {
                    f = ULONG2NUM(flags);
                    c = ULL2NUM(cas);
                    if (!bucket->async) {
                        rb_hash_aset(*rv, k, rb_ary_new3(3, v, f, c));
                    }
                    if (ctx->proc != Qnil) {
                        cb_proc_call(ctx->proc, 4, v, k, f, c);
                    }
                } else {
                    if (!bucket->async) {
                        rb_hash_aset(*rv, k, v);
                    }
                    if (ctx->proc != Qnil) {
                        cb_proc_call(ctx->proc, 2, v, k);
                    }
                }
            }
        } else {
            if (ctx->proc != Qnil) {
                cb_proc_call(ctx->proc, 2, Qnil, k);
            }
        }
    }

    if (bucket->seqno == 0) {
        bucket->io->stop_event_loop(bucket->io);
        rb_hash_delete(object_space, ctx->proc|1);
    }
    (void)handle;
}

    static void
flush_callback(libcouchbase_t handle, const void* cookie,
        const char* authority, libcouchbase_error_t error)
{
    context_t *ctx = (context_t *)cookie;
    bucket_t *bucket = ctx->bucket;
    VALUE node, success = Qtrue, *rv = ctx->rv, exc;

    node = authority ? rb_str_new2(authority) : Qnil;
    exc = cb_check_error(error, "failed to flush bucket", node);
    if (exc != Qnil) {
        if (bucket->async) {
            if (bucket->on_error_proc != Qnil) {
                cb_proc_call(bucket->on_error_proc, 3, sym_flush, node, exc);
            } else {
                if (NIL_P(bucket->exception)) {
                    bucket->exception = exc;
                }
            }
        }
        if (NIL_P(ctx->exception)) {
            ctx->exception = exc;
        }
        success = Qfalse;
    }

    if (authority) {
        if (!bucket->async && RTEST(*rv)) {
            /* rewrite status for positive values only */
            *rv = success;
        }
        if (ctx->proc != Qnil) {
            cb_proc_call(ctx->proc, 2, rb_str_new2(authority), success);
        }
    }
    if (authority == NULL) {
        bucket->seqno--;
        if (bucket->seqno == 0) {
            bucket->io->stop_event_loop(bucket->io);
            rb_hash_delete(object_space, ctx->proc|1);
        }
    }

    (void)handle;
}

    static void
stat_callback(libcouchbase_t handle, const void* cookie,
        const char* authority, libcouchbase_error_t error, const void* key,
        size_t nkey, const void* bytes, size_t nbytes)
{
    context_t *ctx = (context_t *)cookie;
    bucket_t *bucket = ctx->bucket;
    VALUE stats, node, k, v, *rv = ctx->rv, exc = Qnil;

    node = authority ? rb_str_new2(authority) : Qnil;
    exc = cb_check_error(error, "failed to fetch stats", node);
    if (exc != Qnil) {
        if (bucket->async) {
            if (bucket->on_error_proc != Qnil) {
                cb_proc_call(bucket->on_error_proc, 3, sym_stats, node, exc);
            } else {
                if (NIL_P(bucket->exception)) {
                    bucket->exception = exc;
                }
            }
        }
        if (NIL_P(ctx->exception)) {
            ctx->exception = exc;
        }
    }
    if (NIL_P(exc) && nkey != 0) {
        k = rb_str_new((const char*)key, nkey);
        v = rb_str_new((const char*)bytes, nbytes);
        if (!bucket->async) {
            stats = rb_hash_aref(*rv, node);
            if (NIL_P(stats)) {
                stats = rb_hash_new();
                rb_hash_aset(*rv, node, stats);
            }
            rb_hash_aset(stats, k, v);
        }
        if (ctx->proc != Qnil) {
            cb_proc_call(ctx->proc, 3, node, k, v);
        }
    }
    if (authority == NULL) {
        bucket->seqno--;
        if (bucket->seqno == 0) {
            bucket->io->stop_event_loop(bucket->io);
            rb_hash_delete(object_space, ctx->proc|1);
        }
    }
    (void)handle;
}

    static void
touch_callback(libcouchbase_t handle, const void *cookie,
        libcouchbase_error_t error, const void *key, size_t nkey)
{
    context_t *ctx = (context_t *)cookie;
    bucket_t *bucket = ctx->bucket;
    VALUE k, success, *rv = ctx->rv, exc = Qnil;

    bucket->seqno--;
    k = rb_str_new((const char*)key, nkey);
    if (error != LIBCOUCHBASE_KEY_ENOENT || !ctx->quiet) {
        exc = cb_check_error(error, "failed to touch value", k);
        if (exc != Qnil) {
            rb_ivar_set(exc, id_iv_operation, sym_touch);
            if (bucket->async) {
                if (bucket->on_error_proc != Qnil) {
                    cb_proc_call(bucket->on_error_proc, 3, sym_touch, k, exc);
                } else {
                    if (NIL_P(bucket->exception)) {
                        bucket->exception = exc;
                    }
                }
            }
            if (NIL_P(ctx->exception)) {
                ctx->exception = exc;
            }
        }
    }

    if (NIL_P(exc)) {
        success = (error == LIBCOUCHBASE_KEY_ENOENT) ? Qfalse : Qtrue;
        if (!bucket->async) {
            rb_ary_push(*rv, success);
        }
        if (ctx->proc != Qnil) {
            cb_proc_call(ctx->proc, 2, k, success);
        }
    }
    if (bucket->seqno == 0) {
        bucket->io->stop_event_loop(bucket->io);
        rb_hash_delete(object_space, ctx->proc|1);
    }
    (void)handle;
}

    static void
arithmetic_callback(libcouchbase_t handle, const void *cookie,
        libcouchbase_error_t error, const void *key, size_t nkey,
        uint64_t value, uint64_t cas)
{
    context_t *ctx = (context_t *)cookie;
    bucket_t *bucket = ctx->bucket;
    VALUE c, k, v, *rv = ctx->rv, exc;
    ID o;

    bucket->seqno--;

    k = rb_str_new((const char*)key, nkey);
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
        }
    } else {
        v = ULL2NUM(value);
        if (ctx->extended) {
            if (!bucket->async) {
                *rv = rb_ary_new3(2, v, c);
            }
            if (ctx->proc != Qnil) {
                cb_proc_call(ctx->proc, 2, v, c);
            }
        } else {
            if (!bucket->async) {
                *rv = v;
            }
            if (ctx->proc != Qnil) {
                cb_proc_call(ctx->proc, 1, v);
            }
        }
    }
    if (bucket->seqno == 0) {
        bucket->io->stop_event_loop(bucket->io);
        rb_hash_delete(object_space, ctx->proc|1);
    }
    (void)handle;
}

    static char *
parse_path_segment(char *source, const char *key, char **result)
{
    size_t len;
    char *eot;

    if (source == NULL) {
        return NULL;
    }
    eot = strchr(source, '/');
    if (eot > source && strncmp(source, key, eot - source) == 0) {
        *eot = '\0';
        source = eot + 1;
        eot = strchr(source, '/');
        len = strlen(source);
        if (eot > source || len) {
            if (eot) {
                *eot = '\0';
                eot++;
            }
            *result = strdup(source);
        }
    }
    return eot;
}

    static void
parse_bucket_uri(VALUE uri, bucket_t *bucket)
{
    char *src, *ptr, *eot, sep = '\0';

    ptr = src = strdup(StringValueCStr(uri));
    eot = strchr(ptr, ':');
    if (eot < ptr || strncmp(ptr, "http", eot - ptr) != 0) {
        free(src);
        rb_raise(rb_eArgError, "invalid URI format: missing schema");
        return;
    }
    ptr = eot + 1;
    if (ptr[0] != '/' || ptr[1] != '/') {
        free(src);
        rb_raise(rb_eArgError, "invalid URI format.");
        return;
    }
    ptr += 2;
    eot = ptr;
    while (*eot) {
        if (*eot == '?' || *eot == '#' || *eot == ':' || *eot == '/') {
            break;
        }
        ++eot;
    }
    if (eot > ptr) {
        sep = *eot;
        *eot = '\0';
        bucket->hostname = strdup(ptr);
    }
    ptr = eot + 1;
    eot = strchr(ptr, '/');
    if (sep == ':') {
        if (eot > ptr) {
            *eot = '\0';
        }
        bucket->port = (uint16_t)atoi(ptr);
        if (eot > ptr) {
            ptr = eot + 1;
        }
    }
    ptr = parse_path_segment(ptr, "pools", &bucket->pool);
    parse_path_segment(ptr, "buckets", &bucket->bucket);
    free(src);
}

    static int
cb_first_value_i(VALUE key, VALUE value, VALUE arg)
{
    VALUE *val = (VALUE *)arg;

    *val = value;
    (void)key;
    return ST_STOP;
}

    static VALUE
cb_bucket_inspect(VALUE self)
{
    VALUE str;
    bucket_t *bucket = DATA_PTR(self);
    char buf[200];

    str = rb_str_buf_new2("#<");
    rb_str_buf_cat2(str, rb_obj_classname(self));
    snprintf(buf, 20, ":%p ", (void *)self);
    rb_str_buf_cat2(str, buf);
    rb_str_append(str, rb_ivar_get(self, id_iv_url));
    snprintf(buf, 120, " default_format:%s default_flags:0x%x async:%s quiet:%s>",
            rb_id2name(SYM2ID(bucket->default_format)),
            bucket->default_flags,
            bucket->async ? "true" : "false",
            bucket->quiet ? "true" : "false");
    rb_str_buf_cat2(str, buf);

    return str;
}

    static VALUE
cb_bucket_seqno(VALUE self)
{
    bucket_t *bucket = DATA_PTR(self);

    return LONG2FIX(bucket->seqno);
}

    void
cb_bucket_free(void *ptr)
{
    bucket_t *bucket = ptr;

    if (bucket) {
        if (bucket->handle) {
            libcouchbase_destroy(bucket->handle);
            free(bucket->authority);
            free(bucket->hostname);
            free(bucket->pool);
            free(bucket->bucket);
            free(bucket->username);
            free(bucket->password);
        }
        free(bucket);
    }
}

    void
cb_bucket_mark(void *ptr)
{
    bucket_t *bucket = ptr;

    if (bucket) {
        rb_gc_mark(bucket->exception);
        rb_gc_mark(bucket->on_error_proc);
    }
}

    static VALUE
cb_bucket_new(int argc, VALUE *argv, VALUE klass)
{
    VALUE obj;
    bucket_t *bucket;

    /* allocate new bucket struct and set it to zero */
    obj = Data_Make_Struct(klass, bucket_t, cb_bucket_mark, cb_bucket_free,
            bucket);
    rb_obj_call_init(obj, argc, argv);
    return obj;
}

/*
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
 *   @option options [String] :host ("localhost") the hostname or IP address
 *     of the node
 *   @option options [Fixnum] :port (8091) the port of the managemenent API
 *   @option options [String] :pool ("default") the pool name
 *   @option options [String] :bucket ("default") the bucket name
 *   @option options [String] :username (nil) the user name to connect to the
 *     cluster. Used to authenticate on management API.
 *   @option options [String] :password (nil) the password of the user.
 *   @option options [Boolean] :async (false) the flag specifying if the
 *     connection asynchronous. By default all operations are synchronous and
 *     block waiting for results, but you can make them asynchronous and run
 *     event loop explicitly. (see Couchbase::Bucket#run)
 *   @option options [Boolean] :quiet (false) the flag controlling if raising
 *     exception when the client executes operations on unexising keys. If it
 *     is +true+ it will raise +Couchbase::NotFoundError+ exceptions. The
 *     default behaviour is to return +nil+ value silently (could be useful in
 *     Rails cache).
 */
    static VALUE
cb_bucket_init(int argc, VALUE *argv, VALUE self)
{
    VALUE uri, opts, arg, buf;
    libcouchbase_error_t err;
    bucket_t *bucket = DATA_PTR(self);
    size_t len;

    bucket->exception = Qnil;
    bucket->hostname = strdup("localhost");
    bucket->port = 8091;
    bucket->pool = strdup("default");
    bucket->bucket = strdup("default");
    bucket->async = 0;
    bucket->quiet = 1;
    bucket->default_flags = 0;
    bucket->default_format = sym_document;
    bucket->on_error_proc = Qnil;

    if (rb_scan_args(argc, argv, "02", &uri, &opts) > 0) {
        if (TYPE(uri) == T_HASH && argc == 1) {
            opts = uri;
            uri = Qnil;
        }
        if (uri != Qnil) {
            Check_Type(uri, T_STRING);
            parse_bucket_uri(uri, bucket);
        }
        if (TYPE(opts) == T_HASH) {
            arg = rb_hash_aref(opts, sym_hostname);
            if (arg != Qnil) {
                if (bucket->hostname) {
                    free(bucket->hostname);
                }
                bucket->hostname = strdup(StringValueCStr(arg));
            }
            arg = rb_hash_aref(opts, sym_pool);
            if (arg != Qnil) {
                if (bucket->pool) {
                    free(bucket->pool);
                }
                bucket->pool = strdup(StringValueCStr(arg));
            }
            arg = rb_hash_aref(opts, sym_bucket);
            if (arg != Qnil) {
                if (bucket->bucket) {
                    free(bucket->bucket);
                }
                bucket->bucket = strdup(StringValueCStr(arg));
            }
            arg = rb_hash_aref(opts, sym_username);
            if (arg != Qnil) {
                bucket->username = strdup(StringValueCStr(arg));
            }
            arg = rb_hash_aref(opts, sym_password);
            if (arg != Qnil) {
                bucket->password = strdup(StringValueCStr(arg));
            }
            arg = rb_hash_aref(opts, sym_port);
            if (arg != Qnil) {
                bucket->port = (uint16_t)NUM2UINT(arg);
            }
            arg = rb_hash_aref(opts, sym_async);
            bucket->async = RTEST(arg);
            if (RTEST(rb_funcall(opts, id_has_key_p, 1, sym_quiet))) {
                bucket->quiet = RTEST(rb_hash_aref(opts, sym_quiet));
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
        } else {
            opts = Qnil;
        }
    }
    len = strlen(bucket->hostname) + 10;
    bucket->authority = calloc(len, sizeof(char));
    if (bucket->authority == NULL) {
        rb_raise(eNoMemoryError, "failed to allocate memory for Bucket");
    }
    snprintf(bucket->authority, len, "%s:%u", bucket->hostname, bucket->port);
    bucket->io = libcouchbase_create_io_ops(LIBCOUCHBASE_IO_OPS_DEFAULT, NULL, &err);
    if (bucket->io == NULL) {
        rb_exc_raise(cb_check_error(err, "failed to create IO instance", Qnil));
    }
    bucket->handle = libcouchbase_create(bucket->authority,
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

    err = libcouchbase_connect(bucket->handle);
    if (err != LIBCOUCHBASE_SUCCESS) {
        rb_exc_raise(cb_check_error(err, "failed to connect libcouchbase instance to server", Qnil));
    }
    libcouchbase_wait(bucket->handle);
    if (bucket->exception != Qnil) {
        rb_exc_raise(bucket->exception);
    }

    rb_ivar_set(self, id_iv_authority, rb_str_new2(bucket->authority));
    rb_ivar_set(self, id_iv_bucket, rb_str_new2(bucket->bucket));
    rb_ivar_set(self, id_iv_hostname, rb_str_new2(bucket->hostname));
    rb_ivar_set(self, id_iv_password, bucket->password ? rb_str_new2(bucket->password) : Qnil);
    rb_ivar_set(self, id_iv_pool, rb_str_new2(bucket->pool));
    rb_ivar_set(self, id_iv_port, UINT2NUM(bucket->port));
    rb_ivar_set(self, id_iv_username, bucket->username ? rb_str_new2(bucket->username) : Qnil);
    rb_ivar_set(self, id_iv_async, bucket->async ? Qtrue : Qfalse);
    rb_ivar_set(self, id_iv_quiet, bucket->quiet ? Qtrue : Qfalse);
    rb_ivar_set(self, id_iv_default_flags, ULONG2NUM(bucket->default_flags));
    rb_ivar_set(self, id_iv_default_format, bucket->default_format);
    rb_ivar_set(self, id_iv_on_error, bucket->on_error_proc);

    buf = rb_str_buf_new2("http://");
    rb_str_buf_cat2(buf, bucket->authority);
    rb_str_buf_cat2(buf, "/pools/");
    rb_str_buf_cat2(buf, bucket->pool);
    rb_str_buf_cat2(buf, "/buckets/");
    rb_str_buf_cat2(buf, bucket->bucket);
    rb_str_buf_cat2(buf, "/");
    rb_ivar_set(self, id_iv_url, buf);

    return self;
}

    static VALUE
cb_bucket_async_set(VALUE self, VALUE val)
{
    bucket_t *bucket = DATA_PTR(self);
    VALUE new;

    bucket->async = RTEST(val);
    new = bucket->async ? Qtrue : Qfalse;
    rb_ivar_set(self, id_iv_async, new);
    return new;
}

    static VALUE
cb_bucket_quiet_set(VALUE self, VALUE val)
{
    bucket_t *bucket = DATA_PTR(self);
    VALUE new;

    bucket->quiet = RTEST(val);
    new = bucket->quiet ? Qtrue : Qfalse;
    rb_ivar_set(self, id_iv_quiet, new);
    return new;
}

    static VALUE
cb_bucket_default_flags_set(VALUE self, VALUE val)
{
    bucket_t *bucket = DATA_PTR(self);

    bucket->default_flags = (uint32_t)NUM2ULONG(val);
    bucket->default_format = flags_get_format(bucket->default_flags);
    rb_ivar_set(self, id_iv_default_format, bucket->default_format);
    rb_ivar_set(self, id_iv_default_flags, val);
    return val;
}

    static VALUE
cb_bucket_default_format_set(VALUE self, VALUE val)
{
    bucket_t *bucket = DATA_PTR(self);

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
        rb_ivar_set(self, id_iv_default_format, val);
        rb_ivar_set(self, id_iv_default_flags, ULONG2NUM(bucket->default_flags));
    }

    return val;
}

    static VALUE
cb_bucket_on_error_set(VALUE self, VALUE val)
{
    bucket_t *bucket = DATA_PTR(self);

    if (rb_respond_to(val, id_call)) {
        bucket->on_error_proc = val;
    } else {
        bucket->on_error_proc = Qnil;
    }
    rb_ivar_set(self, id_iv_on_error, bucket->on_error_proc);

    return bucket->on_error_proc;
}

    static VALUE
cb_bucket_on_error_get(VALUE self)
{
    bucket_t *bucket = DATA_PTR(self);

    if (rb_block_given_p()) {
        return cb_bucket_on_error_set(self, rb_block_proc());
    } else {
        return bucket->on_error_proc;
    }
}

    static VALUE
cb_bucket_delete(int argc, VALUE *argv, VALUE self)
{
    bucket_t *bucket = DATA_PTR(self);
    context_t *ctx;
    VALUE k, c, rv, proc, exc, opts;
    char *key;
    size_t nkey;
    uint64_t cas = 0;
    libcouchbase_error_t err;

    rb_scan_args(argc, argv, "11&", &k, &opts, &proc);
    k = unify_key(k);
    key = RSTRING_PTR(k);
    nkey = RSTRING_LEN(k);
    ctx = calloc(1, sizeof(context_t));
    ctx->quiet = bucket->quiet;
    if (ctx == NULL) {
        rb_raise(eNoMemoryError, "failed to allocate memory for context");
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
    rb_hash_aset(object_space, ctx->proc|1, ctx->proc);
    rv = rb_ary_new();
    ctx->rv = &rv;
    ctx->bucket = bucket;
    ctx->exception = Qnil;
    err = libcouchbase_remove(bucket->handle, (const void *)ctx,
            (const void *)key, nkey, cas);
    exc = cb_check_error(err, "failed to schedule delete request", Qnil);
    if (exc != Qnil) {
        free(ctx);
        rb_exc_raise(exc);
    }
    bucket->seqno++;
    if (bucket->async) {
        return Qnil;
    } else {
        bucket->io->run_event_loop(bucket->io);
        exc = ctx->exception;
        free(ctx);
        if (exc != Qnil) {
            rb_exc_raise(exc);
        }
        return rv;
    }
}

    static inline VALUE
cb_bucket_store(libcouchbase_storage_t cmd, int argc, VALUE *argv, VALUE self)
{
    bucket_t *bucket = DATA_PTR(self);
    context_t *ctx;
    VALUE k, v, arg, opts, rv, proc, exc, fmt;
    char *key, *bytes;
    size_t nkey, nbytes;
    uint32_t flags;
    time_t exp = 0;
    uint64_t cas = 0;
    libcouchbase_error_t err;

    rb_scan_args(argc, argv, "21&", &k, &v, &opts, &proc);
    k = unify_key(k);
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
    ctx = calloc(1, sizeof(context_t));
    if (ctx == NULL) {
        rb_raise(eNoMemoryError, "failed to allocate memory for context");
    }
    rv = Qnil;
    ctx->rv = &rv;
    ctx->bucket = bucket;
    ctx->proc = proc;
    rb_hash_aset(object_space, ctx->proc|1, ctx->proc);
    ctx->exception = Qnil;
    err = libcouchbase_store(bucket->handle, (const void *)ctx, cmd,
            (const void *)key, nkey, bytes, nbytes, flags, exp, cas);
    exc = cb_check_error(err, "failed to schedule set request", Qnil);
    if (exc != Qnil) {
        free(ctx);
        rb_exc_raise(exc);
    }
    bucket->seqno++;
    if (bucket->async) {
        return Qnil;
    } else {
        bucket->io->run_event_loop(bucket->io);
        exc = ctx->exception;
        free(ctx);
        if (exc != Qnil) {
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
    bucket_t *bucket = DATA_PTR(self);
    context_t *ctx;
    VALUE k, d, arg, opts, rv, proc, exc;
    char *key;
    size_t nkey;
    time_t exp;
    uint64_t delta = 0, initial = 0;
    int create = 0;
    libcouchbase_error_t err;

    rb_scan_args(argc, argv, "12&", &k, &d, &opts, &proc);
    k = unify_key(k);
    ctx = calloc(1, sizeof(context_t));
    if (ctx == NULL) {
        rb_raise(eNoMemoryError, "failed to allocate memory for context");
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
    rb_hash_aset(object_space, ctx->proc|1, ctx->proc);
    ctx->exception = Qnil;
    ctx->arithm = sign;
    err = libcouchbase_arithmetic(bucket->handle, (const void *)ctx,
            (const void *)key, nkey, delta, exp, create, initial);
    exc = cb_check_error(err, "failed to schedule arithmetic request", k);
    if (exc != Qnil) {
        free(ctx);
        rb_exc_raise(exc);
    }
    bucket->seqno++;
    if (bucket->async) {
        return Qnil;
    } else {
        bucket->io->run_event_loop(bucket->io);
        exc = ctx->exception;
        free(ctx);
        if (exc != Qnil) {
            rb_exc_raise(exc);
        }
        return rv;
    }
}

static VALUE
cb_bucket_incr(int argc, VALUE *argv, VALUE self)
{
    return cb_bucket_arithmetic(+1, argc, argv, self);
}

static VALUE
cb_bucket_decr(int argc, VALUE *argv, VALUE self)
{
    return cb_bucket_arithmetic(-1, argc, argv, self);
}

    static VALUE
cb_bucket_get(int argc, VALUE *argv, VALUE self)
{
    bucket_t *bucket = DATA_PTR(self);
    context_t *ctx;
    VALUE args, rv, proc, exc, vv = Qnil, keys;
    long nn;
    libcouchbase_error_t err;
    struct key_traits *traits;
    int extended;

    rb_scan_args(argc, argv, "0*&", &args, &proc);
    rb_funcall(args, id_flatten_bang, 0);
    traits = calloc(1, sizeof(struct key_traits));
    nn = cb_args_scan_keys(RARRAY_LEN(args), args, bucket, traits);
    ctx = calloc(1, sizeof(context_t));
    if (ctx == NULL) {
        rb_raise(eNoMemoryError, "failed to allocate memory for context");
    }
    keys = traits->keys_ary;
    ctx->proc = proc;
    rb_hash_aset(object_space, ctx->proc|1, ctx->proc);
    ctx->bucket = bucket;
    ctx->extended = traits->extended;
    ctx->quiet = traits->quiet;
    rv = rb_hash_new();
    ctx->rv = &rv;
    ctx->exception = Qnil;
    if (!bucket->async) {
        bucket->seqno = 0;
    }
    err = libcouchbase_mget(bucket->handle, (const void *)ctx,
            traits->nkeys, (const void * const *)traits->keys,
            traits->lens, (traits->explicit_ttl) ? traits->ttls : NULL);
    free(traits->keys);
    free(traits->lens);
    free(traits->ttls);
    free(traits);
    exc = cb_check_error(err, "failed to schedule get request", Qnil);
    if (exc != Qnil) {
        free(ctx);
        rb_exc_raise(exc);
    }
    bucket->seqno += nn;
    if (bucket->async) {
        return Qnil;
    } else {
        bucket->io->run_event_loop(bucket->io);
        exc = ctx->exception;
        extended = ctx->extended;
        free(ctx);
        if (exc != Qnil) {
            rb_exc_raise(exc);
        }
        if (bucket->exception != Qnil) {
            rb_exc_raise(bucket->exception);
        }
        if (nn > 1) {
            if (extended) {
                return rv;  /* return as a hash {key => [value, flags, cas], ...} */
            } else {
                long ii;
                VALUE *keys_ptr, ret;
                ret = rb_ary_new();
                keys_ptr = RARRAY_PTR(keys);
                for (ii = 0; ii < nn; ii++) {
                    rb_ary_push(ret, rb_hash_aref(rv, keys_ptr[ii]));
                }
                return ret;  /* return as an array [value1, value2, ...] */
            }
        } else {
            rb_hash_foreach(rv, cb_first_value_i, (VALUE)&vv);
            return vv;
        }
    }
}

    static VALUE
cb_bucket_touch(int argc, VALUE *argv, VALUE self)
{
    bucket_t *bucket = DATA_PTR(self);
    context_t *ctx;
    VALUE args, rv, proc, exc;
    size_t nn;
    libcouchbase_error_t err;
    struct key_traits *traits;

    rb_scan_args(argc, argv, "0*&", &args, &proc);
    rb_funcall(args, id_flatten_bang, 0);
    traits = calloc(1, sizeof(struct key_traits));
    nn = cb_args_scan_keys(RARRAY_LEN(args), args, bucket, traits);
    ctx = calloc(1, sizeof(context_t));
    if (ctx == NULL) {
        rb_raise(eNoMemoryError, "failed to allocate memory for context");
    }
    ctx->proc = proc;
    rb_hash_aset(object_space, ctx->proc|1, ctx->proc);
    ctx->bucket = bucket;
    rv = rb_ary_new();
    ctx->rv = &rv;
    ctx->exception = Qnil;
    if (!bucket->async) {
        bucket->seqno = 0;
    }
    err = libcouchbase_mtouch(bucket->handle, (const void *)ctx,
            traits->nkeys, (const void * const *)traits->keys,
            traits->lens, traits->ttls);
    free(traits);
    exc = cb_check_error(err, "failed to schedule touch request", Qnil);
    if (exc != Qnil) {
        free(ctx);
        rb_exc_raise(exc);
    }
    bucket->seqno += nn;
    if (bucket->async) {
        return Qnil;
    } else {
        bucket->io->run_event_loop(bucket->io);
        exc = ctx->exception;
        free(ctx);
        if (exc != Qnil) {
            rb_exc_raise(exc);
        }
        if (bucket->exception != Qnil) {
            rb_exc_raise(bucket->exception);
        }
        if (nn > 1) {
            return rv;
        } else {
            return rb_ary_pop(rv);
        }
    }
}

    static VALUE
cb_bucket_flush(VALUE self)
{
    bucket_t *bucket = DATA_PTR(self);
    context_t *ctx;
    VALUE rv, exc;
    libcouchbase_error_t err;

    ctx = calloc(1, sizeof(context_t));
    if (ctx == NULL) {
        rb_raise(eNoMemoryError, "failed to allocate memory for context");
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
    rb_hash_aset(object_space, ctx->proc|1, ctx->proc);
    err = libcouchbase_flush(bucket->handle, (const void *)ctx);
    exc = cb_check_error(err, "failed to schedule flush request", Qnil);
    if (exc != Qnil) {
        free(ctx);
        rb_exc_raise(exc);
    }
    bucket->seqno++;
    if (bucket->async) {
        return Qnil;
    } else {
        bucket->io->run_event_loop(bucket->io);
        exc = ctx->exception;
        free(ctx);
        if (exc != Qnil) {
            rb_exc_raise(exc);
        }
        return rv;
    }
}

    static VALUE
cb_bucket_stats(int argc, VALUE *argv, VALUE self)
{
    bucket_t *bucket = DATA_PTR(self);
    context_t *ctx;
    VALUE rv, exc, arg, proc;
    char *key;
    size_t nkey;
    libcouchbase_error_t err;

    rb_scan_args(argc, argv, "01&", &arg, &proc);

    ctx = calloc(1, sizeof(context_t));
    if (ctx == NULL) {
        rb_raise(eNoMemoryError, "failed to allocate memory for context");
    }
    rv = rb_hash_new();
    ctx->rv = &rv;
    ctx->bucket = bucket;
    ctx->proc = proc;
    rb_hash_aset(object_space, ctx->proc|1, ctx->proc);
    ctx->exception = Qnil;
    if (arg != Qnil) {
        key = RSTRING_PTR(arg);
        nkey = RSTRING_LEN(arg);
    } else {
        key = NULL;
        nkey = 0;
    }
    err = libcouchbase_server_stats(bucket->handle, (const void *)ctx,
            key, nkey);
    exc = cb_check_error(err, "failed to schedule stat request", Qnil);
    if (exc != Qnil) {
        free(ctx);
        rb_exc_raise(exc);
    }
    bucket->seqno++;
    if (bucket->async) {
        return Qnil;
    } else {
        bucket->io->run_event_loop(bucket->io);
        exc = ctx->exception;
        free(ctx);
        if (exc != Qnil) {
            rb_exc_raise(exc);
        }
        if (bucket->exception != Qnil) {
            rb_exc_raise(bucket->exception);
        }
        return rv;
    }

    return Qnil;
}

    static VALUE
cb_bucket_run(int argc, VALUE *argv, VALUE self)
{
    bucket_t *bucket = DATA_PTR(self);
    VALUE proc = Qnil;

    if (!bucket->async) {
        return Qnil;
    }
    rb_scan_args(argc, argv, "00&", &proc);
    if (NIL_P(proc)) {
        if (bucket->seqno < 1) {
            bucket->seqno = 0;
            return Qnil;
        }
    } else {
        bucket->seqno = 0;
        cb_proc_call(proc, 1, self);
    }
    bucket->io->run_event_loop(bucket->io);
    if (bucket->exception != Qnil) {
        rb_exc_raise(bucket->exception);
    }
    return Qnil;
}

/*
 * Unconditionally set the object in the cache
 */
    static VALUE
cb_bucket_set(int argc, VALUE *argv, VALUE self)
{
    return cb_bucket_store(LIBCOUCHBASE_SET, argc, argv, self);
}

/*
 * Add the item to the cache, but fail if the object exists alread
 */
    static VALUE
cb_bucket_add(int argc, VALUE *argv, VALUE self)
{
    return cb_bucket_store(LIBCOUCHBASE_ADD, argc, argv, self);
}

/*
 * Replace the existing object in the cache
 */
    static VALUE
cb_bucket_replace(int argc, VALUE *argv, VALUE self)
{
    return cb_bucket_store(LIBCOUCHBASE_REPLACE, argc, argv, self);
}

/*
 * Append this object to the existing object
 */
    static VALUE
cb_bucket_append(int argc, VALUE *argv, VALUE self)
{
    return cb_bucket_store(LIBCOUCHBASE_APPEND, argc, argv, self);
}

/*
 * Prepend this  object to the existing object
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

/* Ruby Extension initializer */
    void
Init_couchbase_ext(void)
{
    mJSON = rb_const_get(rb_cObject, rb_intern("JSON"));
    mMarshal = rb_const_get(rb_cObject, rb_intern("Marshal"));
    mCouchbase = rb_define_module("Couchbase");

    mError = rb_define_module_under(mCouchbase, "Error");
    /* Document-class: Couchbase::Error::Base
     * The base error class */
    eBaseError = rb_define_class_under(mError, "Base", rb_eRuntimeError);
    /* Document-class: Couchbase::Error::Auth
     * Authentication error */
    eAuthError = rb_define_class_under(mError, "Auth", eBaseError);
    /* Document-class: Couchbase::Error::Busy
     * The cluster is too busy now. Try again later */
    eBusyError = rb_define_class_under(mError, "Busy", eBaseError);
    /* Document-class: Couchbase::Error::DeltaBadval
     * The given value is not a number */
    eDeltaBadvalError = rb_define_class_under(mError, "DeltaBadval", eBaseError);
    /* Document-class: Couchbase::Error::Internal
     * Internal error */
    eInternalError = rb_define_class_under(mError, "Internal", eBaseError);
    /* Document-class: Couchbase::Error::Invalid
     * Invalid arguments */
    eInvalidError = rb_define_class_under(mError, "Invalid", eBaseError);
    /* Document-class: Couchbase::Error::KeyExists
     * Key already exists */
    eKeyExistsError = rb_define_class_under(mError, "KeyExists", eBaseError);
    /* Document-class: Couchbase::Error::Libcouchbase
     * Generic error */
    eLibcouchbaseError = rb_define_class_under(mError, "Libcouchbase", eBaseError);
    /* Document-class: Couchbase::Error::Libevent
     * Problem using libevent */
    eLibeventError = rb_define_class_under(mError, "Libevent", eBaseError);
    /* Document-class: Couchbase::Error::Network
     * Network error */
    eNetworkError = rb_define_class_under(mError, "Network", eBaseError);
    /* Document-class: Couchbase::Error::NoMemory
     * Out of memory error */
    eNoMemoryError = rb_define_class_under(mError, "NoMemory", eBaseError);
    /* Document-class: Couchbase::Error::NotFound
     * No such key */
    eNotFoundError = rb_define_class_under(mError, "NotFound", eBaseError);
    /* Document-class: Couchbase::Error::NotMyVbucket
     * The vbucket is not located on this server */
    eNotMyVbucketError = rb_define_class_under(mError, "NotMyVbucket", eBaseError);
    /* Document-class: Couchbase::Error::NotStored
     * Not stored */
    eNotStoredError = rb_define_class_under(mError, "NotStored", eBaseError);
    /* Document-class: Couchbase::Error::NotSupported
     * Not supported */
    eNotSupportedError = rb_define_class_under(mError, "NotSupported", eBaseError);
    /* Document-class: Couchbase::Error::Range
     * Invalid range */
    eRangeError = rb_define_class_under(mError, "Range", eBaseError);
    /* Document-class: Couchbase::Error::TemporaryFail
     * Temporary failure. Try again later */
    eTmpFailError = rb_define_class_under(mError, "TemporaryFail", eBaseError);
    /* Document-class: Couchbase::Error::TooBig
     * Object too big */
    eTooBigError = rb_define_class_under(mError, "TooBig", eBaseError);
    /* Document-class: Couchbase::Error::UnknownCommand
     * Unknown command */
    eUnknownCommandError = rb_define_class_under(mError, "UnknownCommand", eBaseError);
    /* Document-class: Couchbase::Error::UnknownHost
     * Unknown host */
    eUnknownHostError = rb_define_class_under(mError, "UnknownHost", eBaseError);
    /* Document-class: Couchbase::Error::ValueFormat
     * Failed to decode or encode value */
    eValueFormatError = rb_define_class_under(mError, "ValueFormat", eBaseError);
    /* Document-class: Couchbase::Error::Protocol
     * Protocol error */
    eProtocolError = rb_define_class_under(mError, "Protocol", eBaseError);

    /* Document-method: error
     * @return [Boolean] the error code from libcouchbase */
    rb_define_attr(eBaseError, "error", 1, 0);
    id_iv_error = rb_intern("@error");
    /* Document-method: key
     * @return [String] the key which generated error */
    rb_define_attr(eBaseError, "key", 1, 0);
    id_iv_error = rb_intern("@key");
    /* Document-method: cas
     * @return [Fixnum] the version of the key (+nil+ unless accessible */
    rb_define_attr(eBaseError, "cas", 1, 0);
    id_iv_error = rb_intern("@cas");
    /* Document-method: operation
     * @return [Symbol] the operation (+nil+ unless accessible) */
    rb_define_attr(eBaseError, "operation", 1, 0);
    id_iv_error = rb_intern("@operation");

    /* Document-class: Couchbase::Bucket
     * This class in charge of all stuff connected to communication with
     * Couchbase. */
    cBucket = rb_define_class_under(mCouchbase, "Bucket", rb_cObject);
    object_space = rb_hash_new();
    rb_define_const(cBucket, "OBJECT_SPACE", object_space);

    rb_define_const(cBucket, "FMT_MASK", INT2FIX(FMT_MASK));
    rb_define_const(cBucket, "FMT_DOCUMENT", INT2FIX(FMT_DOCUMENT));
    rb_define_const(cBucket, "FMT_MARSHAL", INT2FIX(FMT_MARSHAL));
    rb_define_const(cBucket, "FMT_PLAIN", INT2FIX(FMT_PLAIN));

    rb_define_singleton_method(cBucket, "new", cb_bucket_new, -1);

    rb_define_method(cBucket, "initialize", cb_bucket_init, -1);
    rb_define_method(cBucket, "inspect", cb_bucket_inspect, 0);
    rb_define_method(cBucket, "seqno", cb_bucket_seqno, 0);

    rb_define_method(cBucket, "add", cb_bucket_add, -1);
    rb_define_method(cBucket, "append", cb_bucket_append, -1);
    rb_define_method(cBucket, "prepend", cb_bucket_prepend, -1);
    rb_define_method(cBucket, "replace", cb_bucket_replace, -1);
    rb_define_method(cBucket, "set", cb_bucket_set, -1);
    rb_define_method(cBucket, "get", cb_bucket_get, -1);
    rb_define_method(cBucket, "run", cb_bucket_run, -1);
    rb_define_method(cBucket, "touch", cb_bucket_touch, -1);
    rb_define_method(cBucket, "delete", cb_bucket_delete, -1);
    rb_define_method(cBucket, "stats", cb_bucket_stats, -1);
    rb_define_method(cBucket, "flush", cb_bucket_flush, 0);
    rb_define_method(cBucket, "incr", cb_bucket_incr, -1);
    rb_define_method(cBucket, "decr", cb_bucket_decr, -1);

    rb_define_alias(cBucket, "decrement", "decr");
    rb_define_alias(cBucket, "increment", "incr");

    rb_define_alias(cBucket, "[]", "get");
    /* rb_define_alias(cBucket, "[]=", "set"); */
    rb_define_method(cBucket, "[]=", cb_bucket_aset, -1);

    /* Document-method: async
     * @return [Boolean] is the connection asynchronous. */
    rb_define_attr(cBucket, "async", 1, 0);
    rb_define_method(cBucket, "async=", cb_bucket_async_set, 1);
    rb_define_alias(cBucket, "async?", "async");
    id_iv_async = rb_intern("@async");

    /* Document-method: quiet
     * @return [Boolean] if true, the unknown keys will raise
     *   Couchbase::Error::NotFoundError. */
    rb_define_attr(cBucket, "quiet", 1, 0);
    rb_define_method(cBucket, "quiet=", cb_bucket_quiet_set, 1);
    rb_define_alias(cBucket, "quiet?", "quiet");
    id_iv_quiet = rb_intern("@quiet");

    /* Document-method: default_flags
     * @return [Fixnum] */
    rb_define_attr(cBucket, "default_flags", 1, 0);
    rb_define_method(cBucket, "default_flags=", cb_bucket_default_flags_set, 1);
    id_iv_default_flags = rb_intern("@default_flags");
    /* Document-method: default_format
     * @return [Symbol] */
    rb_define_attr(cBucket, "default_format", 1, 0);
    rb_define_method(cBucket, "default_format=", cb_bucket_default_format_set, 1);
    id_iv_default_format = rb_intern("@default_format");
    /* Document-method: on_error
     * @return [Proc] */
    rb_define_attr(cBucket, "on_error", 1, 0);
    rb_define_method(cBucket, "on_error", cb_bucket_on_error_get, 0);
    rb_define_method(cBucket, "on_error=", cb_bucket_on_error_set, 1);
    id_iv_on_error = rb_intern("@on_error");

    /* Document-method: url
     * @return [String] the address of the cluster management endpoint. */
    rb_define_attr(cBucket, "url", 1, 0);
    id_iv_url = rb_intern("@url");
    /* Document-method: hostname
     * @return [String] */
    rb_define_attr(cBucket, "hostname", 1, 0);
    id_iv_hostname = rb_intern("@hostname");
    /* Document-method: port
     * @return [Fixnum] */
    rb_define_attr(cBucket, "port", 1, 0);
    id_iv_port = rb_intern("@port");
    /* Document-method: authority
     * @return [String] Host with port. */
    rb_define_attr(cBucket, "authority", 1, 0);
    id_iv_authority = rb_intern("@authority");
    /* Document-method: bucket
     * @return [String] the bucket name */
    rb_define_attr(cBucket, "bucket", 1, 0);
    rb_define_alias(cBucket, "name", "bucket");
    id_iv_bucket = rb_intern("@bucket");
    /* Document-method: pool
     * @return [String] */
    rb_define_attr(cBucket, "pool", 1, 0);
    id_iv_pool = rb_intern("@pool");
    /* Document-method: username
     * @return [String] */
    rb_define_attr(cBucket, "username", 1, 0);
    id_iv_username = rb_intern("@username");
    /* Document-method: password
     * @return [String] */
    rb_define_attr(cBucket, "password", 1, 0);
    id_iv_password = rb_intern("@password");

    /* Define symbols */
    id_arity = rb_intern("arity");
    id_call = rb_intern("call");
    id_load = rb_intern("load");
    id_dump = rb_intern("dump");
    id_flatten_bang = rb_intern("flatten!");
    id_has_key_p = rb_intern("has_key?");

    sym_add = ID2SYM(rb_intern("add"));
    sym_append = ID2SYM(rb_intern("append"));
    sym_async = ID2SYM(rb_intern("async"));
    sym_bucket = ID2SYM(rb_intern("bucket"));
    sym_cas = ID2SYM(rb_intern("cas"));
    sym_create = ID2SYM(rb_intern("create"));
    sym_decrement = ID2SYM(rb_intern("decrement"));
    sym_default_flags = ID2SYM(rb_intern("default_flags"));
    sym_default_format = ID2SYM(rb_intern("default_format"));
    sym_delete = ID2SYM(rb_intern("delete"));
    sym_document = ID2SYM(rb_intern("document"));
    sym_extended = ID2SYM(rb_intern("extended"));
    sym_flags = ID2SYM(rb_intern("flags"));
    sym_flush = ID2SYM(rb_intern("flush"));
    sym_format = ID2SYM(rb_intern("format"));
    sym_get = ID2SYM(rb_intern("get"));
    sym_hostname = ID2SYM(rb_intern("hostname"));
    sym_increment = ID2SYM(rb_intern("increment"));
    sym_initial = ID2SYM(rb_intern("initial"));
    sym_marshal = ID2SYM(rb_intern("marshal"));
    sym_password = ID2SYM(rb_intern("password"));
    sym_plain = ID2SYM(rb_intern("plain"));
    sym_pool = ID2SYM(rb_intern("pool"));
    sym_port = ID2SYM(rb_intern("port"));
    sym_prepend = ID2SYM(rb_intern("prepend"));
    sym_quiet = ID2SYM(rb_intern("quiet"));
    sym_replace = ID2SYM(rb_intern("replace"));
    sym_set = ID2SYM(rb_intern("set"));
    sym_stats = ID2SYM(rb_intern("stats"));
    sym_touch = ID2SYM(rb_intern("touch"));
    sym_ttl = ID2SYM(rb_intern("ttl"));
    sym_username = ID2SYM(rb_intern("username"));
}
