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

typedef struct {
    libcouchbase_t handle;
    struct libcouchbase_io_opt_st *io;
    uint16_t port;
    char *authority;
    char *hostname;
    char *pool;
    char *bucket;
    char *username;
    char *password;
    VALUE exception;        /* error delivered by error_callback */
} bucket_t;

static VALUE mCouchbase, mError, cBucket;

static ID sym_bucket,
          sym_hostname,
          sym_passwd,
          sym_pool,
          sym_port,
          sym_username,
          id_arity,
          id_call,
          id_iv_authority,
          id_iv_bucket,
          id_iv_error,
          id_iv_hostname,
          id_iv_password,
          id_iv_pool,
          id_iv_port,
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
cb_check_error(libcouchbase_error_t rc, const char *msg)
{
    VALUE klass, exc, str;
    char buf[100];

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
    snprintf(buf, 100, " (error: %d)", rc);
    exc = rb_exc_new3(klass, str);
    rb_ivar_set(exc, id_iv_error, INT2FIX(rc));
    return exc;
}

    static void
error_callback(libcouchbase_t handle, libcouchbase_error_t error, const char *errinfo)
{
    bucket_t *bucket = (bucket_t *)libcouchbase_get_cookie(handle);

    bucket->io->stop_event_loop(bucket->io);
    bucket->exception = cb_check_error(error, errinfo);
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

    static VALUE
cb_bucket_inspect(VALUE self)
{
    VALUE str;
    char buf[200];

    str = rb_str_buf_new2("#<");
    rb_str_buf_cat2(str, rb_obj_classname(self));
    snprintf(buf, 20, ":%p ", (void *)self);
    rb_str_buf_cat2(str, buf);
    rb_str_append(str, rb_ivar_get(self, id_iv_url));
    snprintf(buf, 120, ">");
    rb_str_buf_cat2(str, buf);

    return str;
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
    (void)ptr;
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
            arg = rb_hash_aref(opts, sym_passwd);
            if (arg != Qnil) {
                bucket->password = strdup(StringValueCStr(arg));
            }
            arg = rb_hash_aref(opts, sym_port);
            if (arg != Qnil) {
                bucket->port = (uint16_t)NUM2UINT(arg);
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
        rb_exc_raise(cb_check_error(err, "failed to create IO instance"));
    }
    bucket->handle = libcouchbase_create(bucket->authority,
            bucket->username, bucket->password, bucket->bucket, bucket->io);
    if (bucket->handle == NULL) {
        rb_raise(eLibcouchbaseError, "failed to create libcouchbase instance");
    }
    libcouchbase_set_cookie(bucket->handle, bucket);
    (void)libcouchbase_set_error_callback(bucket->handle, error_callback);

    err = libcouchbase_connect(bucket->handle);
    if (err != LIBCOUCHBASE_SUCCESS) {
        rb_exc_raise(cb_check_error(err, "failed to connect libcouchbase instance to server"));
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


/* Ruby Extension initializer */
    void
Init_couchbase_ext(void)
{
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

    /* Document-class: Couchbase::Bucket
     * This class in charge of all stuff connected to communication with
     * Couchbase. */
    cBucket = rb_define_class_under(mCouchbase, "Bucket", rb_cObject);

    rb_define_singleton_method(cBucket, "new", cb_bucket_new, -1);

    rb_define_method(cBucket, "initialize", cb_bucket_init, -1);
    rb_define_method(cBucket, "inspect", cb_bucket_inspect, 0);

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

    sym_bucket = ID2SYM(rb_intern("bucket"));
    sym_hostname = ID2SYM(rb_intern("hostname"));
    sym_passwd = ID2SYM(rb_intern("password"));
    sym_port = ID2SYM(rb_intern("port"));
    sym_username = ID2SYM(rb_intern("username"));
    sym_pool = ID2SYM(rb_intern("pool"));
}
