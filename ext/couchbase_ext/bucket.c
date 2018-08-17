/* vim: ft=c et ts=8 sts=4 sw=4 cino=
 *
 *   Copyright 2011-2018 Couchbase, Inc.
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

#include "couchbase_ext.h"

static void
bootstrap_callback(lcb_t handle, lcb_error_t error)
{
    struct cb_bucket_st *bucket = (struct cb_bucket_st *)lcb_get_cookie(handle);

    lcb_breakout(handle);
    bucket->exception = cb_check_error(error, "bootstrap error", Qnil);
    if (!bucket->connected) {
        bucket->connected = 1;
    }
}

void
cb_bucket_free(void *ptr)
{
    struct cb_bucket_st *bucket = ptr;

    if (bucket) {
        bucket->destroying = 1;
        if (bucket->handle) {
            lcb_destroy(bucket->handle);
            lcb_destroy_io_ops(bucket->io);
        }
        if (bucket->object_space) {
            st_free_table(bucket->object_space);
        }
        xfree(bucket);
    }
}

static int
cb_bucket_mark_object_i(st_index_t key, st_data_t value, st_data_t arg)
{
    ((mark_f)value)((void *)key, (struct cb_bucket_st *)arg);
    return ST_CONTINUE;
}

void
cb_bucket_mark(void *ptr)
{
    struct cb_bucket_st *bucket = ptr;

    if (bucket) {
        rb_gc_mark(bucket->connstr);
        rb_gc_mark(bucket->exception);
        if (bucket->object_space) {
            st_foreach(bucket->object_space, cb_bucket_mark_object_i, (st_data_t)bucket);
        }
    }
}

static void
do_scan_connection_options(struct cb_bucket_st *bucket, int argc, VALUE *argv)
{
    VALUE uri, opts, arg;

    if (rb_scan_args(argc, argv, "02", &uri, &opts) > 0) {
        if (TYPE(uri) == T_HASH && argc == 1) {
            opts = uri;
            uri = Qnil;
        }
        if (NIL_P(uri)) {
            bucket->connstr = cb_vStrLocalhost;
        } else {
            Check_Type(uri, T_STRING);
            bucket->connstr = uri;
        }
        if (TYPE(opts) == T_HASH) {
            arg = rb_hash_aref(opts, cb_sym_type);
            if (arg != Qnil) {
                if (arg == cb_sym_cluster) {
                    bucket->type = LCB_TYPE_CLUSTER;
                } else {
                    bucket->type = LCB_TYPE_BUCKET;
                }
            }
            arg = rb_hash_aref(opts, cb_sym_node_list);
            if (arg != Qnil) {
                rb_warning("passing a :node_list to Bucket#new is deprecated, use connection string");
            }
            arg = rb_hash_aref(opts, cb_sym_bootstrap_transports);
            if (arg != Qnil) {
                rb_warning("passing a :bootstrap_transports to Bucket#new is deprecated, use connection string option "
                           "`bootstrap_on`");
            }
            arg = rb_hash_aref(opts, cb_sym_hostname);
            if (arg != Qnil) {
                rb_warning("passing a :hostname to Bucket#new is deprecated, use connection string");
            } else {
                arg = rb_hash_aref(opts, cb_sym_host);
                if (arg != Qnil) {
                    rb_warning("passing a :host to Bucket#new is deprecated, use connection string");
                }
            }
            if (rb_hash_aref(opts, cb_sym_pool) != Qnil) {
                rb_warning("passing a :pool to Bucket#new is deprecated, use connection string");
            }
            arg = rb_hash_aref(opts, cb_sym_bucket);
            if (arg != Qnil) {
                rb_warning("passing a :bucket to Bucket#new is deprecated, use connection string");
            }
            arg = rb_hash_aref(opts, cb_sym_username);
            if (arg != Qnil) {
                bucket->username = rb_str_dup_frozen(StringValue(arg));
            }
            arg = rb_hash_aref(opts, cb_sym_password);
            if (arg != Qnil) {
                bucket->password = rb_str_dup_frozen(StringValue(arg));
            }
            arg = rb_hash_aref(opts, cb_sym_port);
            if (arg != Qnil) {
                rb_warning("passing a :port to Bucket#new is deprecated, use connection string");
            }
            arg = rb_hash_lookup2(opts, cb_sym_quiet, Qundef);
            if (arg != Qundef) {
                bucket->quiet = RTEST(arg);
            }
            arg = rb_hash_aref(opts, cb_sym_timeout);
            if (arg != Qnil) {
                bucket->timeout = (uint32_t)NUM2ULONG(arg);
            }
            arg = rb_hash_aref(opts, cb_sym_default_ttl);
            if (arg != Qnil) {
                bucket->default_ttl = (uint32_t)NUM2ULONG(arg);
            }
            arg = rb_hash_aref(opts, cb_sym_default_observe_timeout);
            if (arg != Qnil) {
                bucket->default_observe_timeout = (uint32_t)NUM2ULONG(arg);
            }
            arg = rb_hash_aref(opts, cb_sym_default_flags);
            if (arg != Qnil) {
                bucket->default_flags = (uint32_t)NUM2ULONG(arg);
            }
            arg = rb_hash_aref(opts, cb_sym_default_format);
            if (arg != Qnil) {
                if (TYPE(arg) == T_FIXNUM) {
                    rb_warn("numeric argument to :default_format option is deprecated, use symbol");
                    switch (FIX2INT(arg)) {
                    case CB_FMT_DOCUMENT:
                        arg = cb_sym_document;
                        break;
                    case CB_FMT_MARSHAL:
                        arg = cb_sym_marshal;
                        break;
                    case CB_FMT_PLAIN:
                        arg = cb_sym_plain;
                        break;
                    }
                }
                if (arg == cb_sym_document) {
                    cb_bucket_transcoder_set(bucket->self, cb_mDocument);
                } else if (arg == cb_sym_marshal) {
                    cb_bucket_transcoder_set(bucket->self, cb_mMarshal);
                } else if (arg == cb_sym_plain) {
                    cb_bucket_transcoder_set(bucket->self, cb_mPlain);
                }
            }
            arg = rb_hash_lookup2(opts, cb_sym_transcoder, Qundef);
            if (arg != Qundef) {
                cb_bucket_transcoder_set(bucket->self, arg);
            }
            arg = rb_hash_aref(opts, cb_sym_environment);
            if (arg != Qnil) {
                if (arg == cb_sym_production || arg == cb_sym_development) {
                    bucket->environment = arg;
                }
            }
            arg = rb_hash_aref(opts, cb_sym_default_arithmetic_init);
            if (arg != Qnil) {
                bucket->default_arith_create = RTEST(arg);
                if (TYPE(arg) == T_FIXNUM) {
                    bucket->default_arith_init = NUM2ULL(arg);
                }
            }
            arg = rb_hash_aref(opts, cb_sym_engine);
            if (arg != Qnil) {
                if (arg == cb_sym_default) {
                    bucket->engine = cb_sym_default;
                } else if (arg == cb_sym_select) {
                    bucket->engine = cb_sym_select;
#ifdef _WIN32
                } else if (arg == cb_sym_iocp) {
                    bucket->engine = cb_sym_iocp;
#else
                } else if (arg == cb_sym_libev) {
                    bucket->engine = cb_sym_libev;
                } else if (arg == cb_sym_libevent) {
                    bucket->engine = cb_sym_libevent;
#endif
                } else {
                    VALUE ins = rb_funcall(arg, rb_intern("inspect"), 0);
                    rb_raise(rb_eArgError, "Couchbase: unknown engine %s", RSTRING_PTR(ins));
                }
            }
            arg = rb_hash_aref(opts, cb_sym_transcoder);
            if (arg != Qnil) {
                bucket->default_arith_create = RTEST(arg);
                if (TYPE(arg) == T_FIXNUM) {
                    bucket->default_arith_init = NUM2ULL(arg);
                }
            }
        } else {
            opts = Qnil;
        }
    }
    if (bucket->default_observe_timeout < 2) {
        rb_raise(rb_eArgError, "default_observe_timeout is too low");
    }
}

static void
do_connect(struct cb_bucket_st *bucket)
{
    lcb_error_t err;
    struct lcb_create_st create_opts;

    if (bucket->handle) {
        cb_bucket_disconnect(bucket->self);
    }

    {
        struct lcb_create_io_ops_st ciops;
        memset(&ciops, 0, sizeof(ciops));
        ciops.version = 0;

        if (bucket->engine == cb_sym_libevent) {
            ciops.v.v0.type = LCB_IO_OPS_LIBEVENT;
        } else if (bucket->engine == cb_sym_select) {
            ciops.v.v0.type = LCB_IO_OPS_SELECT;
#ifdef _WIN32
        } else if (bucket->engine == cb_sym_iocp) {
            ciops.v.v0.type = LCB_IO_OPS_WINIOCP;
#endif
        } else if (bucket->engine == cb_sym_libev) {
            ciops.v.v0.type = LCB_IO_OPS_LIBEV;
        } else {
#ifdef _WIN32
            ciops.v.v0.type = LCB_IO_OPS_DEFAULT;
#else
            ciops.version = 2;
            ciops.v.v2.create = cb_create_ruby_mt_io_opts;
            ciops.v.v2.cookie = NULL;
#endif
        }
        err = lcb_create_io_ops(&bucket->io, &ciops);
        if (err != LCB_SUCCESS) {
            rb_exc_raise(cb_check_error(err, "failed to create IO instance", Qnil));
        }
    }

    memset(&create_opts, 0, sizeof(struct lcb_create_st));
    create_opts.version = 3;
    create_opts.v.v3.type = bucket->type;
    create_opts.v.v3.connstr = RTEST(bucket->connstr) ? RSTRING_PTR(bucket->connstr) : NULL;
    create_opts.v.v3.username = RTEST(bucket->username) ? RSTRING_PTR(bucket->username) : NULL;
    create_opts.v.v2.passwd = RTEST(bucket->password) ? RSTRING_PTR(bucket->password) : NULL;
    create_opts.v.v3.io = bucket->io;
    err = lcb_create(&bucket->handle, &create_opts);
    if (err != LCB_SUCCESS) {
        bucket->handle = NULL;
        rb_exc_raise(cb_check_error(err, "failed to create libcouchbase instance", Qnil));
    }
    lcb_set_cookie(bucket->handle, bucket);
    (void)lcb_set_bootstrap_callback(bucket->handle, bootstrap_callback);
    (void)lcb_install_callback3(bucket->handle, LCB_CALLBACK_HTTP, cb_http_callback);
    (void)lcb_install_callback3(bucket->handle, LCB_CALLBACK_OBSERVE, cb_observe_callback);
    (void)lcb_install_callback3(bucket->handle, LCB_CALLBACK_STORE, cb_storage_callback);
    (void)lcb_install_callback3(bucket->handle, LCB_CALLBACK_STOREDUR, cb_storage_callback);
    (void)lcb_install_callback3(bucket->handle, LCB_CALLBACK_REMOVE, cb_remove_callback);
    (void)lcb_install_callback3(bucket->handle, LCB_CALLBACK_VERSIONS, cb_version_callback);
    (void)lcb_install_callback3(bucket->handle, LCB_CALLBACK_STATS, cb_stat_callback);
    (void)lcb_install_callback3(bucket->handle, LCB_CALLBACK_COUNTER, cb_arithmetic_callback);
    (void)lcb_install_callback3(bucket->handle, LCB_CALLBACK_UNLOCK, cb_unlock_callback);
    (void)lcb_install_callback3(bucket->handle, LCB_CALLBACK_TOUCH, cb_touch_callback);
    (void)lcb_install_callback3(bucket->handle, LCB_CALLBACK_GET, cb_get_callback);
    (void)lcb_install_callback3(bucket->handle, LCB_CALLBACK_GETREPLICA, cb_get_callback);

    lcb_cntl(bucket->handle, (bucket->timeout > 0) ? LCB_CNTL_SET : LCB_CNTL_GET, LCB_CNTL_OP_TIMEOUT,
             &bucket->timeout);
    err = lcb_connect(bucket->handle);
    if (err != LCB_SUCCESS) {
        cb_bucket_disconnect(bucket->self);
        rb_exc_raise(cb_check_error(err, "failed to connect libcouchbase instance to server", Qnil));
    }
    bucket->exception = Qnil;
    lcb_wait(bucket->handle);
    if (bucket->exception != Qnil) {
        cb_bucket_disconnect(bucket->self);
        rb_exc_raise(bucket->exception);
    }
}

VALUE
cb_bucket_alloc(VALUE klass)
{
    VALUE obj;
    struct cb_bucket_st *bucket;

    /* allocate new bucket struct and set it to zero */
    obj = Data_Make_Struct(klass, struct cb_bucket_st, cb_bucket_mark, cb_bucket_free, bucket);
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
 *     exception when the client executes operations on non-existent keys. If it
 *     is +true+ it will raise {Couchbase::Error::NotFound} exceptions. The
 *     default behaviour is to return +nil+ value silently (might be useful in
 *     Rails cache).
 *   @option options [Symbol] :environment (:production) the mode of the
 *     connection. Currently it influences only on design documents set. If
 *     the environment is +:development+, you will able to get design
 *     documents with 'dev_' prefix, otherwise (in +:production+ mode) the
 *     library will hide them from you.
 *   @option options [Fixnum] :timeout (2500000) the timeout for IO
 *     operations (in microseconds)
 *   @option options [Fixnum, true] :default_arithmetic_init (0) the default
 *     initial value for arithmetic operations. Setting this option to any
 *     non positive number forces creation missing keys with given default
 *     value. Setting it to +true+ will use zero as initial value. (see
 *     {Bucket#incr} and {Bucket#decr}).
 *   @option options [Symbol] :engine (:default) the IO engine to use
 *     Currently following engines are supported:
 *     :default      :: Built-in engine (multi-thread friendly)
 *     :select       :: select(2) IO plugin from libcouchbase
 *     :iocp         :: "I/O Completion Ports" plugin from libcouchbase (windows only)
 *     :libevent     :: libevent IO plugin from libcouchbase (optional)
 *     :libev        :: libev IO plugin from libcouchbase (optional)
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
 * @raise [Couchbase::Error::BucketNotFound] if there is no such bucket to
 *   connect to
 *
 * @raise [Couchbase::Error::Connect] if the socket wasn't accessible
 *   (doesn't accept connections or doesn't respond in time)
 *
 * @return [Bucket]
 */
VALUE
cb_bucket_init(int argc, VALUE *argv, VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    bucket->self = self;
    bucket->exception = Qnil;
    bucket->type = LCB_TYPE_BUCKET;
    bucket->username = Qnil;
    bucket->password = Qnil;
    bucket->engine = cb_sym_default;
    bucket->quiet = 0;
    bucket->default_ttl = 0;
    bucket->default_flags = 0;
    cb_bucket_transcoder_set(self, cb_mDocument);
    bucket->default_observe_timeout = 2500000;
    bucket->timeout = 0;
    bucket->environment = cb_sym_production;
    bucket->object_space = st_init_numtable();
    bucket->destroying = 0;
    bucket->connected = 0;

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
VALUE
cb_bucket_init_copy(VALUE copy, VALUE orig)
{
    struct cb_bucket_st *copy_b;
    struct cb_bucket_st *orig_b;

    if (copy == orig)
        return copy;

    if (TYPE(orig) != T_DATA || TYPE(copy) != T_DATA || RDATA(orig)->dfree != (RUBY_DATA_FUNC)cb_bucket_free) {
        rb_raise(rb_eTypeError, "wrong argument type");
    }

    copy_b = DATA_PTR(copy);
    orig_b = DATA_PTR(orig);

    copy_b->self = copy;
    copy_b->engine = orig_b->engine;
    copy_b->quiet = orig_b->quiet;
    copy_b->transcoder = orig_b->transcoder;
    copy_b->default_flags = orig_b->default_flags;
    copy_b->default_ttl = orig_b->default_ttl;
    copy_b->environment = orig_b->environment;
    copy_b->timeout = orig_b->timeout;
    copy_b->exception = Qnil;
    copy_b->object_space = st_init_numtable();
    copy_b->destroying = 0;
    copy_b->connected = 0;

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
 *  @return [Couchbase::Bucket]
 *
 * @overload reconnect(options = {})
 *  see {Bucket#initialize Bucket#initialize(options)}
 *  @return [Couchbase::Bucket]
 *
 *  @example reconnect with current parameters
 *    c.reconnect
 *
 *  @example reconnect the instance to another bucket
 *    c.reconnect(:bucket => 'new')
 */
VALUE
cb_bucket_reconnect(int argc, VALUE *argv, VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);

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
VALUE
cb_bucket_connected_p(VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    return (bucket->handle && bucket->connected) ? Qtrue : Qfalse;
}

VALUE
cb_bucket_quiet_get(VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    return bucket->quiet ? Qtrue : Qfalse;
}

VALUE
cb_bucket_quiet_set(VALUE self, VALUE val)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);

    bucket->quiet = RTEST(val);
    return bucket->quiet ? Qtrue : Qfalse;
}

VALUE
cb_bucket_default_flags_get(VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    return ULONG2NUM(bucket->default_flags);
}

VALUE
cb_bucket_default_flags_set(VALUE self, VALUE val)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);

    bucket->default_flags = (uint32_t)NUM2ULONG(val);
    return val;
}

VALUE
cb_bucket_transcoder_get(VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    return bucket->transcoder;
}

VALUE
cb_bucket_transcoder_set(VALUE self, VALUE val)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);

    if (val != Qnil && !rb_respond_to(val, cb_id_dump) && !rb_respond_to(val, cb_id_load)) {
        rb_raise(rb_eArgError, "transcoder must respond to dump and load methods");
    }
    bucket->transcoder = val;

    return bucket->transcoder;
}

VALUE
cb_bucket_default_format_get(VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);

    if (bucket->transcoder == cb_mDocument) {
        return cb_sym_document;
    } else if (bucket->transcoder == cb_mMarshal) {
        return cb_sym_marshal;
    } else if (bucket->transcoder == cb_mPlain) {
        return cb_sym_plain;
    }
    return Qnil;
}

VALUE
cb_bucket_default_format_set(VALUE self, VALUE val)
{
    if (TYPE(val) == T_FIXNUM) {
        rb_warn("numeric argument to #default_format option is deprecated, use symbol");
        switch (FIX2INT(val)) {
        case CB_FMT_DOCUMENT:
            val = cb_sym_document;
            break;
        case CB_FMT_MARSHAL:
            val = cb_sym_marshal;
            break;
        case CB_FMT_PLAIN:
            val = cb_sym_plain;
            break;
        }
    }
    if (val == cb_sym_document) {
        cb_bucket_transcoder_set(self, cb_mDocument);
    } else if (val == cb_sym_marshal) {
        cb_bucket_transcoder_set(self, cb_mMarshal);
    } else if (val == cb_sym_plain) {
        cb_bucket_transcoder_set(self, cb_mPlain);
    } else {
        rb_raise(rb_eArgError, "unknown format");
    }

    return val;
}

VALUE
cb_bucket_timeout_get(VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    return ULONG2NUM(bucket->timeout);
}

VALUE
cb_bucket_timeout_set(VALUE self, VALUE val)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    VALUE tmval;

    bucket->timeout = (uint32_t)NUM2ULONG(val);
    lcb_cntl(bucket->handle, LCB_CNTL_SET, LCB_CNTL_OP_TIMEOUT, &bucket->timeout);
    tmval = ULONG2NUM(bucket->timeout);

    return tmval;
}

VALUE
cb_bucket_default_arithmetic_init_get(VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    return ULL2NUM(bucket->default_arith_init);
}

VALUE
cb_bucket_default_arithmetic_init_set(VALUE self, VALUE val)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);

    bucket->default_arith_create = RTEST(val);
    if (bucket->default_arith_create) {
        bucket->default_arith_init = NUM2ULL(val);
    } else {
        bucket->default_arith_init = 0;
    }
    return ULL2NUM(bucket->default_arith_init);
}

/* Document-method: bucket
 *
 * @since 1.0.0
 *
 * @return [String] the bucket name
 */
VALUE
cb_bucket_bucket_get(VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    const char *name = NULL;
    lcb_cntl(bucket->handle, LCB_CNTL_GET, LCB_CNTL_BUCKETNAME, &name);
    return rb_str_buf_new2(name);
}

/* Document-method: environment
 *
 * @since 1.2.0
 *
 * @see Bucket#initialize
 *
 * @return [Symbol] the environment (+:development+ or +:production+)
 */
VALUE
cb_bucket_environment_get(VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    return bucket->environment;
}
/* Document-method: num_replicas
 *
 * @since 1.2.0.dp6
 *
 * The numbers of the replicas for each node in the cluster
 *
 * @return [Fixnum]
 */
VALUE
cb_bucket_num_replicas_get(VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    int32_t nr = lcb_get_num_replicas(bucket->handle);
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
 * @return [Fixnum]
 */
VALUE
cb_bucket_default_observe_timeout_get(VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    return INT2FIX(bucket->default_observe_timeout);
}

/* Document-method: default_observe_timeout=
 *
 * @since 1.2.0.dp6
 *
 * Set default timeout value for {Bucket#observe_and_wait} operation in
 * microseconds
 *
 * @return [Fixnum]
 */
VALUE
cb_bucket_default_observe_timeout_set(VALUE self, VALUE val)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    bucket->default_observe_timeout = FIX2INT(val);
    return val;
}

/* Document-method: connstr
 *
 * @since 1.4.0
 *
 * @return [String] the bootstrap address
 */
VALUE
cb_bucket_connstr_get(VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    return bucket->connstr;
}

/*
 * Returns a string containing a human-readable representation of the
 * {Bucket}.
 *
 * @since 1.0.0
 *
 * @return [String]
 */
VALUE
cb_bucket_inspect(VALUE self)
{
    VALUE str;
    struct cb_bucket_st *bucket = DATA_PTR(self);
    char buf[200];

    str = rb_str_buf_new2("#<");
    rb_str_buf_cat2(str, rb_obj_classname(self));
    snprintf(buf, 25, ":%p \"", (void *)self);
    rb_str_buf_cat2(str, buf);
    rb_str_append(str, bucket->connstr);
    rb_str_buf_cat2(str, "/\" transcoder=");
    rb_str_append(str, rb_inspect(bucket->transcoder));
    snprintf(buf, 150, ", default_flags=0x%x, quiet=%s, connected=%s, timeout=%u", bucket->default_flags,
             bucket->quiet ? "true" : "false", (bucket->handle && bucket->connected) ? "true" : "false",
             bucket->timeout);
    rb_str_buf_cat2(str, buf);
    if (bucket->handle && bucket->connected) {
        lcb_config_transport_t type;
        rb_str_buf_cat2(str, ", bootstrap_transport=");
        lcb_cntl(bucket->handle, LCB_CNTL_GET, LCB_CNTL_CONFIG_TRANSPORT, &type);
        switch (type) {
        case LCB_CONFIG_TRANSPORT_HTTP:
            rb_str_buf_cat2(str, ":http");
            break;
        case LCB_CONFIG_TRANSPORT_CCCP:
            rb_str_buf_cat2(str, ":cccp");
            break;
        default:
            rb_str_buf_cat2(str, "<unknown>");
            break;
        }
    }
    rb_str_buf_cat2(str, ">");

    return str;
}

static void
do_loop(struct cb_bucket_st *bucket)
{
    lcb_wait(bucket->handle);
    bucket->nbytes = 0;
}

void
cb_maybe_do_loop(struct cb_bucket_st *bucket)
{
    if (bucket->threshold != 0 && bucket->nbytes > bucket->threshold) {
        do_loop(bucket);
    }
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
VALUE
cb_bucket_disconnect(VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);

    if (bucket->handle) {
        lcb_destroy(bucket->handle);
        lcb_destroy_io_ops(bucket->io);
        bucket->handle = NULL;
        bucket->io = NULL;
        bucket->connected = 0;
        return Qtrue;
    } else {
        rb_raise(cb_eConnectError, "closed connection");
        return Qfalse;
    }
}
