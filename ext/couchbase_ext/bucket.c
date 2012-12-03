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

#include "couchbase_ext.h"

    static void
error_callback(lcb_t handle, lcb_error_t error, const char *errinfo)
{
    struct cb_bucket_st *bucket = (struct cb_bucket_st *)lcb_get_cookie(handle);

    lcb_breakout(handle);
    bucket->exception = cb_check_error(error, errinfo, Qnil);
}

    void
cb_bucket_free(void *ptr)
{
    struct cb_bucket_st *bucket = ptr;

    if (bucket) {
        if (bucket->handle) {
            lcb_destroy(bucket->handle);
            lcb_destroy_io_ops(bucket->io);
        }
    }
    xfree(bucket);
}

    void
cb_bucket_mark(void *ptr)
{
    struct cb_bucket_st *bucket = ptr;

    if (bucket) {
        rb_gc_mark(bucket->authority);
        rb_gc_mark(bucket->hostname);
        rb_gc_mark(bucket->pool);
        rb_gc_mark(bucket->bucket);
        rb_gc_mark(bucket->username);
        rb_gc_mark(bucket->password);
        rb_gc_mark(bucket->exception);
        rb_gc_mark(bucket->on_error_proc);
        rb_gc_mark(bucket->key_prefix_val);
        rb_gc_mark(bucket->object_space);
    }
}

    static void
do_scan_connection_options(struct cb_bucket_st *bucket, int argc, VALUE *argv)
{
    VALUE uri, opts, arg;
    char port_s[8];

    if (rb_scan_args(argc, argv, "02", &uri, &opts) > 0) {
        if (TYPE(uri) == T_HASH && argc == 1) {
            opts = uri;
            uri = Qnil;
        }
        if (uri != Qnil) {
            const char path_re[] = "^(/pools/([A-Za-z0-9_.-]+)(/buckets/([A-Za-z0-9_.-]+))?)?";
            VALUE match, uri_obj, re;

            Check_Type(uri, T_STRING);
            uri_obj = rb_funcall(cb_mURI, cb_id_parse, 1, uri);

            arg = rb_funcall(uri_obj, cb_id_scheme, 0);
            if (arg == Qnil || rb_str_cmp(arg, STR_NEW_CSTR("http"))) {
                rb_raise(rb_eArgError, "invalid URI: invalid scheme");
            }

            arg = rb_funcall(uri_obj, cb_id_user, 0);
            if (arg != Qnil) {
                bucket->username = rb_str_dup_frozen(StringValue(arg));
            }

            arg = rb_funcall(uri_obj, cb_id_password, 0);
            if (arg != Qnil) {
                bucket->password = rb_str_dup_frozen(StringValue(arg));
            }
            arg = rb_funcall(uri_obj, cb_id_host, 0);
            if (arg != Qnil) {
                bucket->hostname = rb_str_dup_frozen(StringValue(arg));
            } else {
                rb_raise(rb_eArgError, "invalid URI: missing hostname");
            }

            arg = rb_funcall(uri_obj, cb_id_port, 0);
            bucket->port = NIL_P(arg) ? 8091 : (uint16_t)NUM2UINT(arg);

            arg = rb_funcall(uri_obj, cb_id_path, 0);
            re = rb_reg_new(path_re, sizeof(path_re) - 1, 0);
            match = rb_funcall(re, cb_id_match, 1, arg);
            arg = rb_reg_nth_match(2, match);
            bucket->pool = NIL_P(arg) ? STR_NEW_CSTR("default") : rb_str_dup_frozen(StringValue(arg));
            rb_str_freeze(bucket->pool);
            arg = rb_reg_nth_match(4, match);
            bucket->bucket = NIL_P(arg) ? STR_NEW_CSTR("default") : rb_str_dup_frozen(StringValue(arg));
            rb_str_freeze(bucket->bucket);
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
                Check_Type(arg, T_ARRAY);
                bucket->node_list = rb_ary_join(arg, STR_NEW_CSTR(";"));
                rb_str_freeze(bucket->node_list);
            }
            arg = rb_hash_aref(opts, cb_sym_hostname);
            if (arg != Qnil) {
                bucket->hostname = rb_str_dup_frozen(StringValue(arg));
            }
            arg = rb_hash_aref(opts, cb_sym_pool);
            if (arg != Qnil) {
                bucket->pool = rb_str_dup_frozen(StringValue(arg));
            }
            arg = rb_hash_aref(opts, cb_sym_bucket);
            if (arg != Qnil) {
                bucket->bucket = rb_str_dup_frozen(StringValue(arg));
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
                bucket->port = (uint16_t)NUM2UINT(arg);
            }
            if (RTEST(rb_funcall(opts, cb_id_has_key_p, 1, cb_sym_quiet))) {
                bucket->quiet = RTEST(rb_hash_aref(opts, cb_sym_quiet));
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
                if (arg == cb_sym_document || arg == cb_sym_marshal || arg == cb_sym_plain) {
                    bucket->default_format = arg;
                    bucket->default_flags = cb_flags_set_format(bucket->default_flags, arg);
                }
            }
            arg = rb_hash_aref(opts, cb_sym_environment);
            if (arg != Qnil) {
                if (arg == cb_sym_production || arg == cb_sym_development) {
                    bucket->environment = arg;
                }
            }
            arg = rb_hash_aref(opts, cb_sym_key_prefix);
            if (arg != Qnil) {
                bucket->key_prefix_val = rb_str_dup_frozen(StringValue(arg));
            }
            arg = rb_hash_aref(opts, cb_sym_default_arithmetic_init);
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
    if (RTEST(bucket->password) && !RTEST(bucket->username)) {
        bucket->username = bucket->bucket;
    }
    if (bucket->default_observe_timeout < 2) {
        rb_raise(rb_eArgError, "default_observe_timeout is too low");
    }
    snprintf(port_s, sizeof(port_s), ":%u", bucket->port);
    bucket->authority = rb_str_dup(bucket->hostname);
    rb_str_cat2(bucket->authority, port_s);
    rb_str_freeze(bucket->authority);
}

    static void
do_connect(struct cb_bucket_st *bucket)
{
    lcb_error_t err;
    struct lcb_create_st create_opts;

    if (bucket->handle) {
        lcb_destroy(bucket->handle);
        lcb_destroy_io_ops(bucket->io);
        bucket->handle = NULL;
        bucket->io = NULL;
    }
    err = lcb_create_io_ops(&bucket->io, NULL);
    if (err != LCB_SUCCESS) {
        rb_exc_raise(cb_check_error(err, "failed to create IO instance", Qnil));
    }

    memset(&create_opts, 0, sizeof(struct lcb_create_st));
    create_opts.version = 1;
    create_opts.v.v1.type = bucket->type;
    create_opts.v.v1.host = RTEST(bucket->node_list) ? RSTRING_PTR(bucket-> node_list) : RSTRING_PTR(bucket->authority);
    create_opts.v.v1.user = RTEST(bucket->username) ? RSTRING_PTR(bucket->username) : NULL;
    create_opts.v.v1.passwd = RTEST(bucket->username) ? RSTRING_PTR(bucket->password) : NULL;
    create_opts.v.v1.bucket = RSTRING_PTR(bucket->bucket);
    create_opts.v.v1.io = bucket->io;
    err = lcb_create(&bucket->handle, &create_opts);
    if (err != LCB_SUCCESS) {
        rb_exc_raise(cb_check_error(err, "failed to create libcouchbase instance", Qnil));
    }
    lcb_set_cookie(bucket->handle, bucket);
    (void)lcb_set_error_callback(bucket->handle, error_callback);
    (void)lcb_set_store_callback(bucket->handle, cb_storage_callback);
    (void)lcb_set_get_callback(bucket->handle, cb_get_callback);
    (void)lcb_set_touch_callback(bucket->handle, cb_touch_callback);
    (void)lcb_set_remove_callback(bucket->handle, cb_delete_callback);
    (void)lcb_set_stat_callback(bucket->handle, cb_stat_callback);
    (void)lcb_set_arithmetic_callback(bucket->handle, cb_arithmetic_callback);
    (void)lcb_set_version_callback(bucket->handle, cb_version_callback);
    (void)lcb_set_http_complete_callback(bucket->handle, cb_http_complete_callback);
    (void)lcb_set_http_data_callback(bucket->handle, cb_http_data_callback);
    (void)lcb_set_observe_callback(bucket->handle, cb_observe_callback);
    (void)lcb_set_unlock_callback(bucket->handle, cb_unlock_callback);

    if (bucket->timeout > 0) {
        lcb_set_timeout(bucket->handle, bucket->timeout);
    } else {
        bucket->timeout = lcb_get_timeout(bucket->handle);
    }
    err = lcb_connect(bucket->handle);
    if (err != LCB_SUCCESS) {
        lcb_destroy(bucket->handle);
        lcb_destroy_io_ops(bucket->io);
        bucket->handle = NULL;
        bucket->io = NULL;
        rb_exc_raise(cb_check_error(err, "failed to connect libcouchbase instance to server", Qnil));
    }
    bucket->exception = Qnil;
    lcb_wait(bucket->handle);
    if (bucket->exception != Qnil) {
        lcb_destroy(bucket->handle);
        lcb_destroy_io_ops(bucket->io);
        bucket->handle = NULL;
        bucket->io = NULL;
        rb_exc_raise(bucket->exception);
    }
}

    VALUE
cb_bucket_alloc(VALUE klass)
{
    VALUE obj;
    struct cb_bucket_st *bucket;

    /* allocate new bucket struct and set it to zero */
    obj = Data_Make_Struct(klass, struct cb_bucket_st, cb_bucket_mark, cb_bucket_free,
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
 *   @option options [Fixnum, true] :default_arithmetic_init (0) the default
 *     initial value for arithmetic operations. Setting this option to any
 *     non positive number forces creation missing keys with given default
 *     value. Setting it to +true+ will use zero as initial value. (see
 *     {Bucket#incr} and {Bucket#decr}).
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
    VALUE
cb_bucket_init(int argc, VALUE *argv, VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);

    bucket->self = self;
    bucket->exception = Qnil;
    bucket->type = LCB_TYPE_BUCKET;
    bucket->hostname = rb_str_new2("localhost");
    bucket->port = 8091;
    bucket->pool = rb_str_new2("default");
    rb_str_freeze(bucket->pool);
    bucket->bucket = rb_str_new2("default");
    rb_str_freeze(bucket->bucket);
    bucket->username = Qnil;
    bucket->password = Qnil;
    bucket->async = 0;
    bucket->quiet = 0;
    bucket->default_ttl = 0;
    bucket->default_flags = 0;
    bucket->default_format = cb_sym_document;
    bucket->default_observe_timeout = 2500000;
    bucket->on_error_proc = Qnil;
    bucket->timeout = 0;
    bucket->environment = cb_sym_production;
    bucket->key_prefix_val = Qnil;
    bucket->node_list = Qnil;
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
    VALUE
cb_bucket_init_copy(VALUE copy, VALUE orig)
{
    struct cb_bucket_st *copy_b;
    struct cb_bucket_st *orig_b;

    if (copy == orig)
        return copy;

    if (TYPE(orig) != T_DATA || TYPE(copy) != T_DATA ||
            RDATA(orig)->dfree != (RUBY_DATA_FUNC)cb_bucket_free) {
        rb_raise(rb_eTypeError, "wrong argument type");
    }

    copy_b = DATA_PTR(copy);
    orig_b = DATA_PTR(orig);

    copy_b->self = copy_b->self;
    copy_b->port = orig_b->port;
    copy_b->authority = orig_b->authority;
    copy_b->hostname = orig_b->hostname;
    copy_b->pool = orig_b->pool;
    copy_b->bucket = orig_b->bucket;
    copy_b->username = orig_b->username;
    copy_b->password = orig_b->password;
    copy_b->async = orig_b->async;
    copy_b->quiet = orig_b->quiet;
    copy_b->default_format = orig_b->default_format;
    copy_b->default_flags = orig_b->default_flags;
    copy_b->default_ttl = orig_b->default_ttl;
    copy_b->environment = orig_b->environment;
    copy_b->timeout = orig_b->timeout;
    copy_b->exception = Qnil;
    if (orig_b->on_error_proc != Qnil) {
        copy_b->on_error_proc = rb_funcall(orig_b->on_error_proc, cb_id_dup, 0);
    }
    copy_b->key_prefix_val = orig_b->key_prefix_val;

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
    VALUE
cb_bucket_async_p(VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    return bucket->async ? Qtrue : Qfalse;
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
    VALUE new;

    bucket->quiet = RTEST(val);
    new = bucket->quiet ? Qtrue : Qfalse;
    return new;
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
    bucket->default_format = cb_flags_get_format(bucket->default_flags);
    return val;
}

    VALUE
cb_bucket_default_format_get(VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    return bucket->default_format;
}

    VALUE
cb_bucket_default_format_set(VALUE self, VALUE val)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);

    if (TYPE(val) == T_FIXNUM) {
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
    if (val == cb_sym_document || val == cb_sym_marshal || val == cb_sym_plain) {
        bucket->default_format = val;
        bucket->default_flags = cb_flags_set_format(bucket->default_flags, val);
    }

    return val;
}

    VALUE
cb_bucket_on_error_set(VALUE self, VALUE val)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);

    if (rb_respond_to(val, cb_id_call)) {
        bucket->on_error_proc = val;
    } else {
        bucket->on_error_proc = Qnil;
    }

    return bucket->on_error_proc;
}

    VALUE
cb_bucket_on_error_get(VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);

    if (rb_block_given_p()) {
        return cb_bucket_on_error_set(self, rb_block_proc());
    } else {
        return bucket->on_error_proc;
    }
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
    lcb_set_timeout(bucket->handle, bucket->timeout);
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

    VALUE
cb_bucket_key_prefix_get(VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    return bucket->key_prefix_val;
}

    VALUE
cb_bucket_key_prefix_set(VALUE self, VALUE val)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);

    bucket->key_prefix_val = rb_str_dup_frozen(StringValue(val));

    return bucket->key_prefix_val;
}

/* Document-method: hostname
 *
 * @since 1.0.0
 *
 * @return [String] the host name of the management interface (default: "localhost")
 */
    VALUE
cb_bucket_hostname_get(VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);

    if (bucket->handle) {
        const char * host = lcb_get_host(bucket->handle);
        unsigned long len = RSTRING_LEN(bucket->hostname);
        if (len != strlen(host) || strncmp(RSTRING_PTR(bucket->hostname), host, len) != 0) {
            bucket->hostname = STR_NEW_CSTR(host);
            rb_str_freeze(bucket->hostname);
        }
    }
    return bucket->hostname;
}

/* Document-method: port
 *
 * @since 1.0.0
 *
 * @return [Fixnum] the port number of the management interface (default: 8091)
 */
    VALUE
cb_bucket_port_get(VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    if (bucket->handle) {
        bucket->port = atoi(lcb_get_port(bucket->handle));
    }
    return UINT2NUM(bucket->port);
}

/* Document-method: authority
 *
 * @since 1.0.0
 *
 * @return [String] host with port
 */
    VALUE
cb_bucket_authority_get(VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    VALUE old_hostname = bucket->hostname;
    uint16_t old_port = bucket->port;
    VALUE hostname = cb_bucket_hostname_get(self);
    cb_bucket_port_get(self);

    if (hostname != old_hostname || bucket->port != old_port) {
        char port_s[8];
        snprintf(port_s, sizeof(port_s), ":%u", bucket->port);
        bucket->authority = rb_str_dup(hostname);
        rb_str_cat2(bucket->authority, port_s);
        rb_str_freeze(bucket->authority);
    }
    return bucket->authority;
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
    return bucket->bucket;
}

/* Document-method: pool
 *
 * @since 1.0.0
 *
 * @return [String] the pool name (usually "default")
 */
    VALUE
cb_bucket_pool_get(VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    return bucket->pool;
}

/* Document-method: username
 *
 * @since 1.0.0
 *
 * @return [String] the username for protected buckets (usually matches
 *   the bucket name)
 */
    VALUE
cb_bucket_username_get(VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    return bucket->username;
}

/* Document-method: password
 *
 * @since 1.0.0
 *
 * @return [String] the password for protected buckets
 */
    VALUE
cb_bucket_password_get(VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    return bucket->password;
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
/* Document-method: url
 *
 * @since 1.0.0
 *
 * @return [String] the address of the cluster management interface
 */
    VALUE
cb_bucket_url_get(VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    VALUE str;

    (void)cb_bucket_authority_get(self);
    str = rb_str_buf_new2("http://");
    rb_str_buf_cat2(str, RSTRING_PTR(bucket->authority));
    rb_str_buf_cat2(str, "/pools/");
    rb_str_buf_cat2(str, RSTRING_PTR(bucket->pool));
    rb_str_buf_cat2(str, "/buckets/");
    rb_str_buf_cat2(str, RSTRING_PTR(bucket->bucket));
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
    VALUE
cb_bucket_inspect(VALUE self)
{
    VALUE str;
    struct cb_bucket_st *bucket = DATA_PTR(self);
    char buf[200];

    str = rb_str_buf_new2("#<");
    rb_str_buf_cat2(str, rb_obj_classname(self));
    snprintf(buf, 25, ":%p \"", (void *)self);
    (void)cb_bucket_authority_get(self);
    rb_str_buf_cat2(str, buf);
    rb_str_buf_cat2(str, "http://");
    rb_str_buf_cat2(str, RSTRING_PTR(bucket->authority));
    rb_str_buf_cat2(str, "/pools/");
    rb_str_buf_cat2(str, RSTRING_PTR(bucket->pool));
    rb_str_buf_cat2(str, "/buckets/");
    rb_str_buf_cat2(str, RSTRING_PTR(bucket->bucket));
    rb_str_buf_cat2(str, "/");
    snprintf(buf, 150, "\" default_format=:%s, default_flags=0x%x, quiet=%s, connected=%s, timeout=%u",
            rb_id2name(SYM2ID(bucket->default_format)),
            bucket->default_flags,
            bucket->quiet ? "true" : "false",
            bucket->handle ? "true" : "false",
            bucket->timeout);
    rb_str_buf_cat2(str, buf);
    if (RTEST(bucket->key_prefix_val)) {
        rb_str_buf_cat2(str, ", key_prefix=");
        rb_str_append(str, rb_inspect(bucket->key_prefix_val));
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

    static VALUE
do_run(VALUE *args)
{
    VALUE self = args[0], opts = args[1], proc = args[2], exc;
    struct cb_bucket_st *bucket = DATA_PTR(self);

    if (bucket->handle == NULL) {
        rb_raise(cb_eConnectError, "closed connection");
    }
    if (bucket->async) {
        rb_raise(cb_eInvalidError, "nested #run");
    }
    bucket->threshold = 0;
    if (opts != Qnil) {
        VALUE arg;
        Check_Type(opts, T_HASH);
        arg = rb_hash_aref(opts, cb_sym_send_threshold);
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
    struct cb_bucket_st *bucket = DATA_PTR(self);

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
    VALUE
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
    VALUE
cb_bucket_stop(VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    lcb_breakout(bucket->handle);
    return Qnil;
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
        return Qtrue;
    } else {
        rb_raise(cb_eConnectError, "closed connection");
        return Qfalse;
    }
}


