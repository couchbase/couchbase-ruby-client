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

    static VALUE
storage_observe_callback(VALUE args, VALUE cookie)
{
    struct cb_context_st *ctx = (struct cb_context_st *)cookie;
    struct cb_bucket_st *bucket = ctx->bucket;
    VALUE res = rb_ary_shift(args);

    if (ctx->proc != Qnil) {
        rb_ivar_set(res, cb_id_iv_operation, ctx->operation);
        cb_proc_call(bucket, ctx->proc, 1, res);
    }
    if (!RTEST(ctx->observe_options)) {
        ctx->nqueries--;
        if (ctx->nqueries == 0) {
            ctx->proc = Qnil;
            if (bucket->async) {
                cb_context_free(ctx);
            }
        }
    }
    return Qnil;
}

    VALUE
storage_opcode_to_sym(lcb_storage_t operation)
{
    switch(operation) {
        case LCB_ADD:
            return cb_sym_add;
        case LCB_REPLACE:
            return cb_sym_replace;
        case LCB_SET:
            return cb_sym_set;
        case LCB_APPEND:
            return cb_sym_append;
        case LCB_PREPEND:
            return cb_sym_prepend;
        default:
            return Qnil;
    }
}

    void
cb_storage_callback(lcb_t handle, const void *cookie, lcb_storage_t operation,
        lcb_error_t error, const lcb_store_resp_t *resp)
{
    struct cb_context_st *ctx = (struct cb_context_st *)cookie;
    struct cb_bucket_st *bucket = ctx->bucket;
    VALUE key, cas, exc, res;

    key = STR_NEW((const char*)resp->v.v0.key, resp->v.v0.nkey);
    cb_strip_key_prefix(bucket, key);

    cas = resp->v.v0.cas > 0 ? ULL2NUM(resp->v.v0.cas) : Qnil;
    ctx->operation = storage_opcode_to_sym(operation);
    exc = cb_check_error(error, "failed to store value", key);
    if (exc != Qnil) {
        rb_ivar_set(exc, cb_id_iv_cas, cas);
        rb_ivar_set(exc, cb_id_iv_operation, ctx->operation);
        ctx->exception = exc;
    }

    if (bucket->async) { /* asynchronous */
        if (RTEST(ctx->observe_options)) {
            VALUE args[2]; /* it's ok to pass pointer to stack struct here */
            args[0] = rb_hash_new();
            rb_hash_aset(args[0], key, cas);
            args[1] = ctx->observe_options;
            rb_block_call(bucket->self, cb_id_observe_and_wait, 2, args,
                    storage_observe_callback, (VALUE)ctx);
            ctx->observe_options = Qnil;
        } else if (ctx->proc != Qnil) {
            res = rb_class_new_instance(0, NULL, cb_cResult);
            rb_ivar_set(res, cb_id_iv_error, exc);
            rb_ivar_set(res, cb_id_iv_key, key);
            rb_ivar_set(res, cb_id_iv_operation, ctx->operation);
            rb_ivar_set(res, cb_id_iv_cas, cas);
            cb_proc_call(bucket, ctx->proc, 1, res);
        }
    } else {             /* synchronous */
        rb_hash_aset(ctx->rv, key, cas);
    }

    if (!RTEST(ctx->observe_options)) {
        ctx->nqueries--;
        if (ctx->nqueries == 0) {
            ctx->proc = Qnil;
            if (bucket->async) {
                cb_context_free(ctx);
            }
        }
    }
    (void)handle;
}

    static inline VALUE
cb_bucket_store(lcb_storage_t cmd, int argc, VALUE *argv, VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    struct cb_context_st *ctx;
    VALUE rv, proc, exc, obs = Qnil;
    lcb_error_t err;
    struct cb_params_st params;

    if (!cb_bucket_connected_bang(bucket, storage_opcode_to_sym(cmd))) {
        return Qnil;
    }
    memset(&params, 0, sizeof(struct cb_params_st));
    rb_scan_args(argc, argv, "0*&", &params.args, &proc);
    if (!bucket->async && proc != Qnil) {
        rb_raise(rb_eArgError, "synchronous mode doesn't support callbacks");
    }
    params.type = cb_cmd_store;
    params.bucket = bucket;
    params.cmd.store.operation = cmd;
    cb_params_build(&params);
    obs = params.cmd.store.observe;
    ctx = cb_context_alloc(bucket);
    if (!bucket->async) {
        ctx->rv = rb_hash_new();
        ctx->observe_options = obs;
    }
    ctx->proc = proc;
    ctx->nqueries = params.cmd.store.num;
    err = lcb_store(bucket->handle, (const void *)ctx,
            params.cmd.store.num, params.cmd.store.ptr);
    cb_params_destroy(&params);
    exc = cb_check_error(err, "failed to schedule set request", Qnil);
    if (exc != Qnil) {
        cb_context_free(ctx);
        rb_exc_raise(exc);
    }
    bucket->nbytes += params.npayload;
    if (bucket->async) {
        cb_maybe_do_loop(bucket);
        return Qnil;
    } else {
        if (ctx->nqueries > 0) {
            /* we have some operations pending */
            lcb_wait(bucket->handle);
        }
        exc = ctx->exception;
        rv = ctx->rv;
        cb_context_free(ctx);
        if (exc != Qnil) {
            rb_exc_raise(exc);
        }
        exc = bucket->exception;
        if (exc != Qnil) {
            bucket->exception = Qnil;
            rb_exc_raise(exc);
        }
        if (RTEST(obs)) {
            rv = rb_funcall(bucket->self, cb_id_observe_and_wait, 2, rv, obs);
        }
        if (params.cmd.store.num > 1) {
            return rv;  /* return as a hash {key => cas, ...} */
        } else {
            VALUE vv = Qnil;
            rb_hash_foreach(rv, cb_first_value_i, (VALUE)&vv);
            return vv;
        }
    }
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
 *   @option options [Hash] :observe Apply persistence condition before
 *     returning result. When this option specified the library will observe
 *     given condition. See {Bucket#observe_and_wait}.
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
 *   @raise [Couchbase::Error::Timeout] if timeout interval for observe
 *     exceeds
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
 *
 *   @example Ensure that the key will be persisted at least on the one node
 *     c.set("foo", "bar", :observe => {:persisted => 1})
 */
    VALUE
cb_bucket_set(int argc, VALUE *argv, VALUE self)
{
    return cb_bucket_store(LCB_SET, argc, argv, self);
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
 *   @option options [Hash] :observe Apply persistence condition before
 *     returning result. When this option specified the library will observe
 *     given condition. See {Bucket#observe_and_wait}.
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
 *   @raise [Couchbase::Error::Timeout] if timeout interval for observe
 *     exceeds
 *
 *   @example Add the same key twice
 *     c.add("foo", "bar")  #=> stored successully
 *     c.add("foo", "baz")  #=> will raise Couchbase::Error::KeyExists: failed to store value (key="foo", error=0x0c)
 *
 *   @example Ensure that the key will be persisted at least on the one node
 *     c.add("foo", "bar", :observe => {:persisted => 1})
 */
    VALUE
cb_bucket_add(int argc, VALUE *argv, VALUE self)
{
    return cb_bucket_store(LCB_ADD, argc, argv, self);
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
 *   @option options [Hash] :observe Apply persistence condition before
 *     returning result. When this option specified the library will observe
 *     given condition. See {Bucket#observe_and_wait}.
 *
 *   @return [Fixnum] The CAS value of the object.
 *
 *   @raise [Couchbase::Error::Connect] if connection closed (see {Bucket#reconnect})
 *   @raise [Couchbase::Error::NotFound] if the key doesn't exists
 *   @raise [Couchbase::Error::KeyExists] on CAS mismatch
 *   @raise [ArgumentError] when passing the block in synchronous mode
 *   @raise [Couchbase::Error::Timeout] if timeout interval for observe
 *     exceeds
 *
 *   @example Replacing missing key
 *     c.replace("foo", "baz")  #=> will raise Couchbase::Error::NotFound: failed to store value (key="foo", error=0x0d)
 *
 *   @example Ensure that the key will be persisted at least on the one node
 *     c.replace("foo", "bar", :observe => {:persisted => 1})
 */
    VALUE
cb_bucket_replace(int argc, VALUE *argv, VALUE self)
{
    return cb_bucket_store(LCB_REPLACE, argc, argv, self);
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
 *   @option options [Hash] :observe Apply persistence condition before
 *     returning result. When this option specified the library will observe
 *     given condition. See {Bucket#observe_and_wait}.
 *
 *   @return [Fixnum] The CAS value of the object.
 *
 *   @raise [Couchbase::Error::Connect] if connection closed (see {Bucket#reconnect})
 *   @raise [Couchbase::Error::KeyExists] on CAS mismatch
 *   @raise [Couchbase::Error::NotStored] if the key doesn't exist
 *   @raise [ArgumentError] when passing the block in synchronous mode
 *   @raise [Couchbase::Error::Timeout] if timeout interval for observe
 *     exceeds
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
 *
 *   @example Ensure that the key will be persisted at least on the one node
 *     c.append("foo", "bar", :observe => {:persisted => 1})
 */
    VALUE
cb_bucket_append(int argc, VALUE *argv, VALUE self)
{
    return cb_bucket_store(LCB_APPEND, argc, argv, self);
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
 *   @option options [Hash] :observe Apply persistence condition before
 *     returning result. When this option specified the library will observe
 *     given condition. See {Bucket#observe_and_wait}.
 *
 *   @raise [Couchbase::Error::Connect] if connection closed (see {Bucket#reconnect})
 *   @raise [Couchbase::Error::KeyExists] on CAS mismatch
 *   @raise [Couchbase::Error::NotStored] if the key doesn't exist
 *   @raise [ArgumentError] when passing the block in synchronous mode
 *   @raise [Couchbase::Error::Timeout] if timeout interval for observe
 *     exceeds
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
 *
 *   @example Ensure that the key will be persisted at least on the one node
 *     c.prepend("foo", "bar", :observe => {:persisted => 1})
 */
    VALUE
cb_bucket_prepend(int argc, VALUE *argv, VALUE self)
{
    return cb_bucket_store(LCB_PREPEND, argc, argv, self);
}

    VALUE
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


