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

    void
cb_get_callback(lcb_t handle, const void *cookie, lcb_error_t error, const lcb_get_resp_t *resp)
{
    struct cb_context_st *ctx = (struct cb_context_st *)cookie;
    struct cb_bucket_st *bucket = ctx->bucket;
    VALUE key, val, flags, cas, exc = Qnil, res;

    ctx->nqueries--;
    key = STR_NEW((const char*)resp->v.v0.key, resp->v.v0.nkey);
    cb_strip_key_prefix(bucket, key);

    if (error != LCB_KEY_ENOENT || !ctx->quiet) {
        exc = cb_check_error(error, "failed to get value", key);
        if (exc != Qnil) {
            rb_ivar_set(exc, cb_id_iv_operation, cb_sym_get);
            ctx->exception = exc;
        }
    }

    flags = ULONG2NUM(resp->v.v0.flags);
    cas = ULL2NUM(resp->v.v0.cas);
    val = Qnil;
    if (resp->v.v0.nbytes != 0) {
        VALUE raw = STR_NEW((const char*)resp->v.v0.bytes, resp->v.v0.nbytes);
        val = cb_decode_value(raw, resp->v.v0.flags, ctx->force_format);
        if (rb_obj_is_kind_of(val, rb_eStandardError)) {
            VALUE exc_str = rb_funcall(val, cb_id_to_s, 0);
            VALUE msg = rb_funcall(rb_mKernel, cb_id_sprintf, 3,
                    rb_str_new2("unable to convert value for key '%s': %s"), key, exc_str);
            ctx->exception = rb_exc_new3(cb_eValueFormatError, msg);
            rb_ivar_set(ctx->exception, cb_id_iv_operation, cb_sym_get);
            rb_ivar_set(ctx->exception, cb_id_iv_key, key);
            rb_ivar_set(ctx->exception, cb_id_iv_inner_exception, val);
            val = raw;
        }
    } else if (cb_flags_get_format(resp->v.v0.flags) == cb_sym_plain) {
        val = cb_vStrEmpty;
    }
    if (bucket->async) { /* asynchronous */
        if (ctx->proc != Qnil) {
            res = rb_class_new_instance(0, NULL, cb_cResult);
            rb_ivar_set(res, cb_id_iv_error, exc);
            rb_ivar_set(res, cb_id_iv_operation, cb_sym_get);
            rb_ivar_set(res, cb_id_iv_key, key);
            rb_ivar_set(res, cb_id_iv_value, val);
            rb_ivar_set(res, cb_id_iv_flags, flags);
            rb_ivar_set(res, cb_id_iv_cas, cas);
            cb_proc_call(bucket, ctx->proc, 1, res);
        }
    } else {                /* synchronous */
        if (NIL_P(exc) && error != LCB_KEY_ENOENT) {
            if (ctx->extended) {
                rb_hash_aset(ctx->rv, key, rb_ary_new3(3, val, flags, cas));
            } else {
                rb_hash_aset(ctx->rv, key, val);
            }
        }
    }

    if (ctx->nqueries == 0) {
        ctx->proc = Qnil;
        if (bucket->async) {
            cb_context_free(ctx);
        }
    }
    (void)handle;
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
    VALUE
cb_bucket_get(int argc, VALUE *argv, VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    struct cb_context_st *ctx;
    VALUE rv, proc, exc;
    size_t ii;
    lcb_error_t err = LCB_SUCCESS;
    struct cb_params_st params;

    if (!cb_bucket_connected_bang(bucket, cb_sym_get)) {
        return Qnil;
    }

    memset(&params, 0, sizeof(struct cb_params_st));
    rb_scan_args(argc, argv, "0*&", &params.args, &proc);
    if (!bucket->async && proc != Qnil) {
        rb_raise(rb_eArgError, "synchronous mode doesn't support callbacks");
    }
    params.type = cb_cmd_get;
    params.bucket = bucket;
    params.cmd.get.keys_ary = rb_ary_new();
    cb_params_build(&params);
    ctx = cb_context_alloc_common(bucket, proc, params.cmd.get.num);
    ctx->extended = params.cmd.get.extended;
    ctx->quiet = params.cmd.get.quiet;
    ctx->force_format = params.cmd.get.forced_format;
    if (params.cmd.get.replica) {
        err = lcb_get_replica(bucket->handle, (const void *)ctx,
                params.cmd.get.num, params.cmd.get.ptr_gr);
    } else {
        err = lcb_get(bucket->handle, (const void *)ctx,
                params.cmd.get.num, params.cmd.get.ptr);
    }
    cb_params_destroy(&params);
    exc = cb_check_error(err, "failed to schedule get request", Qnil);
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
        if (params.cmd.get.gat || params.cmd.get.assemble_hash ||
                (params.cmd.get.extended && (params.cmd.get.num > 1 || params.cmd.get.array))) {
            return rv;  /* return as a hash {key => [value, flags, cas], ...} */
        }
        if (params.cmd.get.num > 1 || params.cmd.get.array) {
            VALUE *keys_ptr, ret;
            ret = rb_ary_new();
            keys_ptr = RARRAY_PTR(params.cmd.get.keys_ary);
            for (ii = 0; ii < params.cmd.get.num; ++ii) {
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


