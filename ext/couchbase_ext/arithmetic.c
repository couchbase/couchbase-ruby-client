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
cb_arithmetic_callback(lcb_t handle, const void *cookie, lcb_error_t error, const lcb_arithmetic_resp_t *resp)
{
    struct cb_context_st *ctx = (struct cb_context_st *)cookie;
    struct cb_bucket_st *bucket = ctx->bucket;
    VALUE cas, key, val, exc, res;
    ID o;

    ctx->nqueries--;
    key = STR_NEW((const char*)resp->v.v0.key, resp->v.v0.nkey);
    cb_strip_key_prefix(bucket, key);

    cas = resp->v.v0.cas > 0 ? ULL2NUM(resp->v.v0.cas) : Qnil;
    o = ctx->arith > 0 ? cb_sym_increment : cb_sym_decrement;
    exc = cb_check_error(error, "failed to perform arithmetic operation", key);
    if (exc != Qnil) {
        rb_ivar_set(exc, cb_id_iv_cas, cas);
        rb_ivar_set(exc, cb_id_iv_operation, o);
        ctx->exception = exc;
    }
    val = ULL2NUM(resp->v.v0.value);
    if (bucket->async) {    /* asynchronous */
        if (ctx->proc != Qnil) {
            res = rb_class_new_instance(0, NULL, cb_cResult);
            rb_ivar_set(res, cb_id_iv_error, exc);
            rb_ivar_set(res, cb_id_iv_operation, o);
            rb_ivar_set(res, cb_id_iv_key, key);
            rb_ivar_set(res, cb_id_iv_value, val);
            rb_ivar_set(res, cb_id_iv_cas, cas);
            cb_proc_call(bucket, ctx->proc, 1, res);
        }
    } else {                /* synchronous */
        if (NIL_P(exc)) {
            if (ctx->extended) {
                rb_hash_aset(ctx->rv, key, rb_ary_new3(2, val, cas));
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

    static inline VALUE
cb_bucket_arithmetic(int sign, int argc, VALUE *argv, VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    struct cb_context_st *ctx;
    VALUE rv, proc, exc;
    lcb_error_t err;
    struct cb_params_st params;

    if (!cb_bucket_connected_bang(bucket, sign > 0 ? cb_sym_increment : cb_sym_decrement)) {
        return Qnil;
    }

    memset(&params, 0, sizeof(struct cb_params_st));
    rb_scan_args(argc, argv, "0*&", &params.args, &proc);
    if (!bucket->async && proc != Qnil) {
        rb_raise(rb_eArgError, "synchronous mode doesn't support callbacks");
    }
    params.type = cb_cmd_arith;
    params.bucket = bucket;
    params.cmd.arith.sign = sign;
    cb_params_build(&params);
    ctx = cb_context_alloc_common(bucket, proc, params.cmd.arith.num);
    err = lcb_arithmetic(bucket->handle, (const void *)ctx,
            params.cmd.arith.num, params.cmd.arith.ptr);
    cb_params_destroy(&params);
    exc = cb_check_error(err, "failed to schedule arithmetic request", Qnil);
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
        if (params.cmd.store.num > 1) {
            return rv;  /* return as a hash {key => cas, ...} */
        } else {
            VALUE vv = Qnil;
            rb_hash_foreach(rv, cb_first_value_i, (VALUE)&vv);
            return vv;
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
    VALUE
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
    VALUE
cb_bucket_decr(int argc, VALUE *argv, VALUE self)
{
    return cb_bucket_arithmetic(-1, argc, argv, self);
}


