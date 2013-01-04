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
cb_touch_callback(lcb_t handle, const void *cookie, lcb_error_t error, const lcb_touch_resp_t *resp)
{
    struct cb_context_st *ctx = (struct cb_context_st *)cookie;
    struct cb_bucket_st *bucket = ctx->bucket;
    VALUE key, exc = Qnil, res;

    ctx->nqueries--;
    key = STR_NEW((const char*)resp->v.v0.key, resp->v.v0.nkey);
    cb_strip_key_prefix(bucket, key);

    if (error != LCB_KEY_ENOENT || !ctx->quiet) {
        exc = cb_check_error(error, "failed to touch value", key);
        if (exc != Qnil) {
            rb_ivar_set(exc, cb_id_iv_operation, cb_sym_touch);
            ctx->exception = exc;
        }
    }

    if (bucket->async) {    /* asynchronous */
        if (ctx->proc != Qnil) {
            res = rb_class_new_instance(0, NULL, cb_cResult);
            rb_ivar_set(res, cb_id_iv_error, exc);
            rb_ivar_set(res, cb_id_iv_operation, cb_sym_touch);
            rb_ivar_set(res, cb_id_iv_key, key);
            cb_proc_call(bucket, ctx->proc, 1, res);
        }
    } else {                /* synchronous */
        rb_hash_aset(ctx->rv, key, (error == LCB_SUCCESS) ? Qtrue : Qfalse);
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
 *   @option options [true, false] :quiet (self.quiet) If set to +true+, the
 *     operation won't raise error for missing key, it will return +nil+.
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
   VALUE
cb_bucket_touch(int argc, VALUE *argv, VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    struct cb_context_st *ctx;
    VALUE rv, proc, exc;
    lcb_error_t err;
    struct cb_params_st params;

    if (!cb_bucket_connected_bang(bucket, cb_sym_touch)) {
        return Qnil;
    }

    memset(&params, 0, sizeof(struct cb_params_st));
    rb_scan_args(argc, argv, "0*&", &params.args, &proc);
    if (!bucket->async && proc != Qnil) {
        rb_raise(rb_eArgError, "synchronous mode doesn't support callbacks");
    }
    rb_funcall(params.args, cb_id_flatten_bang, 0);
    params.type = cb_cmd_touch;
    params.bucket = bucket;
    cb_params_build(&params);
    ctx = cb_context_alloc_common(bucket, proc, params.cmd.touch.num);
    ctx->quiet = params.cmd.touch.quiet;
    err = lcb_touch(bucket->handle, (const void *)ctx,
            params.cmd.touch.num, params.cmd.touch.ptr);
    cb_params_destroy(&params);
    exc = cb_check_error(err, "failed to schedule touch request", Qnil);
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
        if (params.cmd.touch.num > 1) {
            return rv;  /* return as a hash {key => true, ...} */
        } else {
            VALUE vv = Qnil;
            rb_hash_foreach(rv, cb_first_value_i, (VALUE)&vv);
            return vv;
        }
    }
}


