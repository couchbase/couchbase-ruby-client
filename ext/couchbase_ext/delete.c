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
cb_delete_callback(lcb_t handle, const void *cookie, lcb_error_t error, const lcb_remove_resp_t *resp)
{
    struct cb_context_st *ctx = (struct cb_context_st *)cookie;
    struct cb_bucket_st *bucket = ctx->bucket;
    VALUE key, exc = Qnil, res;

    ctx->nqueries--;
    key = STR_NEW((const char*)resp->v.v0.key, resp->v.v0.nkey);
    cb_strip_key_prefix(bucket, key);

    if (error != LCB_KEY_ENOENT || !ctx->quiet) {
        exc = cb_check_error(error, "failed to remove value", key);
        if (exc != Qnil) {
            rb_ivar_set(exc, cb_id_iv_operation, cb_sym_delete);
            ctx->exception = exc;
        }
    }
    if (bucket->async) {    /* asynchronous */
        if (ctx->proc != Qnil) {
            res = rb_class_new_instance(0, NULL, cb_cResult);
            rb_ivar_set(res, cb_id_iv_error, exc);
            rb_ivar_set(res, cb_id_iv_operation, cb_sym_delete);
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
 *   @return [true, false, Hash<String, Boolean>] the result of the
 *     operation
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
    VALUE
cb_bucket_delete(int argc, VALUE *argv, VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    struct cb_context_st *ctx;
    VALUE rv, exc;
    VALUE proc;
    lcb_error_t err;
    struct cb_params_st params;

    if (!cb_bucket_connected_bang(bucket, cb_sym_delete)) {
        return Qnil;
    }

    memset(&params, 0, sizeof(struct cb_params_st));
    rb_scan_args(argc, argv, "0*&", &params.args, &proc);
    if (!bucket->async && proc != Qnil) {
        rb_raise(rb_eArgError, "synchronous mode doesn't support callbacks");
    }
    rb_funcall(params.args, cb_id_flatten_bang, 0);
    params.type = cb_cmd_remove;
    params.bucket = bucket;
    cb_params_build(&params);

    ctx = cb_context_alloc_common(bucket, proc, params.cmd.remove.num);
    ctx->quiet = params.cmd.remove.quiet;
    err = lcb_remove(bucket->handle, (const void *)ctx,
            params.cmd.remove.num, params.cmd.remove.ptr);
    cb_params_destroy(&params);
    exc = cb_check_error(err, "failed to schedule delete request", Qnil);
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
        if (params.cmd.remove.num > 1) {
            return rv;  /* return as a hash {key => true, ...} */
        } else {
            VALUE vv = Qnil;
            rb_hash_foreach(rv, cb_first_value_i, (VALUE)&vv);
            return vv;
        }
        return rv;
    }
}
