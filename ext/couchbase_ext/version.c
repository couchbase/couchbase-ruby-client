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
cb_version_callback(lcb_t handle, const void *cookie, lcb_error_t error, const lcb_server_version_resp_t *resp)
{
    struct cb_context_st *ctx = (struct cb_context_st *)cookie;
    struct cb_bucket_st *bucket = ctx->bucket;
    VALUE node, val, exc, res;

    node = resp->v.v0.server_endpoint ? STR_NEW_CSTR(resp->v.v0.server_endpoint) : Qnil;
    exc = cb_check_error(error, "failed to get version", node);
    if (exc != Qnil) {
        rb_ivar_set(exc, cb_id_iv_operation, cb_sym_version);
        ctx->exception = exc;
    }

    if (node != Qnil) {
        val = STR_NEW((const char*)resp->v.v0.vstring, resp->v.v0.nvstring);
        if (bucket->async) {    /* asynchronous */
            if (ctx->proc != Qnil) {
                res = rb_class_new_instance(0, NULL, cb_cResult);
                rb_ivar_set(res, cb_id_iv_error, exc);
                rb_ivar_set(res, cb_id_iv_operation, cb_sym_version);
                rb_ivar_set(res, cb_id_iv_node, node);
                rb_ivar_set(res, cb_id_iv_value, val);
                cb_proc_call(bucket, ctx->proc, 1, res);
            }
        } else {                /* synchronous */
            if (NIL_P(exc)) {
                rb_hash_aset(ctx->rv, node, val);
            }
        }
    } else {
        ctx->nqueries--;
        ctx->proc = Qnil;
        if (bucket->async) {
            cb_context_free(ctx);
        }
    }

    (void)handle;
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
    VALUE
cb_bucket_version(int argc, VALUE *argv, VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    struct cb_context_st *ctx;
    VALUE rv, exc, proc;
    lcb_error_t err;
    struct cb_params_st params;

    if (!cb_bucket_connected_bang(bucket, cb_sym_version)) {
        return Qnil;
    }

    memset(&params, 0, sizeof(struct cb_params_st));
    rb_scan_args(argc, argv, "0*&", &params.args, &proc);
    if (!bucket->async && proc != Qnil) {
        rb_raise(rb_eArgError, "synchronous mode doesn't support callbacks");
    }
    params.type = cb_cmd_version;
    params.bucket = bucket;
    cb_params_build(&params);
    ctx = cb_context_alloc_common(bucket, proc, params.cmd.version.num);
    err = lcb_server_versions(bucket->handle, (const void *)ctx,
            params.cmd.version.num, params.cmd.version.ptr);
    exc = cb_check_error(err, "failed to schedule version request", Qnil);
    cb_params_destroy(&params);
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
        return rv;
    }
}


