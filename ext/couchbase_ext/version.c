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
version_callback(lcb_t handle, const void *cookie, lcb_error_t error, const lcb_server_version_resp_t *resp)
{
    struct context_st *ctx = (struct context_st *)cookie;
    struct bucket_st *bucket = ctx->bucket;
    VALUE node, val, *rv = ctx->rv, exc, res;

    node = resp->v.v0.server_endpoint ? STR_NEW_CSTR(resp->v.v0.server_endpoint) : Qnil;
    exc = cb_check_error(error, "failed to get version", node);
    if (exc != Qnil) {
        rb_ivar_set(exc, id_iv_operation, sym_flush);
        if (NIL_P(ctx->exception)) {
            ctx->exception = cb_gc_protect(bucket, exc);
        }
    }

    if (node != Qnil) {
        val = STR_NEW((const char*)resp->v.v0.vstring, resp->v.v0.nvstring);
        if (bucket->async) {    /* asynchronous */
            if (ctx->proc != Qnil) {
                res = rb_class_new_instance(0, NULL, cResult);
                rb_ivar_set(res, id_iv_error, exc);
                rb_ivar_set(res, id_iv_operation, sym_version);
                rb_ivar_set(res, id_iv_node, node);
                rb_ivar_set(res, id_iv_value, val);
                cb_proc_call(ctx->proc, 1, res);
            }
        } else {                /* synchronous */
            if (NIL_P(exc)) {
                rb_hash_aset(*rv, node, val);
            }
        }
    } else {
        ctx->nqueries--;
        cb_gc_unprotect(bucket, ctx->proc);
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
    struct bucket_st *bucket = DATA_PTR(self);
    struct context_st *ctx;
    VALUE rv, exc, args, proc;
    lcb_error_t err;
    struct params_st params;

    if (bucket->handle == NULL) {
        rb_raise(eConnectError, "closed connection");
    }
    rb_scan_args(argc, argv, "0*&", &args, &proc);
    if (!bucket->async && proc != Qnil) {
        rb_raise(rb_eArgError, "synchronous mode doesn't support callbacks");
    }
    memset(&params, 0, sizeof(struct params_st));
    params.type = cmd_version;
    params.bucket = bucket;
    cb_params_build(&params, RARRAY_LEN(args), args);
    ctx = xcalloc(1, sizeof(struct context_st));
    if (ctx == NULL) {
        rb_raise(eClientNoMemoryError, "failed to allocate memory for context");
    }
    rv = rb_hash_new();
    ctx->rv = &rv;
    ctx->bucket = bucket;
    ctx->exception = Qnil;
    ctx->proc = cb_gc_protect(bucket, proc);
    ctx->nqueries = params.cmd.flush.num;
    err = lcb_server_versions(bucket->handle, (const void *)ctx,
            params.cmd.version.num, params.cmd.version.ptr);
    exc = cb_check_error(err, "failed to schedule version request", Qnil);
    cb_params_destroy(&params);
    if (exc != Qnil) {
        xfree(ctx);
        rb_exc_raise(exc);
    }
    bucket->nbytes += params.npayload;
    if (bucket->async) {
        maybe_do_loop(bucket);
        return Qnil;
    } else {
        if (ctx->nqueries > 0) {
            /* we have some operations pending */
            lcb_wait(bucket->handle);
        }
        exc = ctx->exception;
        xfree(ctx);
        if (exc != Qnil) {
            cb_gc_unprotect(bucket, exc);
            rb_exc_raise(exc);
        }
        if (bucket->exception != Qnil) {
            rb_exc_raise(bucket->exception);
        }
        return rv;
    }
}


