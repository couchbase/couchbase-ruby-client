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
flush_callback(lcb_t handle, const void *cookie, lcb_error_t error, const lcb_flush_resp_t *resp)
{
    struct context_st *ctx = (struct context_st *)cookie;
    struct bucket_st *bucket = ctx->bucket;
    VALUE node, success = Qtrue, *rv = ctx->rv, exc, res;

    node = resp->v.v0.server_endpoint ? STR_NEW_CSTR(resp->v.v0.server_endpoint) : Qnil;
    exc = cb_check_error(error, "failed to flush bucket", node);
    if (exc != Qnil) {
        rb_ivar_set(exc, id_iv_operation, sym_flush);
        if (NIL_P(ctx->exception)) {
            ctx->exception = cb_gc_protect(bucket, exc);
        }
        success = Qfalse;
    }

    if (node != Qnil) {
        if (bucket->async) {    /* asynchronous */
            if (ctx->proc != Qnil) {
                res = rb_class_new_instance(0, NULL, cResult);
                rb_ivar_set(res, id_iv_error, exc);
                rb_ivar_set(res, id_iv_operation, sym_flush);
                rb_ivar_set(res, id_iv_node, node);
                cb_proc_call(ctx->proc, 1, res);
            }
        } else {                /* synchronous */
            if (RTEST(*rv)) {
                /* rewrite status for positive values only */
                *rv = success;
            }
        }
    } else {
        ctx->nqueries--;
        cb_gc_unprotect(bucket, ctx->proc);
    }

    (void)handle;
}

/*
 * Deletes all values from a server
 *
 * @since 1.0.0
 *
 * @overload flush
 *   @yieldparam [Result] ret the object with +error+, +node+ and +operation+
 *     attributes.
 *
 *   @return [true, false] +true+ on success
 *
 *   @raise [Couchbase::Error::Connect] if connection closed (see {Bucket#reconnect})
 *   @raise [ArgumentError] when passing the block in synchronous mode
 *
 *   @example Simple flush the bucket
 *     c.flush    #=> true
 *
 *   @example Asynchronous flush
 *     c.run do
 *       c.flush do |ret|
 *         ret.operation   #=> :flush
 *         ret.success?    #=> true
 *         ret.node        #=> "localhost:11211"
 *       end
 *     end
 */
    VALUE
cb_bucket_flush(int argc, VALUE *argv, VALUE self)
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
    params.type = cmd_flush;
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
    err = lcb_flush(bucket->handle, (const void *)ctx,
            params.cmd.flush.num, params.cmd.flush.ptr);
    exc = cb_check_error(err, "failed to schedule flush request", Qnil);
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


