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
cb_stat_callback(lcb_t handle, const void *cookie, lcb_error_t error, const lcb_server_stat_resp_t *resp)
{
    struct cb_context_st *ctx = (struct cb_context_st *)cookie;
    struct cb_bucket_st *bucket = ctx->bucket;
    VALUE stats, node, key, val, exc = Qnil, res;

    node = resp->v.v0.server_endpoint ? STR_NEW_CSTR(resp->v.v0.server_endpoint) : Qnil;
    exc = cb_check_error(error, "failed to fetch stats", node);
    if (exc != Qnil) {
        rb_ivar_set(exc, cb_id_iv_operation, cb_sym_stats);
        ctx->exception = exc;
    }
    if (node != Qnil) {
        key = STR_NEW((const char*)resp->v.v0.key, resp->v.v0.nkey);
        val = STR_NEW((const char*)resp->v.v0.bytes, resp->v.v0.nbytes);
        if (bucket->async) {    /* asynchronous */
            if (ctx->proc != Qnil) {
                res = rb_class_new_instance(0, NULL, cb_cResult);
                rb_ivar_set(res, cb_id_iv_error, exc);
                rb_ivar_set(res, cb_id_iv_operation, cb_sym_stats);
                rb_ivar_set(res, cb_id_iv_node, node);
                rb_ivar_set(res, cb_id_iv_key, key);
                rb_ivar_set(res, cb_id_iv_value, val);
                cb_proc_call(bucket, ctx->proc, 1, res);
            }
        } else {                /* synchronous */
            if (NIL_P(exc)) {
                stats = rb_hash_aref(ctx->rv, key);
                if (NIL_P(stats)) {
                    stats = rb_hash_new();
                    rb_hash_aset(ctx->rv, key, stats);
                }
                rb_hash_aset(stats, node, val);
            }
        }
    } else {
        ctx->proc = Qnil;
        if (bucket->async) {
            cb_context_free(ctx);
        }
    }
    (void)handle;
}

/*
 * Request server statistics.
 *
 * @since 1.0.0
 *
 * Fetches stats from each node in cluster. Without a key specified the
 * server will respond with a "default" set of statistical information. In
 * asynchronous mode each statistic is returned in separate call where the
 * Result object yielded (+#key+ contains the name of the statistical item
 * and the +#value+ contains the value, the +#node+ will indicate the server
 * address). In synchronous mode it returns the hash of stats keys and
 * node-value pairs as a value.
 *
 * @overload stats(arg = nil)
 *   @param [String] arg argument to STATS query
 *   @yieldparam [Result] ret the object with +node+, +key+ and +value+
 *     attributes.
 *
 *   @example Found how many items in the bucket
 *     total = 0
 *     c.stats["total_items"].each do |key, value|
 *       total += value.to_i
 *     end
 *
 *   @example Found total items number asynchronously
 *     total = 0
 *     c.run do
 *       c.stats do |ret|
 *         if ret.key == "total_items"
 *           total += ret.value.to_i
 *         end
 *       end
 *     end
 *
 *   @example Get memory stats (works on couchbase buckets)
 *     c.stats(:memory)   #=> {"mem_used"=>{...}, ...}
 *
 *   @return [Hash] where keys are stat keys, values are host-value pairs
 *
 *   @raise [Couchbase::Error::Connect] if connection closed (see {Bucket#reconnect})
 *   @raise [ArgumentError] when passing the block in synchronous mode
 */
    VALUE
cb_bucket_stats(int argc, VALUE *argv, VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    struct cb_context_st *ctx;
    VALUE rv, exc, proc;
    lcb_error_t err;
    struct cb_params_st params;

    if (!cb_bucket_connected_bang(bucket, cb_sym_stats)) {
        return Qnil;
    }

    memset(&params, 0, sizeof(struct cb_params_st));
    rb_scan_args(argc, argv, "0*&", &params.args, &proc);
    if (!bucket->async && proc != Qnil) {
        rb_raise(rb_eArgError, "synchronous mode doesn't support callbacks");
    }
    params.type = cb_cmd_stats;
    params.bucket = bucket;
    cb_params_build(&params);
    ctx = cb_context_alloc_common(bucket, proc, params.cmd.stats.num);
    err = lcb_server_stats(bucket->handle, (const void *)ctx,
            params.cmd.stats.num, params.cmd.stats.ptr);
    exc = cb_check_error(err, "failed to schedule stat request", Qnil);
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

    return Qnil;
}


