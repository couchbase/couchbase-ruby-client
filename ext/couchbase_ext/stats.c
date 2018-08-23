/* vim: ft=c et ts=8 sts=4 sw=4 cino=
 *
 *   Copyright 2011-2018 Couchbase, Inc.
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
cb_stat_callback(lcb_t handle, int cbtype, const lcb_RESPBASE *rb)
{
    VALUE res;
    struct cb_context_st *ctx = (struct cb_context_st *)rb->cookie;
    lcb_RESPSTATS *resp = (lcb_RESPSTATS *)rb;

    if (resp->server == NULL) {
        return;
    }
    res = rb_class_new_instance(0, NULL, cb_cResult);
    rb_ivar_set(res, cb_id_iv_key, rb_external_str_new(resp->key, resp->nkey));
    rb_ivar_set(res, cb_id_iv_node, rb_external_str_new_cstr(resp->server));
    rb_ivar_set(res, cb_id_iv_operation, cb_sym_stats);
    if (rb->rc == LCB_SUCCESS) {
        rb_ivar_set(res, cb_id_iv_value, rb_external_str_new(resp->value, resp->nvalue));
    } else {
        VALUE exc = cb_exc_new(cb_eLibraryError, rb->rc, "failed to fetch stats for node: %s", resp->server);
        rb_ivar_set(exc, cb_id_iv_operation, cb_sym_stats);
        rb_ivar_set(res, cb_id_iv_error, exc);
    }
    Check_Type(ctx->rv, T_ARRAY);
    rb_ary_push(ctx->rv, res);
    (void)handle;
    (void)cbtype;
}

/*
 * Request server statistics.
 *
 * @since 1.0.0
 *
 * Fetches stats from each node in cluster. Without a key specified the
 * server will respond with a "default" set of statistical information.
 * In synchronous mode it returns the hash of stats keys and node-value
 * pairs as a value.
 *
 * @overload stats(arg = nil)
 *   @param [String] arg argument to STATS query
 *
 *   @example Found how many operations has been performed in the bucket
 *     c.stats
 *      .select { |res| res.key == "cmd_total_ops" }
 *      .reduce(0) { |sum, res| sum += res.value }
 *
 *   @example Get memory stats (works on couchbase buckets)
 *     c.stats(:memory)   #=> {"mem_used"=>{...}, ...}
 *
 *   @return [Array<Result>] where keys are stat keys, values are host-value pairs
 *
 *   @raise [Couchbase::Error::Connect] if connection closed (see {Bucket#reconnect})
 *   @raise [ArgumentError] when passing the block in synchronous mode
 */
VALUE
cb_bucket_stats(int argc, VALUE *argv, VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    struct cb_context_st *ctx;
    VALUE rv, arg = Qnil;
    lcb_error_t err;
    lcb_CMDSTATS cmd = {0};

    if (!cb_bucket_connected_bang(bucket, cb_sym_stats)) {
        return Qnil;
    }

    rb_scan_args(argc, argv, "01", &arg);
    ctx = cb_context_alloc(bucket);
    ctx->rv = rb_ary_new();

    if (arg != Qnil) {
        if (TYPE(arg) == T_SYMBOL) {
            arg = rb_sym2str(arg);
        } else {
            Check_Type(arg, T_STRING);
        }
        LCB_CMD_SET_KEY(&cmd, RSTRING_PTR(arg), RSTRING_LEN(arg));
    }
    err = lcb_stats3(bucket->handle, (const void *)ctx, &cmd);
    if (err != LCB_SUCCESS) {
        cb_context_free(ctx);
        cb_raise2(cb_eLibraryError, err, "unable to schedule versions request");
    }
    lcb_wait(bucket->handle);
    rv = ctx->rv;
    cb_context_free(ctx);
    return rv;
}
