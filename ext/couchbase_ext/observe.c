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
cb_observe_callback(lcb_t handle, const void *cookie, lcb_error_t error, const lcb_observe_resp_t *resp)
{
    struct cb_context_st *ctx = (struct cb_context_st *)cookie;
    struct cb_bucket_st *bucket = ctx->bucket;
    VALUE key, res, exc;

    if (resp->v.v0.key) {
        key = STR_NEW((const char*)resp->v.v0.key, resp->v.v0.nkey);
        exc = cb_check_error(error, "failed to execute observe request", key);
        if (exc != Qnil) {
            ctx->exception = exc;
        }
        res = rb_class_new_instance(0, NULL, cb_cResult);
        rb_ivar_set(res, cb_id_iv_completed, Qfalse);
        rb_ivar_set(res, cb_id_iv_error, ctx->exception);
        rb_ivar_set(res, cb_id_iv_operation, cb_sym_observe);
        rb_ivar_set(res, cb_id_iv_key, key);
        rb_ivar_set(res, cb_id_iv_cas, ULL2NUM(resp->v.v0.cas));
        rb_ivar_set(res, cb_id_iv_from_master, resp->v.v0.from_master ? Qtrue : Qfalse);
        rb_ivar_set(res, cb_id_iv_time_to_persist, ULONG2NUM(resp->v.v0.ttp));
        rb_ivar_set(res, cb_id_iv_time_to_replicate, ULONG2NUM(resp->v.v0.ttr));
        switch (resp->v.v0.status) {
            case LCB_OBSERVE_FOUND:
                rb_ivar_set(res, cb_id_iv_status, cb_sym_found);
                break;
            case LCB_OBSERVE_PERSISTED:
                rb_ivar_set(res, cb_id_iv_status, cb_sym_persisted);
                break;
            case LCB_OBSERVE_NOT_FOUND:
                rb_ivar_set(res, cb_id_iv_status, cb_sym_not_found);
                break;
            default:
                rb_ivar_set(res, cb_id_iv_status, Qnil);
        }
        if (bucket->async) { /* asynchronous */
            if (ctx->proc != Qnil) {
                cb_proc_call(bucket, ctx->proc, 1, res);
            }
        } else {             /* synchronous */
            if (NIL_P(ctx->exception)) {
                VALUE stats = rb_hash_aref(ctx->rv, key);
                if (NIL_P(stats)) {
                    stats = rb_ary_new();
                    rb_hash_aset(ctx->rv, key, stats);
                }
                rb_ary_push(stats, res);
            }
        }
    } else {
        if (bucket->async && ctx->proc != Qnil) {
            res = rb_class_new_instance(0, NULL, cb_cResult);
            rb_ivar_set(res, cb_id_iv_completed, Qtrue);
            cb_proc_call(bucket, ctx->proc, 1, res);
        }
        ctx->nqueries--;
        ctx->proc = Qnil;
        if (bucket->async) {
            cb_context_free(ctx);
        }
    }
    (void)handle;
}

/*
 * Observe key state
 *
 * @since 1.2.0.dp6
 *
 * @overload observe(*keys, options = {})
 *   @param keys [String, Symbol, Array] One or several keys to fetch
 *   @param options [Hash] Options for operation.
 *
 *   @yieldparam ret [Result] the result of operation in asynchronous mode
 *     (valid attributes: +error+, +status+, +operation+, +key+, +cas+,
 *     +from_master+, +time_to_persist+, +time_to_replicate+).
 *
 *   @return [Hash<String, Array<Result>>, Array<Result>] the state of the
 *     keys on all nodes. If the +keys+ argument was String or Symbol, this
 *     method will return just array of results (result per each node),
 *     otherwise it will return hash map.
 *
 *   @example Observe single key
 *     c.observe("foo")
 *     #=> [#<Couchbase::Result:0x00000001650df0 ...>, ...]
 *
 *   @example Observe multiple keys
 *     keys = ["foo", "bar"]
 *     stats = c.observe(keys)
 *     stats.size   #=> 2
 *     stats["foo"] #=> [#<Couchbase::Result:0x00000001650df0 ...>, ...]
 */

    VALUE
cb_bucket_observe(int argc, VALUE *argv, VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    struct cb_context_st *ctx;
    VALUE rv, proc, exc;
    lcb_error_t err;
    struct cb_params_st params;

    if (!cb_bucket_connected_bang(bucket, cb_sym_observe)) {
        return Qnil;
    }

    memset(&params, 0, sizeof(struct cb_params_st));
    rb_scan_args(argc, argv, "0*&", &params.args, &proc);
    if (!bucket->async && proc != Qnil) {
        rb_raise(rb_eArgError, "synchronous mode doesn't support callbacks");
    }
    params.type = cb_cmd_observe;
    params.bucket = bucket;
    cb_params_build(&params);
    ctx = cb_context_alloc_common(bucket, proc, params.cmd.observe.num);
    err = lcb_observe(bucket->handle, (const void *)ctx,
            params.cmd.observe.num, params.cmd.observe.ptr);
    cb_params_destroy(&params);
    exc = cb_check_error(err, "failed to schedule observe request", Qnil);
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
        if (params.cmd.observe.num > 1 || params.cmd.observe.array) {
            return rv;  /* return as a hash {key => {}, ...} */
        } else {
            VALUE vv = Qnil;
            rb_hash_foreach(rv, cb_first_value_i, (VALUE)&vv);
            return vv;  /* return first value */
        }
    }
}
