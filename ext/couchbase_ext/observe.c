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
cb_observe_callback(lcb_t instance, int cbtype, const lcb_RESPBASE *rb)
{
    const lcb_RESPOBSERVE *resp = (const lcb_RESPOBSERVE *)rb;
    struct cb_context_st *ctx = (struct cb_context_st *)rb->cookie;
    VALUE res, key;

    if (rb->nkey == 0) {
        return;
    }

    res = rb_class_new_instance(0, NULL, cb_cResult);
    key = rb_external_str_new(rb->key, rb->nkey);
    rb_ivar_set(res, cb_id_iv_key, key);
    rb_ivar_set(res, cb_id_iv_operation, cb_sym_observe);
    if (resp->rc == LCB_SUCCESS) {
        rb_ivar_set(res, cb_id_iv_cas, ULL2NUM(resp->cas));
        rb_ivar_set(res, cb_id_iv_from_master, resp->ismaster ? Qtrue : Qfalse);
        switch (resp->status) {
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
    } else {
        rb_ivar_set(res, cb_id_iv_error,
                    cb_exc_new(cb_eLibraryError, rb->rc, "failed to observe key: %.*s", (int)resp->nkey,
                               (const char *)resp->key));
    }
    switch (TYPE(ctx->rv)) {
    case T_ARRAY:
        rb_ary_push(ctx->rv, res);
        break;
    case T_HASH: {
        VALUE stats = rb_hash_aref(ctx->rv, key);
        if (NIL_P(stats)) {
            stats = rb_ary_new();
            rb_hash_aset(ctx->rv, key, stats);
        }
        rb_ary_push(stats, res);
    } break;
    default:
        // FIXME: use some kind of invalid state error
        cb_raise_msg(cb_eLibraryError, "unexpected result container type: %d", (int)TYPE(ctx->rv));
        break;
    }
    (void)instance;
    (void)cbtype;
}

/*
 * Observe key state
 *
 * @since 1.2.0.dp6
 *
 * @overload observe(keys, options = {})
 *   @param keys [String, Symbol, Array] One or several keys to fetch
 *   @param options [Hash] Options for operation.
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
    lcb_error_t err;
    lcb_MULTICMD_CTX *mctx = NULL;
    lcb_CMDOBSERVE cmd = {0};
    VALUE arg, rv;
    int ii;

    if (!cb_bucket_connected_bang(bucket, cb_sym_observe)) {
        return Qnil;
    }

    rb_scan_args(argc, argv, "1", &arg);

    ctx = cb_context_alloc(bucket);
    mctx = lcb_observe3_ctxnew(bucket->handle);
    switch (TYPE(arg)) {
    case T_ARRAY:
        for (ii = 0; ii < RARRAY_LEN(arg); ii++) {
            VALUE entry = rb_ary_entry(arg, ii);
            switch (TYPE(entry)) {
            case T_SYMBOL:
                arg = rb_sym2str(arg);
                /* fallthrough */
            case T_STRING:
                LCB_CMD_SET_KEY(&cmd, RSTRING_PTR(entry), RSTRING_LEN(entry));
                err = mctx->addcmd(mctx, (const lcb_CMDBASE *)&cmd);
                if (err != LCB_SUCCESS) {
                    mctx->fail(mctx);
                    cb_context_free(ctx);
                    cb_raise2(cb_eLibraryError, err, "unable to add key to observe context");
                }
                break;
            default:
                mctx->fail(mctx);
                cb_context_free(ctx);
                cb_raise_msg(rb_eArgError, "expected array or strings or symbols (type=%d)", (int)TYPE(entry));
                break;
            }
        }
        ctx->rv = rb_hash_new();
        break;
    case T_SYMBOL:
        arg = rb_sym2str(arg);
        /* fallthrough */
    case T_STRING:
        LCB_CMD_SET_KEY(&cmd, RSTRING_PTR(arg), RSTRING_LEN(arg));
        mctx->addcmd(mctx, (const lcb_CMDBASE *)&cmd);
        ctx->rv = rb_ary_new();
        break;
    default:
        mctx->fail(mctx);
        cb_context_free(ctx);
        cb_raise_msg(rb_eArgError, "expected array of keys or single key (type=%d)", (int)TYPE(arg));
        break;
    }
    err = mctx->done(mctx, ctx);
    if (err != LCB_SUCCESS) {
        mctx->fail(mctx);
        cb_context_free(ctx);
        cb_raise2(cb_eLibraryError, err, "unable to schedule observe request");
    }
    lcb_wait(bucket->handle);
    rv = ctx->rv;
    cb_context_free(ctx);
    return rv;
}
