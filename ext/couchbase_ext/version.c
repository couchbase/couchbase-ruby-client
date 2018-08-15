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
cb_version_callback(lcb_t handle, int cbtype, const lcb_RESPBASE *rb)
{
    VALUE res;
    struct cb_context_st *ctx = (struct cb_context_st *)rb->cookie;
    lcb_RESPMCVERSION *resp = (lcb_RESPMCVERSION *)rb;

    if (resp->server == NULL) {
        return;
    }

    res = rb_class_new_instance(0, NULL, cb_cResult);
    rb_ivar_set(res, cb_id_iv_node, rb_external_str_new_cstr(resp->server));
    rb_ivar_set(res, cb_id_iv_operation, cb_sym_version);
    if (rb->rc == LCB_SUCCESS) {
        rb_ivar_set(res, cb_id_iv_value, rb_external_str_new(resp->mcversion, resp->nversion));
    } else {
        VALUE exc = cb_exc_new(cb_eLibraryError, rb->rc, "failed to fetch version for node: %s", resp->server);
        rb_ivar_set(exc, cb_id_iv_operation, cb_sym_version);
        rb_ivar_set(res, cb_id_iv_error, exc);
    }
    if (TYPE(ctx->rv) != T_ARRAY) {
        cb_context_free(ctx);
        cb_raise_msg(cb_eLibraryError, "unexpected result container type: %d", (int)TYPE(ctx->rv));
    }
    rb_ary_push(ctx->rv, res);
    (void)handle;
    (void)cbtype;
}

/*
 * Returns versions of the server for each node in the cluster
 *
 * @since 1.1.0
 *
 * @overload version
 *   @return [Array] nodes version information
 *
 *   @raise [Couchbase::Error::Connect] if connection closed (see {Bucket#reconnect})
 *   @raise [ArgumentError] when passing the block in synchronous mode
 *
 *   @example Synchronous version request
 *     c.version            #=> will render version
 *
 */
VALUE
cb_bucket_version(VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    struct cb_context_st *ctx;
    VALUE rv;
    lcb_error_t err;
    lcb_CMDBASE cmd = {0};

    if (!cb_bucket_connected_bang(bucket, cb_sym_version)) {
        return Qnil;
    }

    ctx = cb_context_alloc(bucket);
    ctx->rv = rb_ary_new();
    err = lcb_server_versions3(bucket->handle, (const void *)ctx, &cmd);
    if (err != LCB_SUCCESS) {
        cb_context_free(ctx);
        cb_raise2(cb_eLibraryError, err, "unable to schedule versions request");
    }
    lcb_wait(bucket->handle);
    rv = ctx->rv;
    cb_context_free(ctx);
    return rv;
}
