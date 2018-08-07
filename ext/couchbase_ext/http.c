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
cb_http_callback(lcb_t instance, int cbtype, const lcb_RESPBASE *r)
{
    const lcb_RESPHTTP *resp = (const lcb_RESPHTTP *)r;
    struct cb_context_st *ctx = (struct cb_context_st *)resp->cookie;
    VALUE chunk = Qnil;

    if (resp->nbody) {
        chunk = rb_str_new(resp->body, resp->nbody);
    }
    if (resp->headers && ctx->headers_val == Qnil) {
        ctx->headers_val = rb_ary_new();
        for (const char *const *cur = resp->headers; *cur && *(cur + 1); cur += 2) {
            VALUE pair = rb_ary_new();
            rb_ary_push(pair, rb_str_new_cstr(cur[0]));
            rb_ary_push(pair, rb_str_new_cstr(cur[1]));
            rb_ary_push(ctx->headers_val, pair);
        }
    }
    if (NIL_P(ctx->proc)) {
        if (resp->nbody) {
            rb_ary_push(rb_hash_aref(ctx->rv, cb_sym_chunks), chunk);
        }
        if (ctx->exception == Qnil) {
            ctx->exception =
                cb_check_error_with_status(resp->rc, "failed to execute HTTP request", Qnil, resp->htstatus);
        }
        if (resp->rflags & LCB_RESP_F_FINAL) {
            rb_hash_aset(ctx->rv, cb_sym_headers, ctx->headers_val);
            rb_hash_aset(ctx->rv, cb_sym_status, INT2FIX(resp->htstatus));
        }
    } else {
        rb_funcall(ctx->proc, cb_id_call, 4, chunk, INT2FIX(resp->rc), INT2FIX(resp->htstatus), ctx->headers_val);
    }
    (void)instance;
    (void)cbtype;
}

VALUE
cb_bucket___http_query(VALUE self, VALUE type, VALUE method, VALUE path, VALUE body, VALUE content_type, VALUE username,
                       VALUE password, VALUE hostname)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    struct cb_context_st *ctx;
    lcb_CMDHTTP cmd = {0};
    lcb_error_t rc;
    VALUE exc, rv;

    cmd.cmdflags = LCB_CMDHTTP_F_STREAM;
    if (type == cb_sym_view) {
        cmd.type = LCB_HTTP_TYPE_VIEW;
    } else if (type == cb_sym_management) {
        cmd.type = LCB_HTTP_TYPE_MANAGEMENT;
    } else if (type == cb_sym_raw) {
        cmd.type = LCB_HTTP_TYPE_RAW;
    } else if (type == cb_sym_n1ql) {
        cmd.type = LCB_HTTP_TYPE_N1QL;
    } else if (type == cb_sym_fts) {
        cmd.type = LCB_HTTP_TYPE_FTS;
    } else if (type == cb_sym_cbas) {
        cmd.type = LCB_HTTP_TYPE_CBAS;
    } else {
        rb_raise(rb_eArgError, "unsupported request type");
    }

    if (method == cb_sym_get) {
        cmd.method = LCB_HTTP_METHOD_GET;
    } else if (method == cb_sym_post) {
        cmd.method = LCB_HTTP_METHOD_POST;
    } else if (method == cb_sym_put) {
        cmd.method = LCB_HTTP_METHOD_PUT;
    } else if (method == cb_sym_delete) {
        cmd.method = LCB_HTTP_METHOD_DELETE;
    } else {
        rb_raise(rb_eArgError, "unsupported HTTP method");
    }

    if (content_type != Qnil) {
        Check_Type(content_type, T_STRING);
        cmd.content_type = RSTRING_PTR(content_type);
    }
    if (username != Qnil) {
        Check_Type(username, T_STRING);
        cmd.username = RSTRING_PTR(username);
    }
    if (password != Qnil) {
        Check_Type(password, T_STRING);
        cmd.password = RSTRING_PTR(password);
    }
    if (hostname != Qnil) {
        Check_Type(hostname, T_STRING);
        cmd.host = RSTRING_PTR(hostname);
    }
    if (path != Qnil) {
        Check_Type(path, T_STRING);
        LCB_CMD_SET_KEY(&cmd, RSTRING_PTR(path), RSTRING_LEN(path))
    }
    if (body != Qnil) {
        Check_Type(body, T_STRING);
        cmd.body = RSTRING_PTR(body);
        cmd.nbody = RSTRING_LEN(body);
    }
    ctx = cb_context_alloc_common(bucket, 1);
    ctx->headers_val = Qnil;
    if (rb_block_given_p()) {
        ctx->proc = rb_block_proc();
    } else {
        rb_hash_aset(ctx->rv, cb_sym_chunks, rb_ary_new());
    }
    rc = lcb_http3(bucket->handle, (void *)ctx, &cmd);
    if (rc != LCB_SUCCESS) {
        rb_raise(cb_eQuery, "cannot execute HTTPrequest: %s\n", lcb_strerror_short(rc));
    }
    lcb_wait(bucket->handle);

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
