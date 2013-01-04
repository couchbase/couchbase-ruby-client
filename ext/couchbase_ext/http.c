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
cb_http_complete_callback(lcb_http_request_t request, lcb_t handle, const void *cookie, lcb_error_t error, const lcb_http_resp_t *resp)
{
    struct cb_context_st *ctx = (struct cb_context_st *)cookie;
    struct cb_bucket_st *bucket = ctx->bucket;
    VALUE key, val, res, exc;
    lcb_http_status_t status;

    ctx->request->completed = 1;

    if (bucket->destroying) {
        cb_context_free(ctx);
        return;
    }

    key = STR_NEW((const char*)resp->v.v0.path, resp->v.v0.npath);
    val = resp->v.v0.nbytes ? STR_NEW((const char*)resp->v.v0.bytes, resp->v.v0.nbytes) : Qnil;
    exc = ctx->exception;
    if (!RTEST(exc)) {
        exc = cb_check_error_with_status(error, "failed to execute HTTP request", key, resp->v.v0.status);
        if (exc != Qnil && val != Qnil) {
            rb_ivar_set(exc, cb_id_iv_body, val);
        }
    }
    if (RTEST(exc)) {
        if (rb_obj_is_kind_of(exc, cb_eHTTPError)) {
            rb_funcall(exc, cb_id_parse_body_bang, 0);
        }
        ctx->exception = exc;
    }
    status = resp->v.v0.status;
    if (resp->v.v0.headers) {
        cb_build_headers(ctx, resp->v.v0.headers);
        ctx->headers_val = Qnil;
    }
    if (ctx->extended) {
        res = rb_class_new_instance(0, NULL, cb_cResult);
        rb_ivar_set(res, cb_id_iv_error, ctx->exception);
        rb_ivar_set(res, cb_id_iv_status, status ? INT2FIX(status) : Qnil);
        rb_ivar_set(res, cb_id_iv_operation, cb_sym_http_request);
        rb_ivar_set(res, cb_id_iv_key, key);
        rb_ivar_set(res, cb_id_iv_value, val);
        rb_ivar_set(res, cb_id_iv_completed, Qtrue);
        rb_ivar_set(res, cb_id_iv_headers, ctx->headers_val);
    } else {
        res = val;
    }
    if (ctx->proc != Qnil) {
        cb_proc_call(bucket, ctx->proc, 1, res);
        ctx->proc = Qnil;
    }
    if (!bucket->async && ctx->exception == Qnil) {
        ctx->rv = res;
    }
    if (bucket->async) {
        cb_context_free(ctx);
    }
    (void)handle;
    (void)request;
}

    void
cb_http_data_callback(lcb_http_request_t request, lcb_t handle, const void *cookie, lcb_error_t error, const lcb_http_resp_t *resp)
{
    struct cb_context_st *ctx = (struct cb_context_st *)cookie;
    struct cb_bucket_st *bucket = ctx->bucket;
    VALUE key, val, res;
    lcb_http_status_t status;

    key = STR_NEW((const char*)resp->v.v0.path, resp->v.v0.npath);
    val = resp->v.v0.nbytes ? STR_NEW((const char*)resp->v.v0.bytes, resp->v.v0.nbytes) : Qnil;
    status = resp->v.v0.status;
    if (NIL_P(ctx->exception)) {
        ctx->exception = cb_check_error_with_status(error,
                "failed to execute HTTP request", key, resp->v.v0.status);
        if (ctx->exception != Qnil) {
            VALUE body_str = rb_ivar_get(ctx->exception, cb_id_iv_body);
            if (NIL_P(body_str)) {
                rb_ivar_set(ctx->exception, cb_id_iv_body, val);
            } else {
                rb_str_concat(body_str, val);
            }
            return;
        }
    }
    if (resp->v.v0.headers) {
        cb_build_headers(ctx, resp->v.v0.headers);
    }
    if (ctx->proc != Qnil) {
        if (ctx->extended) {
            res = rb_class_new_instance(0, NULL, cb_cResult);
            rb_ivar_set(res, cb_id_iv_error, Qnil);
            rb_ivar_set(res, cb_id_iv_status, status ? INT2FIX(status) : Qnil);
            rb_ivar_set(res, cb_id_iv_operation, cb_sym_http_request);
            rb_ivar_set(res, cb_id_iv_key, key);
            rb_ivar_set(res, cb_id_iv_value, val);
            rb_ivar_set(res, cb_id_iv_completed, Qfalse);
            rb_ivar_set(res, cb_id_iv_headers, ctx->headers_val);
        } else {
            res = val;
        }
        cb_proc_call(bucket, ctx->proc, 1, res);
    }
    (void)handle;
    (void)request;
}

    void
cb_http_request_free(void *ptr)
{
    struct cb_http_request_st *request = ptr;
    if (request) {
        request->running = 0;
        if (TYPE(request->bucket_obj) == T_DATA
                && RDATA(request->bucket_obj)->dfree == (RUBY_DATA_FUNC)cb_bucket_free
                && !request->completed) {
            lcb_cancel_http_request(request->bucket->handle, request->request);
        }
        free((char *)request->cmd.v.v0.content_type);
        free((char *)request->cmd.v.v0.path);
        free((char *)request->cmd.v.v0.body);
    }
    xfree(request);
}

    void
cb_http_request_mark(void *ptr)
{
    struct cb_http_request_st *request = ptr;
    if (request) {
        rb_gc_mark(request->on_body_callback);
    }
}

    VALUE
cb_http_request_alloc(VALUE klass)
{
    VALUE obj;
    struct cb_http_request_st *request;

    /* allocate new bucket struct and set it to zero */
    obj = Data_Make_Struct(klass, struct cb_http_request_st, cb_http_request_mark,
            cb_http_request_free, request);
    return obj;
}

/*
 * Returns a string containing a human-readable representation of the
 * CouchRequest.
 *
 * @since 1.2.0
 *
 * @return [String]
 */
    VALUE
cb_http_request_inspect(VALUE self)
{
    VALUE str;
    struct cb_http_request_st *req = DATA_PTR(self);
    char buf[200];

    str = rb_str_buf_new2("#<");
    rb_str_buf_cat2(str, rb_obj_classname(self));
    snprintf(buf, 20, ":%p \"", (void *)self);
    rb_str_buf_cat2(str, buf);
    rb_str_buf_cat2(str, req->cmd.v.v0.path);
    snprintf(buf, 100, "\" chunked:%s>", req->cmd.v.v0.chunked ? "true" : "false");
    rb_str_buf_cat2(str, buf);

    return str;
}

/*
 * Initialize new CouchRequest
 *
 * @since 1.2.0
 *
 * @return [Bucket::CouchRequest]
 */
    VALUE
cb_http_request_init(int argc, VALUE *argv, VALUE self)
{
    struct cb_http_request_st *request = DATA_PTR(self);
    VALUE bucket, path, opts, on_body, pp, arg;
    rb_scan_args(argc, argv, "22", &bucket, &pp, &opts, &on_body);

    if (NIL_P(on_body) && rb_block_given_p()) {
        on_body = rb_block_proc();
    }
    if (CLASS_OF(bucket) != cb_cBucket) {
        rb_raise(rb_eTypeError, "wrong argument type (expected Couchbase::Bucket)");
    }
    memset(&request->cmd, 0, sizeof(lcb_http_cmd_t));
    request->type = LCB_HTTP_TYPE_VIEW;
    request->on_body_callback = on_body;
    request->bucket = DATA_PTR(bucket);
    request->bucket_obj = bucket;
    request->extended = Qfalse;
    path = StringValue(pp);	/* convert path to string */
    request->cmd.v.v0.path = strdup(RSTRING_PTR(path));
    request->cmd.v.v0.npath = RSTRING_LEN(path);
    request->cmd.v.v0.method = LCB_HTTP_METHOD_GET;
    request->cmd.v.v0.content_type = strdup("application/json");

    if (opts != Qnil) {
        Check_Type(opts, T_HASH);
        request->extended = RTEST(rb_hash_aref(opts, cb_sym_extended));
        request->cmd.v.v0.chunked = RTEST(rb_hash_aref(opts, cb_sym_chunked));
        if ((arg = rb_hash_aref(opts, cb_sym_type)) != Qnil) {
            if (arg == cb_sym_view) {
                request->type = LCB_HTTP_TYPE_VIEW;
            } else if (arg == cb_sym_management) {
                request->type = LCB_HTTP_TYPE_MANAGEMENT;
            } else {
                rb_raise(rb_eArgError, "unsupported request type");
            }
        }
        if ((arg = rb_hash_aref(opts, cb_sym_method)) != Qnil) {
            if (arg == cb_sym_get) {
                request->cmd.v.v0.method = LCB_HTTP_METHOD_GET;
            } else if (arg == cb_sym_post) {
                request->cmd.v.v0.method = LCB_HTTP_METHOD_POST;
            } else if (arg == cb_sym_put) {
                request->cmd.v.v0.method = LCB_HTTP_METHOD_PUT;
            } else if (arg == cb_sym_delete) {
                request->cmd.v.v0.method = LCB_HTTP_METHOD_DELETE;
            } else {
                rb_raise(rb_eArgError, "unsupported HTTP method");
            }
        }
        if ((arg = rb_hash_aref(opts, cb_sym_body)) != Qnil) {
            Check_Type(arg, T_STRING);
            request->cmd.v.v0.body = strdup(RSTRING_PTR(arg));
            request->cmd.v.v0.nbody = RSTRING_LEN(arg);
        }
        if ((arg = rb_hash_aref(opts, cb_sym_content_type)) != Qnil) {
            Check_Type(arg, T_STRING);
            free((char *)request->cmd.v.v0.content_type);
            request->cmd.v.v0.content_type = strdup(RSTRING_PTR(arg));
        }
    }

    return self;
}

/*
 * Set +on_body+ callback
 *
 * @since 1.2.0
 */
    VALUE
cb_http_request_on_body(VALUE self)
{
    struct cb_http_request_st *request = DATA_PTR(self);
    VALUE old = request->on_body_callback;
    if (rb_block_given_p()) {
        request->on_body_callback = rb_block_proc();
    }
    return old;
}

/*
 * Execute {Bucket::CouchRequest}
 *
 * @since 1.2.0
 */
    VALUE
cb_http_request_perform(VALUE self)
{
    struct cb_http_request_st *req = DATA_PTR(self);
    struct cb_context_st *ctx;
    VALUE rv, exc;
    lcb_error_t err;
    struct cb_bucket_st *bucket = req->bucket;

    if (!cb_bucket_connected_bang(bucket, cb_sym_http_request)) {
        return Qnil;
    }

    ctx = cb_context_alloc(bucket);
    ctx->rv = Qnil;
    ctx->proc = rb_block_given_p() ? rb_block_proc() : req->on_body_callback;
    ctx->extended = req->extended;
    ctx->request = req;
    ctx->headers_val = rb_hash_new();

    err = lcb_make_http_request(bucket->handle, (const void *)ctx,
            req->type, &req->cmd, &req->request);
    exc = cb_check_error(err, "failed to schedule document request",
            STR_NEW(req->cmd.v.v0.path, req->cmd.v.v0.npath));
    if (exc != Qnil) {
        lcb_cancel_http_request(bucket->handle, req->request);
        rb_exc_raise(exc);
    }
    req->running = 1;
    req->ctx = ctx;
    if (bucket->async) {
        return Qnil;
    } else {
        lcb_wait(bucket->handle);
        if (req->completed) {
            rv = ctx->rv;
            exc = ctx->exception;
            cb_context_free(ctx);
            if (exc != Qnil) {
                rb_exc_raise(exc);
            }
            return rv;
        } else {
            return Qnil;
        }
    }
    return Qnil;
}

    VALUE
cb_http_request_pause(VALUE self)
{
    struct cb_http_request_st *req = DATA_PTR(self);
    lcb_breakout(req->bucket->handle);
    return Qnil;
}

    VALUE
cb_http_request_continue(VALUE self)
{
    VALUE exc, rv;
    struct cb_http_request_st *req = DATA_PTR(self);

    if (req->running) {
        lcb_wait(req->bucket->handle);
        if (req->completed) {
            exc = req->ctx->exception;
            rv = req->ctx->rv;
            cb_context_free(req->ctx);
            if (exc != Qnil) {
                rb_exc_raise(exc);
            }
            return rv;
        }
    } else {
        cb_http_request_perform(self);
    }
    return Qnil;
}

/* Document-method: path
 *
 * @since 1.2.0
 *
 * @return [String] the requested path
 */
    VALUE
cb_http_request_path_get(VALUE self)
{
    struct cb_http_request_st *req = DATA_PTR(self);
    return STR_NEW_CSTR(req->cmd.v.v0.path);
}

/* Document-method: chunked
 *
 * @since 1.2.0
 *
 * @return [Boolean] +false+ if library should collect whole response before
 *   yielding, +true+ if the client is ready to handle response in chunks.
 */
    VALUE
cb_http_request_chunked_get(VALUE self)
{
    struct cb_http_request_st *req = DATA_PTR(self);
    return req->cmd.v.v0.chunked ? Qtrue : Qfalse;
}

/* Document-method: extended
 *
 * @since 1.2.0
 *
 * @return [Boolean] if +false+ the callbacks should receive just the data,
 *   and {Couchbase::Result} instance otherwise.
 */
    VALUE
cb_http_request_extended_get(VALUE self)
{
    struct cb_http_request_st *req = DATA_PTR(self);
    return req->extended ? Qtrue : Qfalse;
}

/* Document-method: make_http_request(path, options = {})
 *
 * @since 1.2.0
 *
 * @param path [String]
 * @param options [Hash]
 * @option options [Boolean] :extended (false) set it to +true+ if the
 *   {Couchbase::Result} object needed. The response chunk will be
 *   accessible through +#value+ attribute.
 * @yieldparam [String,Couchbase::Result] res the response chunk if the
 *   :extended option is +false+ and result object otherwise
 *
 * @return [Couchbase::Bucket::CouchRequest]
 */
    VALUE
cb_bucket_make_http_request(int argc, VALUE *argv, VALUE self)
{
    VALUE args[4]; /* bucket, path, options, block */

    args[0] = self;
    rb_scan_args(argc, argv, "11&", &args[1], &args[2], &args[3]);

    return rb_class_new_instance(4, args, cb_cCouchRequest);
}


