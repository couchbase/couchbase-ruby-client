/*
 *   Copyright 2015 Couchbase, Inc.
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

ID cb_sym_positional;
ID cb_sym_named;
ID cb_sym_prepared;

static void
n1ql_callback(lcb_t handle, int type, const lcb_RESPN1QL *resp)
{
    struct cb_context_st *ctx = (struct cb_context_st *)resp->cookie;
    VALUE res = ctx->rv;
    if (resp->rflags & LCB_RESP_F_FINAL) {
        if (resp->rc != LCB_SUCCESS) {
            char buf[512];
            char *p = buf, *end = buf + 512;
            VALUE meta = Qnil;

            p += snprintf(buf, 512, "failed to perform query, rc = 0x%02x", resp->rc);
            if (resp->htresp) {
                p += snprintf(p, end - p, ". Inner HTTP requeest failed (rc = 0x%02x, http_status = %d)",
                              resp->htresp->rc, resp->htresp->htstatus);
            }
            if (resp->row) {
                VALUE errors;
                meta = rb_funcall(cb_mMultiJson, cb_id_load, 1, STR_NEW(resp->row, resp->nrow));
                errors = rb_hash_lookup2(meta, STR_NEW_CSTR("errors"), Qnil);
                if (errors != Qnil) {
                    int i, len;
                    p += snprintf(p, end - p, ": ");
                    len = RARRAY_LEN(errors);
                    for (i = 0; i < len; i++) {
                        VALUE error = rb_ary_entry(errors, i);
                        int code = FIX2INT(rb_hash_lookup2(error, STR_NEW_CSTR("code"), INT2FIX(0)));
                        char *msg = RSTRING_PTR(rb_hash_lookup2(error, STR_NEW_CSTR("msg"), STR_NEW_CSTR("")));
                        p += snprintf(p, end - p, "%s (%d)", msg, code);
                        if (len > 1 && i < len - 1) {
                            p += snprintf(p, end - p, ",");
                        }
                    }
                }
            }
            ctx->exception = rb_exc_new2(cb_eQuery, buf);
            rb_ivar_set(ctx->exception, cb_id_iv_error, INT2FIX(resp->rc));
            rb_ivar_set(ctx->exception, cb_id_iv_status, INT2FIX(resp->htresp->htstatus));
            rb_ivar_set(ctx->exception, cb_id_iv_meta, meta);
        }
        if (resp->row) {
            rb_hash_aset(res, cb_sym_meta, rb_funcall(cb_mMultiJson, cb_id_load, 1, STR_NEW(resp->row, resp->nrow)));
        }
    } else {
        /* TODO: protect from exceptions from MultiJson */
        VALUE rows = rb_hash_aref(res, cb_sym_rows);
        rb_ary_push(rows, rb_funcall(cb_mMultiJson, cb_id_load, 1, STR_NEW(resp->row, resp->nrow)));
    }
    (void)handle;
    (void)type;
}

typedef struct __cb_query_arg_i {
    lcb_N1QLPARAMS *params;
    lcb_CMDN1QL *cmd;
} __cb_query_arg_i;

lcb_error_t
lcb_n1p_namedparam(lcb_N1QLPARAMS *params, const char *name, size_t nname, const char *value, size_t nvalue)
{
    return lcb_n1p_setopt(params, name, nname, value, nvalue);
}
static int
cb_query_extract_named_params_i(VALUE key, VALUE value, VALUE cookie)
{
    lcb_error_t rc;
    __cb_query_arg_i *arg = (__cb_query_arg_i *)cookie;

    if (TYPE(key) == T_SYMBOL) {
        key = rb_sym2str(key);
    } else if (TYPE(key) != T_STRING) {
        lcb_n1p_free(arg->params);
        cb_raise_msg(cb_eLibraryError, "expected key for N1QL query option to be a String or Symbol, given type: %d",
                     (int)TYPE(key));
    }
    value = rb_funcall(cb_mMultiJson, cb_id_dump, 1, value);
    rc = lcb_n1p_namedparam(arg->params, RSTRING_PTR(key), RSTRING_LEN(key), RSTRING_PTR(value), RSTRING_LEN(value));
    if (rc != LCB_SUCCESS) {
        lcb_n1p_free(arg->params);
        cb_raise(cb_eLibraryError, rc, "cannot set N1QL query named parameter: %.*s", (int)RSTRING_LEN(key),
                 RSTRING_PTR(key));
    }
    return ST_CONTINUE;
}

static int
cb_query_extract_params_i(VALUE key, VALUE value, VALUE cookie)
{
    lcb_error_t rc;
    __cb_query_arg_i *arg = (__cb_query_arg_i *)cookie;

    if (TYPE(key) == T_SYMBOL) {
        if (key == cb_sym_positional) {
            long ii;
            if (TYPE(value) != T_ARRAY) {
                lcb_n1p_free(arg->params);
                cb_raise_msg(cb_eLibraryError,
                             "expected value of :positional option for N1QL query to be an Array, given type: %d",
                             (int)TYPE(value));
            }
            for (ii = 0; ii < RARRAY_LEN(value); ii++) {
                VALUE entry = rb_funcall(cb_mMultiJson, cb_id_dump, 1, rb_ary_entry(value, ii));
                rc = lcb_n1p_posparam(arg->params, RSTRING_PTR(entry), RSTRING_LEN(entry));
                if (rc != LCB_SUCCESS)
                    if (rc != LCB_SUCCESS) {
                        lcb_n1p_free(arg->params);
                        cb_raise2(cb_eLibraryError, rc, "cannot set N1QL query positional parameter");
                    }
            }
        } else if (key == cb_sym_named) {
            if (TYPE(value) != T_HASH) {
                lcb_n1p_free(arg->params);
                cb_raise_msg(cb_eLibraryError,
                             "expected value of :named option for N1QL query to be an Array, given type: %d",
                             (int)TYPE(value));
            }
            rb_hash_foreach(value, cb_query_extract_named_params_i, (VALUE)&arg);
        } else if (key == cb_sym_prepared && RTEST(value)) {
            arg->cmd->cmdflags |= LCB_CMDN1QL_F_PREPCACHE;
        } else {
            key = rb_sym2str(key);
        }
    } else if (TYPE(key) != T_STRING) {
        lcb_n1p_free(arg->params);
        cb_raise_msg(cb_eLibraryError, "expected key for N1QL query option to be a String or Symbol, given type: %d",
                     (int)TYPE(key));
    }
    value = rb_funcall(cb_mMultiJson, cb_id_dump, 1, value);
    rc = lcb_n1p_setopt(arg->params, RSTRING_PTR(key), RSTRING_LEN(key), RSTRING_PTR(value), RSTRING_LEN(value));
    if (rc != LCB_SUCCESS) {
        lcb_n1p_free(arg->params);
        cb_raise(cb_eLibraryError, rc, "cannot set N1QL query option: %.*s", (int)RSTRING_LEN(key), RSTRING_PTR(key));
    }

    return ST_CONTINUE;
}

VALUE
cb_bucket_query(int argc, VALUE *argv, VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    struct cb_context_st *ctx;
    lcb_N1QLPARAMS *params;
    lcb_CMDN1QL cmd = {0};
    lcb_error_t rc;
    VALUE qstr, options = Qnil;
    VALUE exc, rv;

    rb_scan_args(argc, argv, "11", &qstr, &options);

    params = lcb_n1p_new();
    rc = lcb_n1p_setquery(params, RSTRING_PTR(qstr), RSTRING_LEN(qstr), LCB_N1P_QUERY_STATEMENT);
    if (rc != LCB_SUCCESS) {
        lcb_n1p_free(params);
        cb_raise2(cb_eLibraryError, rc, "cannot set query for N1QL command");
    }
    if (options != Qnil) {
        __cb_query_arg_i iarg = {0};
        if (TYPE(options) != T_HASH) {
            lcb_n1p_free(params);
            cb_raise_msg(cb_eLibraryError, "expected options to be a Hash, given type: %d", (int)TYPE(options));
        }
        iarg.params = params;
        iarg.cmd = &cmd;
        rb_hash_foreach(options, cb_query_extract_params_i, (VALUE)&iarg);
    }

    rc = lcb_n1p_mkcmd(params, &cmd);
    if (rc != LCB_SUCCESS) {
        rb_raise(cb_eQuery, "cannot construct N1QL command: %s", lcb_strerror(bucket->handle, rc));
    }

    ctx = cb_context_alloc_common(bucket, 1);
    ctx->rv = rb_hash_new();
    rb_hash_aset(ctx->rv, cb_sym_rows, rb_ary_new());
    rb_hash_aset(ctx->rv, cb_sym_meta, Qnil);
    cmd.callback = n1ql_callback;
    rc = lcb_n1ql_query(bucket->handle, (void *)ctx, &cmd);
    if (rc != LCB_SUCCESS) {
        rb_raise(cb_eQuery, "cannot excute N1QL command: %s\n", lcb_strerror(bucket->handle, rc));
    }
    lcb_n1p_free(params);
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

void
init_n1ql()
{
    cb_sym_positional = ID2SYM(rb_intern("positional"));
    cb_sym_named = ID2SYM(rb_intern("named"));
    cb_sym_prepared = ID2SYM(rb_intern("prepared"));
}
