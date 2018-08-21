/*
 *   Copyright 2018 Couchbase, Inc.
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

ID cb_sym_include_docs;
ID cb_sym_docs_concurrent_max;
ID cb_sym_id;
ID cb_sym_key;
ID cb_sym_spatial;
ID cb_sym_value;
ID cb_sym_error;
ID cb_sym_doc;

static void
view_callback(lcb_t handle, int type, const lcb_RESPVIEWQUERY *resp)
{
    struct cb_context_st *ctx = (struct cb_context_st *)resp->cookie;

    if (resp->rc != LCB_SUCCESS) {
        rb_hash_aset(ctx->rv, cb_sym_error,
                     cb_exc_new(cb_eValueFormatError, resp->rc,
                                "unable to execute View query. Inner HTTP response (rc: %s, status: %d, body: %.*s)",
                                lcb_strerror_short(resp->htresp->rc), resp->htresp->htstatus, (int)resp->htresp->nbody,
                                (const char *)resp->htresp->body));
        return;
    }
    if (resp->rflags & LCB_RESP_F_FINAL) {
        rb_hash_aset(ctx->rv, cb_sym_meta, rb_external_str_new(resp->value, resp->nvalue));
    } else {
        VALUE res = rb_hash_new();
        rb_hash_aset(res, cb_sym_key, rb_external_str_new(resp->key, resp->nkey));
        rb_hash_aset(res, cb_sym_id, rb_external_str_new(resp->docid, resp->ndocid));
        if (resp->value) {
            rb_hash_aset(res, cb_sym_value, rb_external_str_new(resp->value, resp->nvalue));
        }
        if (resp->docresp) {
            VALUE raw, decoded;
            raw = rb_external_str_new(resp->docresp->value, resp->docresp->nvalue);
            decoded = cb_decode_value(ctx->transcoder, raw, resp->docresp->itmflags, ctx->transcoder_opts);
            if (rb_obj_is_kind_of(decoded, rb_eStandardError)) {
                VALUE exc = cb_exc_new_msg(cb_eValueFormatError, "unable to decode value for key \"%.*s\"",
                                           (int)resp->ndocid, (const char *)resp->docid);
                rb_ivar_set(exc, cb_id_iv_inner_exception, decoded);
                rb_hash_aset(res, cb_sym_error, exc);
            } else {
                rb_hash_aset(res, cb_sym_doc, decoded);
            }
            rb_hash_aset(res, cb_sym_cas, ULL2NUM(resp->docresp->cas));
        }
        rb_ary_push(rb_hash_aref(ctx->rv, cb_sym_rows), res);
    }
    (void)handle;
    (void)type;
}

VALUE
cb_bucket___view_query(int argc, VALUE *argv, VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    struct cb_context_st *ctx;
    lcb_CMDVIEWQUERY cmd = {0};
    lcb_error_t rc;
    VALUE exc, rv;
    VALUE ddoc, view, optstr, postdata, options = Qnil;
    VALUE transcoder, transcoder_opts;

    rb_scan_args(argc, argv, "41", &ddoc, &view, &optstr, &postdata, &options);

    cmd.docs_concurrent_max = 10;
    transcoder = bucket->transcoder;
    transcoder_opts = rb_hash_new();
    if (!NIL_P(options)) {
        if (TYPE(options) != T_HASH) {
            cb_raise_msg(rb_eArgError, "expected options to be a hash. type: %d", (int)TYPE(options));
        } else {
            VALUE tmp;
            tmp = rb_hash_lookup2(options, cb_sym_include_docs, Qundef);
            if (tmp != Qundef && RTEST(tmp)) {
                cmd.cmdflags |= LCB_CMDVIEWQUERY_F_INCLUDE_DOCS;
            }
            tmp = rb_hash_lookup2(options, cb_sym_spatial, Qundef);
            if (tmp != Qundef && RTEST(tmp)) {
                cmd.cmdflags |= LCB_CMDVIEWQUERY_F_SPATIAL;
            }
            tmp = rb_hash_lookup2(options, cb_sym_docs_concurrent_max, Qundef);
            if (tmp != Qundef) {
                Check_Type(tmp, T_FIXNUM);
                cmd.docs_concurrent_max = INT2FIX(tmp);
            }
            tmp = rb_hash_aref(options, cb_sym_format);
            if (tmp != Qnil) {
                if (tmp == cb_sym_document || tmp == cb_sym_marshal || tmp == cb_sym_plain) {
                    transcoder = cb_get_transcoder(bucket, tmp, 1, transcoder_opts);
                } else {
                    cb_raise_msg2(rb_eArgError, "unexpected format (expected :document, :marshal or :plain)");
                }
            }
            tmp = rb_hash_lookup2(options, cb_sym_transcoder, Qundef);
            if (tmp != Qundef) {
                if (tmp == Qnil || (rb_respond_to(tmp, cb_id_dump) && rb_respond_to(tmp, cb_id_load))) {
                    transcoder = cb_get_transcoder(bucket, tmp, 0, transcoder_opts);
                } else {
                    cb_raise_msg2(rb_eArgError, "transcoder must respond to :load and :dump methods");
                }
            }
        }
    }
    if (TYPE(ddoc) != T_STRING) {
        cb_raise_msg(rb_eArgError, "design document name has to be a string. type: %d", (int)TYPE(ddoc));
    }
    cmd.ddoc = RSTRING_PTR(ddoc);
    cmd.nddoc = RSTRING_LEN(ddoc);
    if (TYPE(view) != T_STRING) {
        cb_raise_msg(rb_eArgError, "view name has to be a string. type: %d", (int)TYPE(view));
    }
    cmd.view = RSTRING_PTR(view);
    cmd.nview = RSTRING_LEN(view);
    if (!NIL_P(optstr)) {
        if (TYPE(optstr) != T_STRING) {
            cb_raise_msg(rb_eArgError, "query parameters have to be a string. type: %d", (int)TYPE(optstr));
        }
        cmd.optstr = RSTRING_PTR(optstr);
        cmd.noptstr = RSTRING_LEN(optstr);
    }
    if (!NIL_P(postdata)) {
        if (TYPE(postdata) != T_STRING) {
            cb_raise_msg(rb_eArgError, "POST data has to be a string. type: %d", (int)TYPE(postdata));
        }
        cmd.postdata = RSTRING_PTR(postdata);
        cmd.npostdata = RSTRING_LEN(postdata);
    }
    cmd.callback = view_callback;

    ctx = cb_context_alloc_common(bucket, 1);
    ctx->rv = rb_hash_new();
    ctx->transcoder = transcoder;
    ctx->transcoder_opts = transcoder_opts;

    rb_hash_aset(ctx->rv, cb_sym_rows, rb_ary_new());

    rc = lcb_view_query(bucket->handle, (void *)ctx, &cmd);
    if (rc != LCB_SUCCESS) {
        cb_raise2(cb_eLibraryError, rc, "unable to schedule view request");
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

void
init_views()
{
    cb_sym_include_docs = ID2SYM(rb_intern("include_docs"));
    cb_sym_docs_concurrent_max = ID2SYM(rb_intern("doc_concurrent_max"));
    cb_sym_key = ID2SYM(rb_intern("key"));
    cb_sym_id = ID2SYM(rb_intern("id"));
    cb_sym_spatial = ID2SYM(rb_intern("spatial"));
    cb_sym_value = ID2SYM(rb_intern("value"));
    cb_sym_error = ID2SYM(rb_intern("error"));
    cb_sym_doc = ID2SYM(rb_intern("doc"));
}
