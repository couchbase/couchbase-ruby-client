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

    VALUE
cb_gc_protect(struct bucket_st *bucket, VALUE val)
{
    rb_hash_aset(bucket->object_space, val|1, val);
    return val;
}

    VALUE
cb_gc_unprotect(struct bucket_st *bucket, VALUE val)
{
    rb_funcall(bucket->object_space, id_delete, 1, val|1);
    return val;
}

    VALUE
cb_proc_call(VALUE recv, int argc, ...)
{
    VALUE *argv;
    va_list ar;
    int arity;
    int ii;

    arity = FIX2INT(rb_funcall(recv, id_arity, 0));
    if (arity < 0) {
        arity = argc;
    }
    if (arity > 0) {
        va_init_list(ar, argc);
        argv = ALLOCA_N(VALUE, argc);
        for (ii = 0; ii < arity; ++ii) {
            if (ii < argc) {
                argv[ii] = va_arg(ar, VALUE);
            } else {
                argv[ii] = Qnil;
            }
        }
        va_end(ar);
    } else {
        argv = NULL;
    }
    return rb_funcall2(recv, id_call, arity, argv);
}

VALUE
cb_hash_delete(VALUE hash, VALUE key)
{
    return rb_funcall(hash, id_delete, 1, key);
}

/* Helper to convert return code from libcouchbase to meaningful exception.
 * Returns nil if the code considering successful and exception object
 * otherwise. Store given string to exceptions as message, and also
 * initialize +error+ attribute with given return code.  */
    VALUE
cb_check_error_with_status(lcb_error_t rc, const char *msg, VALUE key,
        lcb_http_status_t status)
{
    VALUE klass, exc, str;
    char buf[300];

    if (rc == LCB_SUCCESS || rc == LCB_AUTH_CONTINUE) {
        return Qnil;
    }
    switch (rc) {
        case LCB_AUTH_ERROR:
            klass = eAuthError;
            break;
        case LCB_DELTA_BADVAL:
            klass = eDeltaBadvalError;
            break;
        case LCB_E2BIG:
            klass = eTooBigError;
            break;
        case LCB_EBUSY:
            klass = eBusyError;
            break;
        case LCB_EINTERNAL:
            klass = eInternalError;
            break;
        case LCB_EINVAL:
            klass = eInvalidError;
            break;
        case LCB_ENOMEM:
            klass = eNoMemoryError;
            break;
        case LCB_ERANGE:
            klass = eRangeError;
            break;
        case LCB_ETMPFAIL:
            klass = eTmpFailError;
            break;
        case LCB_KEY_EEXISTS:
            klass = eKeyExistsError;
            break;
        case LCB_KEY_ENOENT:
            klass = eNotFoundError;
            break;
        case LCB_LIBEVENT_ERROR:
            klass = eLibeventError;
            break;
        case LCB_NETWORK_ERROR:
            klass = eNetworkError;
            break;
        case LCB_NOT_MY_VBUCKET:
            klass = eNotMyVbucketError;
            break;
        case LCB_NOT_STORED:
            klass = eNotStoredError;
            break;
        case LCB_NOT_SUPPORTED:
            klass = eNotSupportedError;
            break;
        case LCB_UNKNOWN_COMMAND:
            klass = eUnknownCommandError;
            break;
        case LCB_UNKNOWN_HOST:
            klass = eUnknownHostError;
            break;
        case LCB_PROTOCOL_ERROR:
            klass = eProtocolError;
            break;
        case LCB_ETIMEDOUT:
            klass = eTimeoutError;
            break;
        case LCB_CONNECT_ERROR:
            klass = eConnectError;
            break;
        case LCB_BUCKET_ENOENT:
            klass = eBucketNotFoundError;
            break;
        case LCB_CLIENT_ENOMEM:
            klass = eClientNoMemoryError;
            break;
        case LCB_CLIENT_ETMPFAIL:
            klass = eClientTmpFailError;
            break;
        case LCB_EBADHANDLE:
            klass = eBadHandleError;
            break;
        case LCB_ERROR:
            /* fall through */
        default:
            klass = eLibcouchbaseError;
    }

    str = rb_str_buf_new2(msg ? msg : "");
    rb_str_buf_cat2(str, " (");
    if (key != Qnil) {
        snprintf(buf, 300, "key=\"%s\", ", RSTRING_PTR(key));
        rb_str_buf_cat2(str, buf);
    }
    if (status > 0) {
        const char *reason = NULL;
        snprintf(buf, 300, "status=\"%d\"", status);
        rb_str_buf_cat2(str, buf);
        switch (status) {
            case LCB_HTTP_STATUS_BAD_REQUEST:
                reason = " (Bad Request)";
                break;
            case LCB_HTTP_STATUS_UNAUTHORIZED:
                reason = " (Unauthorized)";
                break;
            case LCB_HTTP_STATUS_PAYMENT_REQUIRED:
                reason = " (Payment Required)";
                break;
            case LCB_HTTP_STATUS_FORBIDDEN:
                reason = " (Forbidden)";
                break;
            case LCB_HTTP_STATUS_NOT_FOUND:
                reason = " (Not Found)";
                break;
            case LCB_HTTP_STATUS_METHOD_NOT_ALLOWED:
                reason = " (Method Not Allowed)";
                break;
            case LCB_HTTP_STATUS_NOT_ACCEPTABLE:
                reason = " (Not Acceptable)";
                break;
            case LCB_HTTP_STATUS_PROXY_AUTHENTICATION_REQUIRED:
                reason = " (Proxy Authentication Required)";
                break;
            case LCB_HTTP_STATUS_REQUEST_TIMEOUT:
                reason = " (Request Timeout)";
                break;
            case LCB_HTTP_STATUS_CONFLICT:
                reason = " (Conflict)";
                break;
            case LCB_HTTP_STATUS_GONE:
                reason = " (Gone)";
                break;
            case LCB_HTTP_STATUS_LENGTH_REQUIRED:
                reason = " (Length Required)";
                break;
            case LCB_HTTP_STATUS_PRECONDITION_FAILED:
                reason = " (Precondition Failed)";
                break;
            case LCB_HTTP_STATUS_REQUEST_ENTITY_TOO_LARGE:
                reason = " (Request Entity Too Large)";
                break;
            case LCB_HTTP_STATUS_REQUEST_URI_TOO_LONG:
                reason = " (Request Uri Too Long)";
                break;
            case LCB_HTTP_STATUS_UNSUPPORTED_MEDIA_TYPE:
                reason = " (Unsupported Media Type)";
                break;
            case LCB_HTTP_STATUS_REQUESTED_RANGE_NOT_SATISFIABLE:
                reason = " (Requested Range Not Satisfiable)";
                break;
            case LCB_HTTP_STATUS_EXPECTATION_FAILED:
                reason = " (Expectation Failed)";
                break;
            case LCB_HTTP_STATUS_UNPROCESSABLE_ENTITY:
                reason = " (Unprocessable Entity)";
                break;
            case LCB_HTTP_STATUS_LOCKED:
                reason = " (Locked)";
                break;
            case LCB_HTTP_STATUS_FAILED_DEPENDENCY:
                reason = " (Failed Dependency)";
                break;
            case LCB_HTTP_STATUS_INTERNAL_SERVER_ERROR:
                reason = " (Internal Server Error)";
                break;
            case LCB_HTTP_STATUS_NOT_IMPLEMENTED:
                reason = " (Not Implemented)";
                break;
            case LCB_HTTP_STATUS_BAD_GATEWAY:
                reason = " (Bad Gateway)";
                break;
            case LCB_HTTP_STATUS_SERVICE_UNAVAILABLE:
                reason = " (Service Unavailable)";
                break;
            case LCB_HTTP_STATUS_GATEWAY_TIMEOUT:
                reason = " (Gateway Timeout)";
                break;
            case LCB_HTTP_STATUS_HTTP_VERSION_NOT_SUPPORTED:
                reason = " (Http Version Not Supported)";
                break;
            case LCB_HTTP_STATUS_INSUFFICIENT_STORAGE:
                reason = " (Insufficient Storage)";
                break;
            default:
                reason = "";
        }
        rb_str_buf_cat2(str, reason);
        rb_str_buf_cat2(str, ", ");

    }
    snprintf(buf, 300, "error=0x%02x)", rc);
    rb_str_buf_cat2(str, buf);
    exc = rb_exc_new3(klass, str);
    rb_ivar_set(exc, id_iv_error, INT2FIX(rc));
    rb_ivar_set(exc, id_iv_key, key);
    rb_ivar_set(exc, id_iv_cas, Qnil);
    rb_ivar_set(exc, id_iv_operation, Qnil);
    rb_ivar_set(exc, id_iv_status, status ? INT2FIX(status) : Qnil);
    return exc;
}

    VALUE
cb_check_error(lcb_error_t rc, const char *msg, VALUE key)
{
    return cb_check_error_with_status(rc, msg, key, 0);
}


    uint32_t
flags_set_format(uint32_t flags, ID format)
{
    flags &= ~((uint32_t)FMT_MASK); /* clear format bits */

    if (format == sym_document) {
        return flags | FMT_DOCUMENT;
    } else if (format == sym_marshal) {
        return flags | FMT_MARSHAL;
    } else if (format == sym_plain) {
        return flags | FMT_PLAIN;
    }
    return flags; /* document is the default */
}

    ID
flags_get_format(uint32_t flags)
{
    flags &= FMT_MASK; /* select format bits */

    switch (flags) {
        case FMT_DOCUMENT:
            return sym_document;
        case FMT_MARSHAL:
            return sym_marshal;
        case FMT_PLAIN:
            /* fall through */
        default:
            /* all other formats treated as plain */
            return sym_plain;
    }
}


    static VALUE
do_encode(VALUE *args)
{
    VALUE val = args[0];
    uint32_t flags = ((uint32_t)args[1] & FMT_MASK);

    switch (flags) {
        case FMT_DOCUMENT:
            return rb_funcall(mMultiJson, id_dump, 1, val);
        case FMT_MARSHAL:
            return rb_funcall(mMarshal, id_dump, 1, val);
        case FMT_PLAIN:
            /* fall through */
        default:
            /* all other formats treated as plain */
            return val;
    }
}

    static VALUE
do_decode(VALUE *args)
{
    VALUE blob = args[0];
    VALUE force_format = args[2];

    if (TYPE(force_format) == T_SYMBOL) {
        if (force_format == sym_document) {
            return rb_funcall(mMultiJson, id_load, 1, blob);
        } else if (force_format == sym_marshal) {
            return rb_funcall(mMarshal, id_load, 1, blob);
        } else { /* sym_plain and any other symbol */
            return blob;
        }
    } else {
        uint32_t flags = ((uint32_t)args[1] & FMT_MASK);

        switch (flags) {
            case FMT_DOCUMENT:
                return rb_funcall(mMultiJson, id_load, 1, blob);
            case FMT_MARSHAL:
                return rb_funcall(mMarshal, id_load, 1, blob);
            case FMT_PLAIN:
                /* fall through */
            default:
                /* all other formats treated as plain */
                return blob;
        }
    }
}

    static VALUE
coding_failed(void)
{
    return Qundef;
}

    VALUE
encode_value(VALUE val, uint32_t flags)
{
    VALUE blob, args[2];

    args[0] = val;
    args[1] = (VALUE)flags;
    /* FIXME re-raise proper exception */
    blob = rb_rescue(do_encode, (VALUE)args, coding_failed, 0);
    /* it must be bytestring after all */
    if (TYPE(blob) != T_STRING) {
        return Qundef;
    }
    return blob;
}

    VALUE
decode_value(VALUE blob, uint32_t flags, VALUE force_format)
{
    VALUE val, args[3];

    /* first it must be bytestring */
    if (TYPE(blob) != T_STRING) {
        return Qundef;
    }
    args[0] = blob;
    args[1] = (VALUE)flags;
    args[2] = (VALUE)force_format;
    val = rb_rescue(do_decode, (VALUE)args, coding_failed, 0);
    return val;
}

    void
strip_key_prefix(struct bucket_st *bucket, VALUE key)
{
    if (bucket->key_prefix) {
        rb_str_update(key, 0, RSTRING_LEN(bucket->key_prefix_val), STR_NEW_CSTR(""));
    }
}

    VALUE
unify_key(struct bucket_st *bucket, VALUE key, int apply_prefix)
{
    VALUE ret = Qnil, tmp;

    if (bucket->key_prefix && apply_prefix) {
        ret = rb_str_dup(bucket->key_prefix_val);
    }
    switch (TYPE(key)) {
        case T_STRING:
            return NIL_P(ret) ? key : rb_str_concat(ret, key);
        case T_SYMBOL:
            tmp = STR_NEW_CSTR(rb_id2name(SYM2ID(key)));
            return NIL_P(ret) ? tmp : rb_str_concat(ret, tmp);
        default:    /* call #to_str or raise error */
            tmp = StringValue(key);
            return NIL_P(ret) ? tmp : rb_str_concat(ret, tmp);
    }
}

    void
cb_build_headers(struct context_st *ctx, const char * const *headers)
{
    if (!ctx->headers_built) {
        VALUE key = Qnil, val;
        for (size_t ii = 1; *headers != NULL; ++ii, ++headers) {
            if (ii % 2 == 0) {
                if (key == Qnil) {
                    break;
                }
                val = rb_hash_aref(ctx->headers_val, key);
                switch (TYPE(val)) {
                    case T_NIL:
                        rb_hash_aset(ctx->headers_val, key, STR_NEW_CSTR(*headers));
                        break;
                    case T_ARRAY:
                        rb_ary_push(val, STR_NEW_CSTR(*headers));
                        break;
                    default:
                        {
                            VALUE ary = rb_ary_new();
                            rb_ary_push(ary, val);
                            rb_ary_push(ary, STR_NEW_CSTR(*headers));
                            rb_hash_aset(ctx->headers_val, key, ary);
                        }
                }
            } else {
                key = STR_NEW_CSTR(*headers);
            }
        }
        ctx->headers_built = 1;
    }
}

    int
cb_first_value_i(VALUE key, VALUE value, VALUE arg)
{
    VALUE *val = (VALUE *)arg;

    *val = value;
    (void)key;
    return ST_STOP;
}
