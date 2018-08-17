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
cb_touch_callback(lcb_t handle, int cbtype, const lcb_RESPBASE *rb)
{
    struct cb_context_st *ctx = (struct cb_context_st *)rb->cookie;
    VALUE key, res;

    res = rb_class_new_instance(0, NULL, cb_cResult);
    key = rb_external_str_new(rb->key, rb->nkey);
    rb_ivar_set(res, cb_id_iv_key, key);
    rb_ivar_set(res, cb_id_iv_operation, cb_sym_touch);
    rb_ivar_set(res, cb_id_iv_cas, ULL2NUM(rb->cas));
    if (rb->rc != LCB_SUCCESS) {
        VALUE exc =
            cb_exc_new(cb_eLibraryError, rb->rc, "failed to touch key: %.*s", (int)rb->nkey, (const char *)rb->key);
        rb_ivar_set(res, cb_id_iv_error, exc);
        rb_ivar_set(exc, cb_id_iv_operation, cb_sym_touch);
    }
    if (TYPE(ctx->rv) == T_HASH) {
        rb_hash_aset(ctx->rv, key, res);
    } else {
        ctx->rv = res;
    }
    (void)handle;
    (void)cbtype;
}

typedef struct __cb_touch_arg_i {
    lcb_t handle;
    lcb_CMDTOUCH *cmd;
    struct cb_context_st *ctx;
} __cb_touch_arg_i;

static int
cb_touch_extract_pairs_i(VALUE key, VALUE value, VALUE cookie)
{
    lcb_error_t err;
    __cb_touch_arg_i *arg = (__cb_touch_arg_i *)cookie;
    if (value != Qnil) {
        if (TYPE(value) != T_FIXNUM) {
            lcb_sched_fail(arg->handle);
            cb_context_free(arg->ctx);
            cb_raise_msg(rb_eArgError, "expected number (expiration) for touch value, given type: %d",
                         (int)TYPE(value));
        }
        arg->cmd->exptime = NUM2ULONG(value);
    }
    switch (TYPE(key)) {
    case T_SYMBOL:
        key = rb_sym2str(key);
        /* fallthrough */
    case T_STRING:
        LCB_CMD_SET_KEY(arg->cmd, RSTRING_PTR(key), RSTRING_LEN(key));
        err = lcb_touch3(arg->handle, (const void *)arg->ctx, arg->cmd);
        if (err != LCB_SUCCESS) {
            lcb_sched_fail(arg->handle);
            cb_context_free(arg->ctx);
            cb_raise2(cb_eLibraryError, err, "unable to schedule key for touch operation");
        }
        break;
    default:
        lcb_sched_fail(arg->handle);
        cb_context_free(arg->ctx);
        cb_raise_msg(rb_eArgError, "expected array or strings or symbols (type=%d)", (int)TYPE(key));
        break;
    }
    return ST_CONTINUE;
}

/*
 * Update the expiry time of an item
 *
 * @since 1.0.0
 *
 * The +touch+ method allow you to update the expiration time on a given
 * key. This can be useful for situations where you want to prevent an item
 * from expiring without resetting the associated value. For example, for a
 * session database you might want to keep the session alive in the database
 * each time the user accesses a web page without explicitly updating the
 * session value, keeping the user's session active and available.
 *
 * @overload touch(key, options = {})
 *   @param key [String, Symbol] Key used to reference the value.
 *   @param options [Hash] Options for operation.
 *   @option options [Fixnum] :ttl (self.default_ttl) Expiry time for key.
 *     Values larger than 30*24*60*60 seconds (30 days) are interpreted as
 *     absolute times (from the epoch).
 *   @option options [true, false] :quiet (self.quiet) If set to +true+, the
 *     operation won't raise error for missing key, it will return +nil+.
 *
 *   @return [true, false] +true+ if the operation was successful and +false+
 *     otherwise.
 *
 *   @raise [Couchbase::Error::Connect] if connection closed (see {Bucket#reconnect})
 *
 *   @raise [ArgumentError] when passing the block in synchronous mode
 *
 *   @example Touch value using +default_ttl+
 *     c.touch("foo")
 *
 *   @example Touch value using custom TTL (10 seconds)
 *     c.touch("foo", :ttl => 10)
 *
 * @overload touch(keys)
 *   @param keys [Hash] The Hash where keys represent the keys in the
 *     database, values -- the expiry times for corresponding key. See
 *     description of +:ttl+ argument above for more information about TTL
 *     values.
 *
 *   @return [Hash] Mapping keys to result of touch operation (+true+ if the
 *     operation was successful and +false+ otherwise)
 *
 *   @example Touch several values
 *     c.touch("foo" => 10, :bar => 20) #=> {"foo" => true, "bar" => true}
 *
 *   @example Touch single value
 *     c.touch("foo" => 10)             #=> true
 *
 */
VALUE
cb_bucket_touch(int argc, VALUE *argv, VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    struct cb_context_st *ctx;
    VALUE rv, options = Qnil, arg;
    lcb_error_t err;
    lcb_CMDTOUCH cmd = {0};
    long ii;

    if (!cb_bucket_connected_bang(bucket, cb_sym_touch)) {
        return Qnil;
    }

    rb_scan_args(argc, argv, "11", &arg, &options);

    cmd.exptime = bucket->default_ttl;
    if (!NIL_P(options)) {
        switch (TYPE(options)) {
        case T_HASH: {
            VALUE tmp;
            Check_Type(options, T_HASH);
            tmp = rb_hash_aref(options, cb_sym_ttl);
            if (tmp != Qnil) {
                cmd.exptime = NUM2ULONG(tmp);
            }
        } break;
        case T_FIXNUM:
            cmd.exptime = NUM2ULONG(options);
            break;
        default:
            cb_raise_msg(rb_eArgError, "expected Hash options or Number (expiration) as second argument (type=%d)",
                         (int)TYPE(options));
            break;
        }
    }

    ctx = cb_context_alloc(bucket);
    lcb_sched_enter(bucket->handle);
    switch (TYPE(arg)) {
    case T_HASH: {
        __cb_touch_arg_i iarg = {0};
        iarg.handle = bucket->handle;
        iarg.cmd = &cmd;
        iarg.ctx = ctx;
        ctx->rv = rb_hash_new();
        rb_hash_foreach(arg, cb_touch_extract_pairs_i, (VALUE)&iarg);
    } break;
    case T_ARRAY:
        for (ii = 0; ii < RARRAY_LEN(arg); ii++) {
            VALUE entry = rb_ary_entry(arg, ii);
            switch (TYPE(entry)) {
            case T_SYMBOL:
                arg = rb_sym2str(arg);
                /* fallthrough */
            case T_STRING:
                LCB_CMD_SET_KEY(&cmd, RSTRING_PTR(entry), RSTRING_LEN(entry));
                err = lcb_touch3(bucket->handle, (const void *)ctx, &cmd);
                if (err != LCB_SUCCESS) {
                    lcb_sched_fail(bucket->handle);
                    cb_context_free(ctx);
                    cb_raise2(cb_eLibraryError, err, "unable to schedule key for touch operation");
                }
                ctx->rv = Qnil;
                break;
            default:
                lcb_sched_fail(bucket->handle);
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
        err = lcb_touch3(bucket->handle, (const void *)ctx, &cmd);
        if (err != LCB_SUCCESS) {
            lcb_sched_fail(bucket->handle);
            cb_context_free(ctx);
            cb_raise2(cb_eLibraryError, err, "unable to schedule key for touch operation");
        }
        ctx->rv = Qnil;
        break;
    default:
        lcb_sched_fail(bucket->handle);
        cb_context_free(ctx);
        cb_raise_msg(rb_eArgError, "expected array of keys or single key (type=%d)", (int)TYPE(arg));
        break;
    }
    lcb_sched_leave(bucket->handle);

    lcb_wait(bucket->handle);
    rv = ctx->rv;
    cb_context_free(ctx);
    return rv;
}
