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
cb_unlock_callback(lcb_t handle, int cbtype, const lcb_RESPBASE *rb)
{
    struct cb_context_st *ctx = (struct cb_context_st *)rb->cookie;
    VALUE key, res;

    res = rb_class_new_instance(0, NULL, cb_cResult);
    key = rb_external_str_new(rb->key, rb->nkey);
    rb_ivar_set(res, cb_id_iv_key, key);
    rb_ivar_set(res, cb_id_iv_operation, cb_sym_unlock);
    if (rb->rc != LCB_SUCCESS) {
        VALUE exc =
            cb_exc_new(cb_eLibraryError, rb->rc, "failed to unlock key: %.*s", (int)rb->nkey, (const char *)rb->key);
        rb_ivar_set(res, cb_id_iv_error, exc);
        rb_ivar_set(exc, cb_id_iv_operation, cb_sym_unlock);
    }
    if (TYPE(ctx->rv) == T_HASH) {
        rb_hash_aset(ctx->rv, key, res);
    } else {
        ctx->rv = res;
    }
    (void)cbtype;
    (void)handle;
}

typedef struct __cb_unlock_arg_i {
    lcb_t handle;
    lcb_CMDUNLOCK *cmd;
    struct cb_context_st *ctx;
} __cb_unlock_arg_i;

static int
cb_unlock_extract_pairs_i(VALUE key, VALUE value, VALUE cookie)
{
    lcb_error_t err;
    __cb_unlock_arg_i *arg = (__cb_unlock_arg_i *)cookie;
    if (value != Qnil) {
        switch (TYPE(value)) {
        case T_FIXNUM:
        case T_BIGNUM:
            arg->cmd->cas = NUM2ULL(value);
            break;
        default:
            lcb_sched_fail(arg->handle);
            cb_context_free(arg->ctx);
            cb_raise_msg(rb_eArgError, "expected number (CAS) for unlock value, given type: %d", (int)TYPE(value));
        }
    }
    switch (TYPE(key)) {
    case T_SYMBOL:
        key = rb_sym2str(key);
        /* fallthrough */
    case T_STRING:
        LCB_CMD_SET_KEY(arg->cmd, RSTRING_PTR(key), RSTRING_LEN(key));
        err = lcb_unlock3(arg->handle, (const void *)arg->ctx, arg->cmd);
        if (err != LCB_SUCCESS) {
            lcb_sched_fail(arg->handle);
            cb_context_free(arg->ctx);
            cb_raise2(cb_eLibraryError, err, "unable to schedule key for unlock operation");
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
 * Unlock key
 *
 * @since 1.2.0
 *
 * The +unlock+ method allow you to unlock key once locked by {Bucket#get}
 * with +:lock+ option.
 *
 * @overload unlock(key, options = {})
 *   @param key [String, Symbol] Key used to reference the value.
 *   @param options [Hash] Options for operation.
 *   @option options [Fixnum] :cas The CAS value must match the current one
 *     from the storage.
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
 *   @raise [Couchbase::Error::NotFound] if key(s) not found in the storage
 *
 *   @raise [Couchbase::Error::TemporaryFail] if either the key wasn't
 *      locked or given CAS value doesn't match to actual in the storage
 *
 *   @example Unlock the single key
 *     val, _, cas = c.get("foo", :lock => true, :extended => true)
 *     c.unlock("foo", :cas => cas)
 *
 * @overload unlock(keys)
 *   @param keys [Hash] The Hash where keys represent the keys in the
 *     database, values -- the CAS for corresponding key.
 *
 *   @return [Hash] Mapping keys to result of unlock operation (+true+ if the
 *     operation was successful and +false+ otherwise)
 *
 *   @example Unlock several keys
 *     c.unlock("foo" => cas1, :bar => cas2) #=> {"foo" => true, "bar" => true}
 */
VALUE
cb_bucket_unlock(int argc, VALUE *argv, VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    struct cb_context_st *ctx;
    VALUE rv, arg, options = Qnil;
    lcb_error_t err;
    lcb_CMDUNLOCK cmd = {0};

    if (!cb_bucket_connected_bang(bucket, cb_sym_unlock)) {
        return Qnil;
    }

    rb_scan_args(argc, argv, "11", &arg, &options);

    if (!NIL_P(options)) {
        switch (TYPE(options)) {
        case T_HASH: {
            VALUE tmp;
            Check_Type(options, T_HASH);
            tmp = rb_hash_aref(options, cb_sym_cas);
            if (tmp != Qnil) {
                cmd.cas = NUM2ULL(tmp);
            }
        } break;
        case T_FIXNUM:
        case T_BIGNUM:
            cmd.cas = NUM2ULL(options);
            break;
        default:
            cb_raise_msg(rb_eArgError, "expected Hash options or Number (CAS) as second argument (type=%d)",
                         (int)TYPE(options));
            break;
        }
    }

    ctx = cb_context_alloc(bucket);
    lcb_sched_enter(bucket->handle);
    switch (TYPE(arg)) {
    case T_HASH: {
        __cb_unlock_arg_i iarg = {0};
        iarg.handle = bucket->handle;
        iarg.cmd = &cmd;
        iarg.ctx = ctx;
        ctx->rv = rb_hash_new();
        rb_hash_foreach(arg, cb_unlock_extract_pairs_i, (VALUE)&iarg);
    } break;
    case T_SYMBOL:
        arg = rb_sym2str(arg);
        /* fallthrough */
    case T_STRING:
        LCB_CMD_SET_KEY(&cmd, RSTRING_PTR(arg), RSTRING_LEN(arg));
        err = lcb_unlock3(bucket->handle, (const void *)ctx, &cmd);
        if (err != LCB_SUCCESS) {
            lcb_sched_fail(bucket->handle);
            cb_context_free(ctx);
            cb_raise2(cb_eLibraryError, err, "unable to schedule key for unlock operation");
        }
        ctx->rv = rb_ary_new();
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
