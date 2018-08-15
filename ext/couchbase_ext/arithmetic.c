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
cb_arithmetic_callback(lcb_t handle, int cbtype, const lcb_RESPBASE *rb)
{
    struct cb_context_st *ctx = (struct cb_context_st *)rb->cookie;
    lcb_RESPCOUNTER *resp = (lcb_RESPCOUNTER *)rb;
    VALUE key, res;

    res = rb_class_new_instance(0, NULL, cb_cResult);
    key = rb_external_str_new(rb->key, rb->nkey);
    rb_ivar_set(res, cb_id_iv_key, key);
    rb_ivar_set(res, cb_id_iv_operation, ctx->operation);
    rb_ivar_set(res, cb_id_iv_cas, ULL2NUM(rb->cas));
    if (rb->rc != LCB_SUCCESS) {
        VALUE exc = cb_exc_new(cb_eLibraryError, rb->rc, "failed to update counter for key: %.*s", (int)rb->nkey,
                               (const char *)rb->key);
        rb_ivar_set(res, cb_id_iv_error, exc);
        rb_ivar_set(exc, cb_id_iv_operation, ctx->operation);
    } else {
        rb_ivar_set(res, cb_id_iv_value, ULL2NUM(resp->value));
    }
    if (TYPE(ctx->rv) == T_HASH) {
        rb_hash_aset(ctx->rv, key, res);
    } else {
        ctx->rv = res;
    }
    (void)handle;
    (void)cbtype;
}

typedef struct __cb_counter_arg_i {
    lcb_t handle;
    lcb_CMDCOUNTER *cmd;
    struct cb_context_st *ctx;
    int sign;
} __cb_counter_arg_i;

static int
cb_counter_extract_pairs_i(VALUE key, VALUE value, VALUE cookie)
{
    lcb_error_t err;
    __cb_counter_arg_i *arg = (__cb_counter_arg_i *)cookie;
    if (value != Qnil) {
        switch (TYPE(value)) {
        case T_FIXNUM:
        case T_BIGNUM:
            arg->cmd->delta = arg->sign * (NUM2ULL(value) & INT64_MAX);
            break;
        default:
            lcb_sched_fail(arg->handle);
            cb_context_free(arg->ctx);
            cb_raise_msg(rb_eArgError, "expected number for counter delta, given type: %d", (int)TYPE(value));
        }
    }
    switch (TYPE(key)) {
    case T_SYMBOL:
        key = rb_sym2str(key);
        /* fallthrough */
    case T_STRING:
        LCB_CMD_SET_KEY(arg->cmd, RSTRING_PTR(key), RSTRING_LEN(key));
        err = lcb_counter3(arg->handle, (const void *)arg->ctx, arg->cmd);
        if (err != LCB_SUCCESS) {
            lcb_sched_fail(arg->handle);
            cb_context_free(arg->ctx);
            cb_raise2(cb_eLibraryError, err, "unable to schedule key for counter operation");
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

static inline VALUE
cb_bucket_arithmetic(int sign, int argc, VALUE *argv, VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    struct cb_context_st *ctx;
    VALUE rv, options = Qnil, key, keys = Qnil, value = Qnil;
    lcb_error_t err;
    VALUE operation = sign > 0 ? cb_sym_increment : cb_sym_decrement;
    lcb_CMDCOUNTER cmd = {0};
    long ii;

    if (!cb_bucket_connected_bang(bucket, operation)) {
        return Qnil;
    }

    rb_scan_args(argc, argv, "12", &key, &value, &options);

    switch (TYPE(key)) {
    case T_HASH:
    case T_ARRAY:
        if (!NIL_P(options)) {
            cb_raise_msg(rb_eArgError, "wrong number of arguments (expected 2, type of 3rd arg: %d)",
                         (int)TYPE(options));
        }
        if (TYPE(value) == T_HASH || NIL_P(value)) {
            options = value;
        } else {
            cb_raise_msg(rb_eArgError, "expected options to be a Hash, given type: %d", (int)TYPE(value));
        }
        keys = key;
        key = Qnil;
        break;
    case T_SYMBOL:
        key = rb_sym2str(key);
        /* fallthrough */
    case T_STRING:
        break;
    default:
        cb_raise_msg(rb_eArgError, "expected key to be a Symbol or String, given type: %d", (int)TYPE(key));
        break;
    }
    if (TYPE(value) == T_HASH && NIL_P(options)) {
        options = value;
        value = Qnil;
    }
    cmd.delta = sign;
    cmd.create = bucket->default_arith_create;
    cmd.initial = bucket->default_arith_init;
    if (!NIL_P(options)) {
        VALUE tmp;
        Check_Type(options, T_HASH);
        tmp = rb_hash_aref(options, cb_sym_ttl);
        if (tmp != Qnil) {
            cmd.exptime = NUM2ULONG(tmp);
        }
        tmp = rb_hash_aref(options, cb_sym_create);
        if (tmp != Qnil) {
            cmd.create = RTEST(tmp);
        }
        tmp = rb_hash_aref(options, cb_sym_initial);
        if (tmp != Qnil) {
            cmd.create = 1;
            cmd.initial = NUM2ULL(tmp);
        }
        tmp = rb_hash_aref(options, cb_sym_delta);
        if (tmp != Qnil) {
            cmd.delta = sign * (NUM2ULL(tmp) & INT64_MAX);
        }
    }
    ctx = cb_context_alloc(bucket);
    ctx->operation = operation;
    lcb_sched_enter(bucket->handle);
    if (NIL_P(keys)) {
        if (value != Qnil) {
            switch (TYPE(value)) {
            case T_FIXNUM:
            case T_BIGNUM:
                cmd.delta = sign * (NUM2ULL(value) & INT64_MAX);
                break;
            default:
                lcb_sched_fail(bucket->handle);
                cb_context_free(ctx);
                cb_raise_msg(rb_eArgError, "expected number for counter delta, given type: %d", (int)TYPE(value));
            }
        }
        ctx->rv = Qnil;
        LCB_CMD_SET_KEY(&cmd, RSTRING_PTR(key), RSTRING_LEN(key));
        err = lcb_counter3(bucket->handle, (const void *)ctx, &cmd);
        if (err != LCB_SUCCESS) {
            lcb_sched_fail(bucket->handle);
            cb_context_free(ctx);
            cb_raise2(cb_eLibraryError, err, "unable to schedule store request");
        }
    } else {
        ctx->rv = rb_hash_new();
        switch (TYPE(keys)) {
        case T_HASH: {
            __cb_counter_arg_i iarg = {0};
            iarg.handle = bucket->handle;
            iarg.cmd = &cmd;
            iarg.ctx = ctx;
            iarg.sign = sign;
            rb_hash_foreach(keys, cb_counter_extract_pairs_i, (VALUE)&iarg);
        } break;
        case T_ARRAY:
            for (ii = 0; ii < RARRAY_LEN(keys); ii++) {
                VALUE entry = rb_ary_entry(keys, ii);
                switch (TYPE(entry)) {
                case T_SYMBOL:
                    entry = rb_sym2str(entry);
                    /* fallthrough */
                case T_STRING:
                    LCB_CMD_SET_KEY(&cmd, RSTRING_PTR(entry), RSTRING_LEN(entry));
                    err = lcb_counter3(bucket->handle, (const void *)ctx, &cmd);
                    if (err != LCB_SUCCESS) {
                        lcb_sched_fail(bucket->handle);
                        cb_context_free(ctx);
                        cb_raise2(cb_eLibraryError, err, "unable to schedule key for counter operation");
                    }
                    break;
                default:
                    lcb_sched_fail(bucket->handle);
                    cb_context_free(ctx);
                    cb_raise_msg(rb_eArgError, "expected array or strings or symbols (type=%d)", (int)TYPE(entry));
                    break;
                }
            }
            break;
        default:
            cb_raise_msg(rb_eArgError, "expected keys to be a Array, Hash, Symbol or String, given type: %d",
                         (int)TYPE(keys));
            break;
        }
    }
    lcb_sched_leave(bucket->handle);
    lcb_wait(bucket->handle);
    rv = ctx->rv;
    cb_context_free(ctx);
    return rv;
}

/*
 * Increment the value of an existing numeric key
 *
 * @since 1.0.0
 *
 * The increment methods allow you to increase a given stored integer
 * value. These are the incremental equivalent of the decrement operations
 * and work on the same basis; updating the value of a key if it can be
 * parsed to an integer. The update operation occurs on the server and is
 * provided at the protocol level. This simplifies what would otherwise be a
 * two-stage get and set operation.
 *
 * @note that server treats values as unsigned numbers, therefore if
 * you try to store negative number and then increment or decrement it
 * will cause overflow. (see "Integer overflow" example below)
 *
 * @overload incr(key, delta = 1, options = {})
 *   @param key [String, Symbol] Key used to reference the value.
 *   @param delta [Fixnum] Integer (up to 64 bits) value to increment
 *   @param options [Hash] Options for operation.
 *   @option options [true, false] :create (false) If set to +true+, it will
 *     initialize the key with zero value and zero flags (use +:initial+
 *     option to set another initial value). Note: it won't increment the
 *     missing value.
 *   @option options [Fixnum] :initial (0) Integer (up to 64 bits) value for
 *     missing key initialization. This option imply +:create+ option is
 *     +true+.
 *   @option options [Fixnum] :ttl (self.default_ttl) Expiry time for key.
 *     Values larger than 30*24*60*60 seconds (30 days) are interpreted as
 *     absolute times (from the epoch). This option ignored for existent
 *     keys.
 *   @option options [true, false] :extended (false) If set to +true+, the
 *     operation will return tuple +[value, cas]+, otherwise (by default) it
 *     returns just value.
 *
 *   @return [Fixnum] the actual value of the key.
 *
 *   @raise [Couchbase::Error::NotFound] if key is missing and +:create+
 *     option isn't +true+.
 *
 *   @raise [Couchbase::Error::DeltaBadval] if the key contains non-numeric
 *     value
 *
 *   @raise [Couchbase::Error::Connect] if connection closed (see {Bucket#reconnect})
 *
 *   @raise [ArgumentError] when passing the block in synchronous mode
 *
 *   @example Increment key by one
 *     c.incr("foo")
 *
 *   @example Increment key by 50
 *     c.incr("foo", 50)
 *
 *   @example Increment key by one <b>OR</b> initialize with zero
 *     c.incr("foo", :create => true)   #=> will return old+1 or 0
 *
 *   @example Increment key by one <b>OR</b> initialize with three
 *     c.incr("foo", 50, :initial => 3) #=> will return old+50 or 3
 *
 *   @example Increment key and get its CAS value
 *     val, cas = c.incr("foo", :extended => true)
 *
 *   @example Integer overflow
 *     c.set("foo", -100)
 *     c.get("foo")           #=> -100
 *     c.incr("foo")          #=> 18446744073709551517
 *     # but it might look like working
 *     c.set("foo", -2)
 *     c.get("foo")           #=> -2
 *     c.incr("foo", 2)       #=> 0
 *     # on server:
 *     #    // UINT64_MAX is 18446744073709551615
 *     #    uint64_t num = atoll("-2");
 *     #    // num is 18446744073709551614
 *     #    num += 2
 *     #    // num is 0
 *
 */
VALUE
cb_bucket_incr(int argc, VALUE *argv, VALUE self)
{
    return cb_bucket_arithmetic(+1, argc, argv, self);
}

/*
 * Decrement the value of an existing numeric key
 *
 * @since 1.0.0
 *
 * The decrement methods reduce the value of a given key if the
 * corresponding value can be parsed to an integer value. These operations
 * are provided at a protocol level to eliminate the need to get, update,
 * and reset a simple integer value in the database. It supports the use of
 * an explicit offset value that will be used to reduce the stored value in
 * the database.
 *
 * @note that server values stored and transmitted as unsigned numbers,
 *   therefore if you try to decrement negative or zero key, you will always
 *   get zero.
 *
 * @overload decr(key, delta = 1, options = {})
 *   @param key [String, Symbol] Key used to reference the value.
 *   @param delta [Fixnum] Integer (up to 64 bits) value to decrement
 *   @param options [Hash] Options for operation.
 *   @option options [true, false] :create (false) If set to +true+, it will
 *     initialize the key with zero value and zero flags (use +:initial+
 *     option to set another initial value). Note: it won't decrement the
 *     missing value.
 *   @option options [Fixnum] :initial (0) Integer (up to 64 bits) value for
 *     missing key initialization. This option imply +:create+ option is
 *     +true+.
 *   @option options [Fixnum] :ttl (self.default_ttl) Expiry time for key.
 *     Values larger than 30*24*60*60 seconds (30 days) are interpreted as
 *     absolute times (from the epoch). This option ignored for existent
 *     keys.
 *   @option options [true, false] :extended (false) If set to +true+, the
 *     operation will return tuple +[value, cas]+, otherwise (by default) it
 *     returns just value.
 *
 *   @return [Fixnum] the actual value of the key.
 *
 *   @raise [Couchbase::Error::NotFound] if key is missing and +:create+
 *     option isn't +true+.
 *
 *   @raise [Couchbase::Error::DeltaBadval] if the key contains non-numeric
 *     value
 *
 *   @raise [Couchbase::Error::Connect] if connection closed (see {Bucket#reconnect})
 *
 *   @raise [ArgumentError] when passing the block in synchronous mode
 *
 *   @example Decrement key by one
 *     c.decr("foo")
 *
 *   @example Decrement key by 50
 *     c.decr("foo", 50)
 *
 *   @example Decrement key by one <b>OR</b> initialize with zero
 *     c.decr("foo", :create => true)   #=> will return old-1 or 0
 *
 *   @example Decrement key by one <b>OR</b> initialize with three
 *     c.decr("foo", 50, :initial => 3) #=> will return old-50 or 3
 *
 *   @example Decrement key and get its CAS value
 *     val, cas = c.decr("foo", :extended => true)
 *
 *   @example Decrementing zero
 *     c.set("foo", 0)
 *     c.decrement("foo", 100500)   #=> 0
 *
 *   @example Decrementing negative value
 *     c.set("foo", -100)
 *     c.decrement("foo", 100500)   #=> 0
 *
 */
VALUE
cb_bucket_decr(int argc, VALUE *argv, VALUE self)
{
    return cb_bucket_arithmetic(-1, argc, argv, self);
}
