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
cb_get_callback(lcb_t handle, int cbtype, const lcb_RESPBASE *rb)
{
    struct cb_context_st *ctx = (struct cb_context_st *)rb->cookie;
    const lcb_RESPGET *resp = (const lcb_RESPGET *)rb;
    VALUE key, res;

    res = rb_class_new_instance(0, NULL, cb_cResult);
    key = rb_external_str_new(rb->key, rb->nkey);
    rb_ivar_set(res, cb_id_iv_key, key);
    rb_ivar_set(res, cb_id_iv_operation, cb_sym_get);
    if (rb->rc != LCB_SUCCESS) {
        VALUE exc =
            cb_exc_new(cb_eLibraryError, rb->rc, "failed to get key: %.*s", (int)rb->nkey, (const char *)rb->key);
        rb_ivar_set(res, cb_id_iv_error, exc);
        rb_ivar_set(exc, cb_id_iv_operation, cb_sym_get);
    } else {
        VALUE raw, decoded;
        raw = rb_external_str_new(resp->value, resp->nvalue);
        decoded = cb_decode_value(ctx->transcoder, raw, resp->itmflags, ctx->transcoder_opts);
        if (rb_obj_is_kind_of(decoded, rb_eStandardError)) {
            VALUE exc = cb_exc_new_msg(cb_eValueFormatError, "unable to decode value for key \"%.*s\"",
                                       (int)RSTRING_LEN(key), (const char *)RSTRING_PTR(key));
            rb_ivar_set(exc, cb_id_iv_inner_exception, decoded);
            rb_ivar_set(exc, cb_id_iv_operation, cb_sym_touch);
            rb_ivar_set(res, cb_id_iv_error, exc);
        } else {
            rb_ivar_set(res, cb_id_iv_value, decoded);
        }
        // rb_ivar_set(res, cb_id_iv_datatype, UINT2NUM(rb->datatype));
        rb_ivar_set(res, cb_id_iv_cas, ULL2NUM(rb->cas));
    }

    if (TYPE(ctx->rv) == T_HASH) {
        rb_hash_aset(ctx->rv, key, res);
    } else {
        ctx->rv = res;
    }
    (void)cbtype;
    (void)handle;
}

typedef struct __cb_get_arg_i {
    lcb_t handle;
    lcb_CMDGET *cmd;
    struct cb_context_st *ctx;
} __cb_get_arg_i;

static int
cb_get_extract_pairs_i(VALUE key, VALUE value, VALUE cookie)
{
    lcb_error_t err;
    __cb_get_arg_i *arg = (__cb_get_arg_i *)cookie;
    if (value != Qnil) {
        if (TYPE(value) != T_FIXNUM) {
            lcb_sched_fail(arg->handle);
            cb_context_free(arg->ctx);
            cb_raise_msg(rb_eArgError, "expected number (expiration) for get value, given type: %d", (int)TYPE(value));
        }
        arg->cmd->exptime = NUM2ULONG(value);
    }
    switch (TYPE(key)) {
    case T_SYMBOL:
        key = rb_sym2str(key);
        /* fallthrough */
    case T_STRING:
        LCB_CMD_SET_KEY(arg->cmd, RSTRING_PTR(key), RSTRING_LEN(key));
        err = lcb_get3(arg->handle, (const void *)arg->ctx, arg->cmd);
        if (err != LCB_SUCCESS) {
            lcb_sched_fail(arg->handle);
            cb_context_free(arg->ctx);
            cb_raise2(cb_eLibraryError, err, "unable to schedule key for get operation");
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
 * Obtain an object stored in Couchbase by given key.
 *
 * @since 1.0.0
 *
 * @see
 * http://couchbase.com/docs/couchbase-manual-2.0/couchbase-architecture-apis-memcached-protocol-additions.html#couchbase-architecture-apis-memcached-protocol-additions-getl
 *
 * @overload get(*keys, options = {})
 *   @param keys [String, Symbol, Array] One or several keys to fetch
 *   @param options [Hash] Options for operation.
 *   @option options [Fixnum] :ttl (self.default_ttl) Expiry time for key.
 *     Values larger than 30*24*60*60 seconds (30 days) are interpreted as
 *     absolute times (from the epoch).
 *   @option options [Symbol] :format (nil) Explicitly choose the decoder
 *     for this key (+:plain+, +:document+, +:marshal+). See
 *     {Bucket#default_format}.
 *   @option options [Fixnum, Boolean] :lock Lock the keys for time span.
 *     If this parameter is +true+ the key(s) will be locked for default
 *     timeout. Also you can use number to setup your own timeout in
 *     seconds. If it will be lower that zero or exceed the maximum, the
 *     server will use default value. You can determine actual default and
 *     maximum values calling {Bucket#stats} without arguments and
 *     inspecting keys  "ep_getl_default_timeout" and "ep_getl_max_timeout"
 *     correspondingly. See overloaded hash syntax to specify custom timeout
 *     per each key.
 *   @option options [true, false, :all, :first, Fixnum] :replica
 *     (false) Read key from replica node. Options +:ttl+ and +:lock+
 *     are not compatible with +:replica+. Value +true+ is a synonym to
 *     +:first+, which means sequentially iterate over all replicas
 *     and return first successful response, skipping all failures.
 *     It is also possible to query all replicas in parallel using
 *     the +:all+ option, or pass a replica index, starting from zero.
 *
 *   @return [Object, Array, Hash] the value(s) (or tuples in extended mode)
 *     associated with the key.
 *
 *   @raise [Couchbase::Error::NotFound] if the key is missing in the
 *     bucket.
 *
 *   @raise [Couchbase::Error::Connect] if connection closed (see {Bucket#reconnect})
 *
 *   @raise [ArgumentError] when passing the block in synchronous mode
 *
 *   @example Get single value in quiet mode (the default)
 *     c.get("foo")     #=> the associated value or nil
 *
 *   @example Use alternative hash-like syntax
 *     c["foo"]         #=> the associated value or nil
 *
 *   @example Get single value in verbose mode
 *     c.get("missing-foo", :quiet => false)  #=> raises Couchbase::NotFound
 *     c.get("missing-foo", :quiet => true)   #=> returns nil
 *
 *   @example Get and touch single value. The key won't be accessible after 10 seconds
 *     c.get("foo", :ttl => 10)
 *
 *   @example Extended get
 *     val, flags, cas = c.get("foo", :extended => true)
 *
 *   @example Get multiple keys
 *     c.get("foo", "bar", "baz")   #=> [val1, val2, val3]
 *
 *   @example Get multiple keys with assembing result into the Hash
 *     c.get("foo", "bar", "baz", :assemble_hash => true)
 *     #=> {"foo" => val1, "bar" => val2, "baz" => val3}
 *
 *   @example Extended get multiple keys
 *     c.get("foo", "bar", :extended => true)
 *     #=> {"foo" => [val1, flags1, cas1], "bar" => [val2, flags2, cas2]}
 *
 *   @example Get and lock key using default timeout
 *     c.get("foo", :lock => true)
 *
 *   @example Determine lock timeout parameters
 *     c.stats.values_at("ep_getl_default_timeout", "ep_getl_max_timeout")
 *     #=> [{"127.0.0.1:11210"=>"15"}, {"127.0.0.1:11210"=>"30"}]
 *
 *   @example Get and lock key using custom timeout
 *     c.get("foo", :lock => 3)
 *
 *   @example Get and lock multiple keys using custom timeout
 *     c.get("foo", "bar", :lock => 3)
 *
 * @overload get(keys, options = {})
 *   When the method receive hash map, it will behave like it receive list
 *   of keys (+keys.keys+), but also touch each key setting expiry time to
 *   the corresponding value. But unlike usual get this command always
 *   return hash map +{key => value}+ or +{key => [value, flags, cas]}+.
 *
 *   @param keys [Hash] Map key-ttl
 *   @param options [Hash] Options for operation. (see options definition
 *     above)
 *
 *   @return [Hash] the values (or tuples in extended mode) associated with
 *     the keys.
 *
 *   @example Get and touch multiple keys
 *     c.get("foo" => 10, "bar" => 20)   #=> {"foo" => val1, "bar" => val2}
 *
 *   @example Extended get and touch multiple keys
 *     c.get({"foo" => 10, "bar" => 20}, :extended => true)
 *     #=> {"foo" => [val1, flags1, cas1], "bar" => [val2, flags2, cas2]}
 *
 *   @example Get and lock multiple keys for chosen period in seconds
 *     c.get("foo" => 10, "bar" => 20, :lock => true)
 *     #=> {"foo" => val1, "bar" => val2}
 */
VALUE
cb_bucket_get(int argc, VALUE *argv, VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    struct cb_context_st *ctx;
    VALUE rv, arg, options, transcoder, transcoder_opts;
    long ii;
    lcb_error_t err = LCB_SUCCESS;
    int is_replica = 0;
    lcb_CMDGET get = {0};
    lcb_CMDGETREPLICA getr = {0};

    if (!cb_bucket_connected_bang(bucket, cb_sym_get)) {
        return Qnil;
    }

    rb_scan_args(argc, argv, "11", &arg, &options);

    transcoder = bucket->transcoder;
    transcoder_opts = rb_hash_new();
    if (!NIL_P(options)) {
        Check_Type(options, T_HASH);
        VALUE tmp;
        tmp = rb_hash_lookup2(options, cb_sym_replica, Qundef);
        if (tmp != Qundef) {
            is_replica = 1;
            switch (TYPE(tmp)) {
            case T_FIXNUM: {
                int nr = NUM2INT(tmp);
                int max = lcb_get_num_replicas(bucket->handle);
                if (nr < 0 || nr >= max) {
                    cb_raise_msg(rb_eArgError, "replica index should be in interval 0...%d, given: %d", max, nr);
                }
                getr.strategy = LCB_REPLICA_SELECT;
                getr.index = nr;
            } break;
            case T_SYMBOL:
                if (tmp == cb_sym_all) {
                    getr.strategy = LCB_REPLICA_ALL;
                } else if (tmp == cb_sym_first) {
                    getr.strategy = LCB_REPLICA_FIRST;
                } else {
                    VALUE idstr = rb_sym2str(tmp);
                    cb_raise_msg(rb_eArgError, "unknown replica strategy: %s (expected :all, :first or replica index)",
                                 idstr ? RSTRING_PTR(idstr) : "(null)");
                }
                break;
            case T_TRUE:
                getr.strategy = LCB_REPLICA_FIRST;
                break;
            case T_FALSE:
                is_replica = 0;
                break;
            default:
                cb_raise_msg(rb_eArgError, "expected replica option to be index or :all/:first symbol (given type=%d)",
                             (int)TYPE(tmp));
                break;
            }
        }
        tmp = rb_hash_aref(options, cb_sym_ttl);
        if (tmp != Qnil) {
            if (is_replica) {
                cb_raise_msg2(rb_eArgError, "expiration option (:ttl) is not allowed for get-replica operation");
            }
            get.exptime = NUM2ULONG(tmp);
            if (get.exptime == 0) {
                get.cmdflags = LCB_CMDGET_F_CLEAREXP;
            }
        }
        tmp = rb_hash_aref(options, cb_sym_lock);
        if (tmp != Qnil) {
            if (is_replica) {
                cb_raise_msg2(rb_eArgError, ":lock option is not allowed for get-replica operation");
            }
            switch (TYPE(tmp)) {
            case T_FIXNUM:
                get.exptime = NUM2ULONG(tmp);
                /* fallthrough */
            case T_TRUE:
                get.lock = 1;
                break;
            case T_FALSE:
                get.lock = 0;
                break;
            default:
                cb_raise_msg(rb_eArgError,
                             "unexpected type for :lock option (expected boolean or number, but given type=%d)",
                             (int)TYPE(tmp));
                break;
            }
        }
        tmp = rb_hash_lookup2(options, cb_sym_format, Qundef);
        if (tmp != Qundef) {
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

    ctx = cb_context_alloc(bucket);
    ctx->operation = cb_sym_get;
    ctx->transcoder = transcoder;
    ctx->transcoder_opts = transcoder_opts;
    lcb_sched_enter(bucket->handle);
    switch (TYPE(arg)) {
    case T_HASH:
        if (is_replica) {
            cb_raise_msg2(rb_eArgError, "key/ttl Hash is not allowed for get-replica operation");
        } else {
            __cb_get_arg_i iarg = {0};
            iarg.handle = bucket->handle;
            iarg.cmd = &get;
            iarg.ctx = ctx;
            rb_hash_foreach(arg, cb_get_extract_pairs_i, (VALUE)&iarg);
            ctx->rv = rb_hash_new();
        }
        break;
    case T_ARRAY:
        for (ii = 0; ii < RARRAY_LEN(arg); ii++) {
            VALUE entry = rb_ary_entry(arg, ii);
            switch (TYPE(entry)) {
            case T_SYMBOL:
                arg = rb_sym2str(arg);
                /* fallthrough */
            case T_STRING:
                if (is_replica) {
                    LCB_CMD_SET_KEY(&getr, RSTRING_PTR(entry), RSTRING_LEN(entry));
                    err = lcb_rget3(bucket->handle, (const void *)ctx, &getr);
                } else {
                    LCB_CMD_SET_KEY(&get, RSTRING_PTR(entry), RSTRING_LEN(entry));
                    err = lcb_get3(bucket->handle, (const void *)ctx, &get);
                }
                if (err != LCB_SUCCESS) {
                    lcb_sched_fail(bucket->handle);
                    cb_context_free(ctx);
                    cb_raise2(cb_eLibraryError, err, "unable to schedule key for get operation");
                }
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
        if (is_replica) {
            LCB_CMD_SET_KEY(&getr, RSTRING_PTR(arg), RSTRING_LEN(arg));
            err = lcb_rget3(bucket->handle, (const void *)ctx, &getr);
        } else {
            LCB_CMD_SET_KEY(&get, RSTRING_PTR(arg), RSTRING_LEN(arg));
            err = lcb_get3(bucket->handle, (const void *)ctx, &get);
        }
        if (err != LCB_SUCCESS) {
            lcb_sched_fail(bucket->handle);
            cb_context_free(ctx);
            cb_raise2(cb_eLibraryError, err, "unable to schedule key for get operation");
        }
        ctx->rv = Qnil;
        break;
    default:
        lcb_sched_fail(bucket->handle);
        cb_context_free(ctx);
        cb_raise_msg(rb_eArgError, "expected array of keys, key/ttl pairs or single key (type=%d)", (int)TYPE(arg));
        break;
    }
    lcb_sched_leave(bucket->handle);

    lcb_wait(bucket->handle);
    rv = ctx->rv;
    cb_context_free(ctx);
    return rv;
}
