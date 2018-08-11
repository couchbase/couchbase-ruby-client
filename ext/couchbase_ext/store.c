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

static VALUE
storage_opcode_to_sym(lcb_storage_t operation)
{
    switch (operation) {
    case LCB_ADD:
        return cb_sym_add;
    case LCB_REPLACE:
        return cb_sym_replace;
    case LCB_SET:
        return cb_sym_set;
    case LCB_APPEND:
        return cb_sym_append;
    case LCB_PREPEND:
        return cb_sym_prepend;
    default:
        cb_raise_msg(cb_eLibraryError, "unexpected type of store operation: %d", (int)operation);
    }
}

void
cb_storage_callback(lcb_t handle, int cbtype, const lcb_RESPBASE *rb)
{
    const lcb_RESPSTORE *resp = (const lcb_RESPSTORE *)rb;
    struct cb_context_st *ctx = (struct cb_context_st *)rb->cookie;
    VALUE key, exc = Qnil, res;

    res = rb_class_new_instance(0, NULL, cb_cResult);
    key = rb_external_str_new(rb->key, rb->nkey);
    rb_ivar_set(res, cb_id_iv_key, key);
    rb_ivar_set(res, cb_id_iv_operation, ctx->operation);
    rb_ivar_set(res, cb_id_iv_cas, ULL2NUM(resp->cas));
    if (resp->rc != LCB_SUCCESS) {
        exc =
            cb_exc_new(cb_eLibraryError, rb->rc, "failed to store key: %.*s", (int)resp->nkey, (const char *)resp->key);
        rb_ivar_set(res, cb_id_iv_error, exc);
    }
    ctx->exception = exc;
    if (TYPE(ctx->rv) == T_HASH) {
        rb_hash_aset(ctx->rv, key, res);
    } else if (NIL_P(ctx->rv)) {
        ctx->rv = res;
    } else {
        // FIXME: use some kind of invalid state error
        cb_context_free(ctx);
        cb_raise_msg(cb_eLibraryError, "unexpected result container type: %d", (int)TYPE(ctx->rv));
    }
    (void)cbtype;
    (void)handle;
}

typedef struct __cb_store_arg_i {
    lcb_t handle;
    lcb_CMDSTOREDUR *cmd;
    struct cb_context_st *ctx;
    uint32_t *flags;
    VALUE transcoder;
    VALUE transcoder_opts;
    lcb_storage_t operation;
} __cb_store_arg_i;

static int
cb_store_extract_pairs_i(VALUE key, VALUE value, VALUE cookie)
{
    VALUE encoded;
    lcb_error_t err;
    __cb_store_arg_i *arg = (__cb_store_arg_i *)cookie;

    if (arg->operation == LCB_PREPEND || arg->operation == LCB_APPEND) {
        if (TYPE(value) != T_STRING) {
            lcb_sched_fail(arg->handle);
            cb_context_free(arg->ctx);
            cb_raise_msg(cb_eValueFormatError,
                         "unable to schedule operation for key \"%.*s\": string value required for prepend/append",
                         (int)RSTRING_LEN(key), (const char *)RSTRING_PTR(key));
        }
    } else {
        encoded = cb_encode_value(arg->transcoder, value, arg->flags, arg->transcoder_opts);
        if (TYPE(encoded) != T_STRING) {
            lcb_sched_fail(arg->handle);
            cb_context_free(arg->ctx);
            if (rb_obj_is_kind_of(encoded, rb_eStandardError)) {
                VALUE exc = cb_exc_new_msg(cb_eValueFormatError, "unable to encode value for key \"%.*s\"",
                                           (int)RSTRING_LEN(key), (const char *)RSTRING_PTR(key));
                rb_ivar_set(exc, cb_id_iv_inner_exception, encoded);
                rb_exc_raise(exc);
            } else {
                cb_raise_msg(cb_eValueFormatError, "unable to encode value for key \"%.*s\"", (int)RSTRING_LEN(key),
                             (const char *)RSTRING_PTR(key));
            }
        }
        value = encoded;
    }
    if (TYPE(key) == T_SYMBOL) {
        key = rb_sym2str(key);
    }
    LCB_CMD_SET_KEY(arg->cmd, RSTRING_PTR(key), RSTRING_LEN(key));
    LCB_CMD_SET_VALUE(arg->cmd, RSTRING_PTR(value), RSTRING_LEN(value));
    if (arg->cmd->persist_to || arg->cmd->replicate_to) {
        err = lcb_storedur3(arg->handle, (const void *)arg->ctx, arg->cmd);
    } else {
        err = lcb_store3(arg->handle, (const void *)arg->ctx, (lcb_CMDSTORE *)arg->cmd);
    }
    if (err != LCB_SUCCESS) {
        lcb_sched_fail(arg->handle);
        cb_context_free(arg->ctx);
        cb_raise2(cb_eLibraryError, err, "unable to schedule store request");
    }
    return ST_CONTINUE;
}

static inline VALUE
cb_bucket_store(lcb_storage_t operation, int argc, VALUE *argv, VALUE self)
{
    struct cb_bucket_st *bucket = DATA_PTR(self);
    struct cb_context_st *ctx;
    VALUE rv, key = Qnil, keys = Qnil, value = Qnil, options = Qnil;
    VALUE transcoder = Qnil, transcoder_opts = Qnil;
    lcb_error_t err;
    lcb_CMDSTOREDUR cmd = {0};

    if (!cb_bucket_connected_bang(bucket, storage_opcode_to_sym(operation))) {
        return Qnil;
    }

    rb_scan_args(argc, argv, "12", &key, &value, &options);

    switch (TYPE(key)) {
    case T_HASH:
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
    cmd.datatype = 0x00;
    cmd.exptime = bucket->default_ttl;
    cmd.persist_to = 0;
    cmd.replicate_to = 0;
    cmd.operation = operation;
    transcoder = bucket->transcoder;
    transcoder_opts = rb_hash_new();
    if (!NIL_P(options)) {
        VALUE tmp;
        Check_Type(options, T_HASH);
        tmp = rb_hash_aref(options, cb_sym_ttl);
        if (tmp != Qnil) {
            cmd.exptime = NUM2ULONG(tmp);
        }
        tmp = rb_hash_aref(options, cb_sym_cas);
        if (tmp != Qnil) {
            if (operation == LCB_ADD) {
                cb_raise_msg2(rb_eArgError, "CAS is not allowed for add operation");
            }
            cmd.cas = NUM2ULL(tmp);
        }
        tmp = rb_hash_aref(options, cb_sym_observe);
        if (tmp != Qnil) {
            VALUE obs;
            Check_Type(tmp, T_HASH);
            obs = rb_hash_aref(tmp, cb_sym_persisted);
            if (obs != Qnil) {
                Check_Type(obs, T_FIXNUM);
                cmd.persist_to = NUM2INT(obs);
            }
            obs = rb_hash_aref(tmp, cb_sym_replicated);
            if (obs != Qnil) {
                Check_Type(obs, T_FIXNUM);
                cmd.replicate_to = NUM2INT(obs);
            }
            if (cmd.persist_to == 0 && cmd.replicate_to == 0) {
                cb_raise_msg2(rb_eArgError, "either :persisted or :replicated option must be set");
            }
        }
        tmp = rb_hash_aref(options, cb_sym_format);
        if (tmp != Qnil) {
            transcoder = cb_get_transcoder(bucket, tmp, 1, transcoder_opts);
        }
        tmp = rb_hash_lookup2(options, cb_sym_transcoder, Qundef);
        if (tmp != Qundef) {
            transcoder = cb_get_transcoder(bucket, tmp, 0, transcoder_opts);
        }
    }
    ctx = cb_context_alloc(bucket);
    ctx->operation = storage_opcode_to_sym(operation);
    lcb_sched_enter(bucket->handle);
    if (NIL_P(keys)) {
        if (operation == LCB_PREPEND || operation == LCB_APPEND) {
            if (TYPE(value) != T_STRING) {
                lcb_sched_fail(bucket->handle);
                cb_context_free(ctx);
                cb_raise_msg(cb_eValueFormatError,
                             "unable to schedule operation for key \"%.*s\": string value required for prepend/append",
                             (int)RSTRING_LEN(key), (const char *)RSTRING_PTR(key));
            }
        } else {
            VALUE encoded;
            encoded = cb_encode_value(transcoder, value, &cmd.flags, transcoder_opts);
            if (TYPE(encoded) != T_STRING) {
                VALUE val;
                lcb_sched_fail(bucket->handle);
                cb_context_free(ctx);
                val = rb_any_to_s(encoded);
                if (rb_obj_is_kind_of(encoded, rb_eStandardError)) {
                    VALUE exc =
                        cb_exc_new_msg(cb_eValueFormatError, "unable to convert value for key \"%.*s\" to string: %.*s",
                                       (int)RSTRING_LEN(key), (const char *)RSTRING_PTR(key), (int)RSTRING_LEN(val),
                                       (const char *)RSTRING_PTR(val));
                    rb_ivar_set(exc, cb_id_iv_inner_exception, encoded);
                    rb_exc_raise(exc);
                } else {
                    cb_raise_msg(cb_eValueFormatError, "unable to convert value for key \"%.*s\" to string: %.*s",
                                 (int)RSTRING_LEN(key), (const char *)RSTRING_PTR(key), (int)RSTRING_LEN(val),
                                 (const char *)RSTRING_PTR(val));
                }
            }
            value = encoded;
        }
        ctx->rv = Qnil;
        LCB_CMD_SET_KEY(&cmd, RSTRING_PTR(key), RSTRING_LEN(key));
        LCB_CMD_SET_VALUE(&cmd, RSTRING_PTR(value), RSTRING_LEN(value));
        if (cmd.persist_to || cmd.replicate_to) {
            err = lcb_storedur3(bucket->handle, (const void *)ctx, &cmd);
        } else {
            err = lcb_store3(bucket->handle, (const void *)ctx, (lcb_CMDSTORE *)&cmd);
        }
        if (err != LCB_SUCCESS) {
            lcb_sched_fail(bucket->handle);
            cb_context_free(ctx);
            cb_raise2(cb_eLibraryError, err, "unable to schedule store request");
        }
    } else {
        __cb_store_arg_i iarg = {0};
        iarg.handle = bucket->handle;
        iarg.cmd = &cmd;
        iarg.ctx = ctx;
        iarg.transcoder = transcoder;
        iarg.transcoder_opts = transcoder_opts;
        iarg.flags = &cmd.flags;
        iarg.operation = operation;
        ctx->rv = rb_hash_new();
        rb_hash_foreach(keys, cb_store_extract_pairs_i, (VALUE)&iarg);
    }
    lcb_sched_leave(bucket->handle);
    if (err != LCB_SUCCESS) {
        cb_context_free(ctx);
        cb_raise2(cb_eLibraryError, err, "unable to schedule observe request");
    }
    lcb_wait(bucket->handle);
    rv = ctx->rv;
    cb_context_free(ctx);
    return rv;
}

/*
 * Unconditionally store the object in the Couchbase
 *
 * @since 1.0.0
 *
 * @overload set(key, value, options = {})
 *
 *   @param key [String, Symbol] Key used to reference the value.
 *   @param value [Object] Value to be stored
 *   @param options [Hash] Options for operation.
 *   @option options [Fixnum] :ttl (self.default_ttl) Expiry time for key.
 *     Values larger than 30*24*60*60 seconds (30 days) are interpreted as
 *     absolute times (from the epoch).
 *   @option options [Fixnum] :flags (self.default_flags) Flags for storage
 *     options. Flags are ignored by the server but preserved for use by the
 *     client. For more info see {Bucket#default_flags}.
 *   @option options [Symbol] :format (self.default_format) The
 *     representation for storing the value in the bucket. For more info see
 *     {Bucket#default_format}.
 *   @option options [Fixnum] :cas The CAS value for an object. This value is
 *     created on the server and is guaranteed to be unique for each value of
 *     a given key. This value is used to provide simple optimistic
 *     concurrency control when multiple clients or threads try to update an
 *     item simultaneously.
 *   @option options [Hash] :observe Apply persistence condition before
 *     returning result. When this option specified the library will observe
 *     given condition. See {Bucket#observe_and_wait}.
 *
 *   @return [Fixnum] The CAS value of the object.
 *
 *   @raise [Couchbase::Error::Connect] if connection closed (see {Bucket#reconnect}).
 *   @raise [Couchbase::Error::KeyExists] if the key already exists on the
 *     server.
 *   @raise [Couchbase::Error::ValueFormat] if the value cannot be serialized
 *     with chosen encoder, e.g. if you try to store the Hash in +:plain+
 *     mode.
 *   @raise [ArgumentError] when passing the block in synchronous mode
 *   @raise [Couchbase::Error::Timeout] if timeout interval for observe
 *     exceeds
 *
 *   @example Store the key which will be expired in 2 seconds using relative TTL.
 *     c.set("foo", "bar", :ttl => 2)
 *
 *   @example Perform multi-set operation. It takes a Hash store its keys/values into the bucket
 *     c.set("foo1" => "bar1", "foo2" => "bar2")
 *     #=> {"foo1" => cas1, "foo2" => cas2}
 *
 *   @example Store the key which will be expired in 2 seconds using absolute TTL.
 *     c.set("foo", "bar", :ttl => Time.now.to_i + 2)
 *
 *   @example Force JSON document format for value
 *     c.set("foo", {"bar" => "baz}, :format => :document)
 *
 *   @example Use hash-like syntax to store the value
 *     c["foo"] = {"bar" => "baz}
 *
 *   @example Use extended hash-like syntax
 *     c["foo", {:flags => 0x1000, :format => :plain}] = "bar"
 *     c["foo", :flags => 0x1000] = "bar"  # for ruby 1.9.x only
 *
 *   @example Set application specific flags (note that it will be OR-ed with format flags)
 *     c.set("foo", "bar", :flags => 0x1000)
 *
 *   @example Perform optimistic locking by specifying last known CAS version
 *     c.set("foo", "bar", :cas => 8835713818674332672)
 *
 *   @example Ensure that the key will be persisted at least on the one node
 *     c.set("foo", "bar", :observe => {:persisted => 1})
 */
VALUE
cb_bucket_set(int argc, VALUE *argv, VALUE self)
{
    return cb_bucket_store(LCB_SET, argc, argv, self);
}

/*
 * Add the item to the database, but fail if the object exists already
 *
 * @since 1.0.0
 *
 * @overload add(key, value, options = {})
 *
 *   @param key [String, Symbol] Key used to reference the value.
 *   @param value [Object] Value to be stored
 *   @param options [Hash] Options for operation.
 *   @option options [Fixnum] :ttl (self.default_ttl) Expiry time for key.
 *     Values larger than 30*24*60*60 seconds (30 days) are interpreted as
 *     absolute times (from the epoch).
 *   @option options [Fixnum] :flags (self.default_flags) Flags for storage
 *     options. Flags are ignored by the server but preserved for use by the
 *     client. For more info see {Bucket#default_flags}.
 *   @option options [Symbol] :format (self.default_format) The
 *     representation for storing the value in the bucket. For more info see
 *     {Bucket#default_format}.
 *   @option options [Fixnum] :cas The CAS value for an object. This value
 *     created on the server and is guaranteed to be unique for each value of
 *     a given key. This value is used to provide simple optimistic
 *     concurrency control when multiple clients or threads try to update an
 *     item simultaneously.
 *   @option options [Hash] :observe Apply persistence condition before
 *     returning result. When this option specified the library will observe
 *     given condition. See {Bucket#observe_and_wait}.
 *
 *   @return [Fixnum] The CAS value of the object.
 *
 *   @raise [Couchbase::Error::Connect] if connection closed (see {Bucket#reconnect})
 *   @raise [Couchbase::Error::KeyExists] if the key already exists on the
 *     server
 *   @raise [Couchbase::Error::ValueFormat] if the value cannot be serialized
 *     with chosen encoder, e.g. if you try to store the Hash in +:plain+
 *     mode.
 *   @raise [ArgumentError] when passing the block in synchronous mode
 *   @raise [Couchbase::Error::Timeout] if timeout interval for observe
 *     exceeds
 *
 *   @example Add the same key twice
 *     c.add("foo", "bar")  #=> stored successully
 *     c.add("foo", "baz")  #=> will raise Couchbase::Error::KeyExists: failed to store value (key="foo", error=0x0c)
 *
 *   @example Ensure that the key will be persisted at least on the one node
 *     c.add("foo", "bar", :observe => {:persisted => 1})
 */
VALUE
cb_bucket_add(int argc, VALUE *argv, VALUE self)
{
    return cb_bucket_store(LCB_ADD, argc, argv, self);
}

/*
 * Replace the existing object in the database
 *
 * @since 1.0.0
 *
 * @overload replace(key, value, options = {})
 *   @param key [String, Symbol] Key used to reference the value.
 *   @param value [Object] Value to be stored
 *   @param options [Hash] Options for operation.
 *   @option options [Fixnum] :ttl (self.default_ttl) Expiry time for key.
 *     Values larger than 30*24*60*60 seconds (30 days) are interpreted as
 *     absolute times (from the epoch).
 *   @option options [Fixnum] :flags (self.default_flags) Flags for storage
 *     options. Flags are ignored by the server but preserved for use by the
 *     client. For more info see {Bucket#default_flags}.
 *   @option options [Symbol] :format (self.default_format) The
 *     representation for storing the value in the bucket. For more info see
 *     {Bucket#default_format}.
 *   @option options [Fixnum] :cas The CAS value for an object. This value
 *     created on the server and is guaranteed to be unique for each value of
 *     a given key. This value is used to provide simple optimistic
 *     concurrency control when multiple clients or threads try to update an
 *     item simultaneously.
 *   @option options [Hash] :observe Apply persistence condition before
 *     returning result. When this option specified the library will observe
 *     given condition. See {Bucket#observe_and_wait}.
 *
 *   @return [Fixnum] The CAS value of the object.
 *
 *   @raise [Couchbase::Error::Connect] if connection closed (see {Bucket#reconnect})
 *   @raise [Couchbase::Error::NotFound] if the key doesn't exists
 *   @raise [Couchbase::Error::KeyExists] on CAS mismatch
 *   @raise [ArgumentError] when passing the block in synchronous mode
 *   @raise [Couchbase::Error::Timeout] if timeout interval for observe
 *     exceeds
 *
 *   @example Replacing missing key
 *     c.replace("foo", "baz")  #=> will raise Couchbase::Error::NotFound: failed to store value (key="foo", error=0x0d)
 *
 *   @example Ensure that the key will be persisted at least on the one node
 *     c.replace("foo", "bar", :observe => {:persisted => 1})
 */
VALUE
cb_bucket_replace(int argc, VALUE *argv, VALUE self)
{
    return cb_bucket_store(LCB_REPLACE, argc, argv, self);
}

/*
 * Append this object to the existing object
 *
 * @since 1.0.0
 *
 * @note This operation is kind of data-aware from server point of view.
 *   This mean that the server treats value as binary stream and just
 *   perform concatenation, therefore it won't work with +:marshal+ and
 *   +:document+ formats, because of lack of knowledge how to merge values
 *   in these formats. See {Bucket#cas} for workaround.
 *
 * @overload append(key, value, options = {})
 *   @param key [String, Symbol] Key used to reference the value.
 *   @param value [Object] Value to be stored
 *   @param options [Hash] Options for operation.
 *   @option options [Fixnum] :cas The CAS value for an object. This value
 *     created on the server and is guaranteed to be unique for each value of
 *     a given key. This value is used to provide simple optimistic
 *     concurrency control when multiple clients or threads try to update an
 *     item simultaneously.
 *   @option options [Symbol] :format (self.default_format) The
 *     representation for storing the value in the bucket. For more info see
 *     {Bucket#default_format}.
 *   @option options [Hash] :observe Apply persistence condition before
 *     returning result. When this option specified the library will observe
 *     given condition. See {Bucket#observe_and_wait}.
 *
 *   @return [Fixnum] The CAS value of the object.
 *
 *   @raise [Couchbase::Error::Connect] if connection closed (see {Bucket#reconnect})
 *   @raise [Couchbase::Error::KeyExists] on CAS mismatch
 *   @raise [Couchbase::Error::NotStored] if the key doesn't exist
 *   @raise [ArgumentError] when passing the block in synchronous mode
 *   @raise [Couchbase::Error::Timeout] if timeout interval for observe
 *     exceeds
 *
 *   @example Simple append
 *     c.set("foo", "aaa")
 *     c.append("foo", "bbb")
 *     c.get("foo")           #=> "aaabbb"
 *
 *   @example Implementing sets using append
 *     def set_add(key, *values)
 *       encoded = values.flatten.map{|v| "+#{v} "}.join
 *       append(key, encoded)
 *     end
 *
 *     def set_remove(key, *values)
 *       encoded = values.flatten.map{|v| "-#{v} "}.join
 *       append(key, encoded)
 *     end
 *
 *     def set_get(key)
 *       encoded = get(key)
 *       ret = Set.new
 *       encoded.split(' ').each do |v|
 *         op, val = v[0], v[1..-1]
 *         case op
 *         when "-"
 *           ret.delete(val)
 *         when "+"
 *           ret.add(val)
 *         end
 *       end
 *       ret
 *     end
 *
 *   @example Using optimistic locking. The operation will fail on CAS mismatch
 *     ver = c.set("foo", "aaa")
 *     c.append("foo", "bbb", :cas => ver)
 *
 *   @example Ensure that the key will be persisted at least on the one node
 *     c.append("foo", "bar", :observe => {:persisted => 1})
 */
VALUE
cb_bucket_append(int argc, VALUE *argv, VALUE self)
{
    return cb_bucket_store(LCB_APPEND, argc, argv, self);
}

/*
 * Prepend this object to the existing object
 *
 * @since 1.0.0
 *
 * @note This operation is kind of data-aware from server point of view.
 *   This mean that the server treats value as binary stream and just
 *   perform concatenation, therefore it won't work with +:marshal+ and
 *   +:document+ formats, because of lack of knowledge how to merge values
 *   in these formats. See {Bucket#cas} for workaround.
 *
 * @overload prepend(key, value, options = {})
 *   @param key [String, Symbol] Key used to reference the value.
 *   @param value [Object] Value to be stored
 *   @param options [Hash] Options for operation.
 *   @option options [Fixnum] :cas The CAS value for an object. This value
 *     created on the server and is guaranteed to be unique for each value of
 *     a given key. This value is used to provide simple optimistic
 *     concurrency control when multiple clients or threads try to update an
 *     item simultaneously.
 *   @option options [Symbol] :format (self.default_format) The
 *     representation for storing the value in the bucket. For more info see
 *     {Bucket#default_format}.
 *   @option options [Hash] :observe Apply persistence condition before
 *     returning result. When this option specified the library will observe
 *     given condition. See {Bucket#observe_and_wait}.
 *
 *   @raise [Couchbase::Error::Connect] if connection closed (see {Bucket#reconnect})
 *   @raise [Couchbase::Error::KeyExists] on CAS mismatch
 *   @raise [Couchbase::Error::NotStored] if the key doesn't exist
 *   @raise [ArgumentError] when passing the block in synchronous mode
 *   @raise [Couchbase::Error::Timeout] if timeout interval for observe
 *     exceeds
 *
 *   @example Simple prepend example
 *     c.set("foo", "aaa")
 *     c.prepend("foo", "bbb")
 *     c.get("foo")           #=> "bbbaaa"
 *
 *   @example Using explicit format option
 *     c.default_format       #=> :document
 *     c.set("foo", {"y" => "z"})
 *     c.prepend("foo", '[', :format => :plain)
 *     c.append("foo", ', {"z": "y"}]', :format => :plain)
 *     c.get("foo")           #=> [{"y"=>"z"}, {"z"=>"y"}]
 *
 *   @example Using optimistic locking. The operation will fail on CAS mismatch
 *     ver = c.set("foo", "aaa")
 *     c.prepend("foo", "bbb", :cas => ver)
 *
 *   @example Ensure that the key will be persisted at least on the one node
 *     c.prepend("foo", "bar", :observe => {:persisted => 1})
 */
VALUE
cb_bucket_prepend(int argc, VALUE *argv, VALUE self)
{
    return cb_bucket_store(LCB_PREPEND, argc, argv, self);
}

VALUE
cb_bucket_aset(int argc, VALUE *argv, VALUE self)
{
    VALUE temp;

    if (argc == 3) {
        /* swap opts and value, because value goes last for []= */
        temp = argv[2];
        argv[2] = argv[1];
        argv[1] = temp;
    }
    return cb_bucket_set(argc, argv, self);
}
