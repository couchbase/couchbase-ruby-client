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

#define _alloc_data_for_s(type, _type, size, items, ptr) do {\
    lcb_size_t ii; \
    \
    params->cmd.type.num = size; \
    params->cmd.type.items = calloc(size, sizeof(_type)); \
    if (params->cmd.type.items == NULL) { \
        rb_raise(cb_eClientNoMemoryError, "failed to allocate memory for arguments"); \
    } \
    params->cmd.type.ptr = calloc(size, sizeof(_type *)); \
    if (params->cmd.type.ptr == NULL) { \
        rb_raise(cb_eClientNoMemoryError, "failed to allocate memory for arguments"); \
    } \
    for (ii = 0; ii < size; ++ii) { \
        params->cmd.type.ptr[ii] = params->cmd.type.items + ii; \
    } \
} while(0)

#define _alloc_data_for(type, _type) _alloc_data_for_s(type, _type, size, items, ptr)


#define _release_data_for_s(type, items, ptr) \
    free(params->cmd.type.items); \
    free(params->cmd.type.ptr);

#define _release_data_for(type) _release_data_for_s(type, items, ptr)

    static VALUE
get_transcoder(struct cb_bucket_st *bucket, VALUE override, int compat, VALUE opts)
{
    VALUE ret = Qundef;

    /* override with symbol */
    if (TYPE(override) == T_SYMBOL) {
        if (override == cb_sym_document) {
            ret = cb_mDocument;
        } else if (override == cb_sym_marshal) {
            ret = cb_mMarshal;
        } else if (override == cb_sym_plain) {
            ret = cb_mPlain;
        }
    } else if (!compat) {
        /* override with transcoder */
        if (rb_respond_to(override, cb_id_dump)
                && rb_respond_to(override, cb_id_load)) {
            ret = override;
        }
        /* nil is also valid */
        if (NIL_P(override)) {
            ret = Qnil;
        }
    }
    if (ret == Qundef) {
        return bucket->transcoder;
    } else {
        rb_hash_aset(opts, cb_sym_forced, Qtrue);
        return ret;
    }
}

/* TOUCH */

    static void
cb_params_touch_alloc(struct cb_params_st *params, lcb_size_t size)
{
    _alloc_data_for(touch, lcb_touch_cmd_t);
}

    static void
cb_params_touch_init_item(struct cb_params_st *params, lcb_size_t idx, VALUE key_obj, lcb_time_t exptime)
{
    key_obj = cb_unify_key(params->bucket, key_obj, 1);
    rb_ary_push(params->ensurance, key_obj);
    params->cmd.touch.items[idx].v.v0.key = RSTRING_PTR(key_obj);
    params->cmd.touch.items[idx].v.v0.nkey = RSTRING_LEN(key_obj);
    params->cmd.touch.items[idx].v.v0.exptime = exptime;
    params->npayload += RSTRING_LEN(key_obj) + sizeof(exptime);
}

    static int
cb_params_touch_extract_keys_i(VALUE key, VALUE value, VALUE arg)
{
    struct cb_params_st *params = (struct cb_params_st *)arg;
    cb_params_touch_init_item(params, params->idx++, key, NUM2ULONG(value));
    return ST_CONTINUE;
}

    static void
cb_params_touch_parse_options(struct cb_params_st *params, VALUE options)
{
    VALUE tmp;

    if (NIL_P(options)) {
        return;
    }
    tmp = rb_hash_aref(options, cb_sym_ttl);
    if (tmp != Qnil) {
        params->cmd.touch.ttl = NUM2ULONG(tmp);
    }
    tmp = rb_hash_lookup2(options, cb_sym_quiet, Qundef);
    if (tmp != Qundef) {
        params->cmd.touch.quiet = RTEST(tmp);
    }
}

    static void
cb_params_touch_parse_arguments(struct cb_params_st *params, int argc, VALUE argv)
{
    lcb_size_t ii;

    if (argc < 1) {
        rb_raise(rb_eArgError, "must be at least one key");
    }
    if (argc == 1) {
        VALUE keys = RARRAY_PTR(argv)[0];
        switch(TYPE(keys)) {
            case T_ARRAY:
                /* array of keys as a first argument */
                params->cmd.touch.array = 1;
                cb_params_touch_alloc(params, RARRAY_LEN(keys));
                for (ii = 0; ii < params->cmd.touch.num; ++ii) {
                    cb_params_touch_init_item(params, ii, RARRAY_PTR(keys)[ii], params->cmd.touch.ttl);
                }
                break;
            case T_HASH:
                /* key-ttl pairs */
                cb_params_touch_alloc(params, RHASH_SIZE(keys));
                rb_hash_foreach(keys, cb_params_touch_extract_keys_i,
                        (VALUE)params);
                break;
            default:
                /* single key */
                cb_params_touch_alloc(params, 1);
                cb_params_touch_init_item(params, 0, keys, params->cmd.touch.ttl);
        }
    } else {
        /* just list of arguments */
        cb_params_touch_alloc(params, argc);
        for (ii = 0; ii < params->cmd.touch.num; ++ii) {
            cb_params_touch_init_item(params, ii, RARRAY_PTR(argv)[ii], params->cmd.touch.ttl);
        }
    }
}


/* REMOVE */

    static void
cb_params_remove_alloc(struct cb_params_st *params, lcb_size_t size)
{
    _alloc_data_for(remove, lcb_remove_cmd_t);
}

    static void
cb_params_remove_init_item(struct cb_params_st *params, lcb_size_t idx, VALUE key_obj, lcb_cas_t cas)
{
    key_obj = cb_unify_key(params->bucket, key_obj, 1);
    rb_ary_push(params->ensurance, key_obj);
    params->cmd.remove.items[idx].v.v0.key = RSTRING_PTR(key_obj);
    params->cmd.remove.items[idx].v.v0.nkey = RSTRING_LEN(key_obj);
    params->cmd.remove.items[idx].v.v0.cas = cas;
    params->npayload += RSTRING_LEN(key_obj);
}

    static int
cb_params_remove_extract_keys_i(VALUE key, VALUE value, VALUE arg)
{
    struct cb_params_st *params = (struct cb_params_st *)arg;
    cb_params_remove_init_item(params, params->idx++, key, NUM2ULL(value));
    return ST_CONTINUE;
}

    static void
cb_params_remove_parse_options(struct cb_params_st *params, VALUE options)
{
    VALUE tmp;

    if (NIL_P(options)) {
        return;
    }
    tmp = rb_hash_lookup2(options, cb_sym_quiet, Qundef);
    if (tmp != Qundef) {
        params->cmd.remove.quiet = RTEST(tmp);
    }
    tmp = rb_hash_aref(options, cb_sym_cas);
    if (tmp != Qnil) {
        params->cmd.remove.cas = NUM2ULL(tmp);
    }
}

    static void
cb_params_remove_parse_arguments(struct cb_params_st *params, int argc, VALUE argv)
{
    lcb_size_t ii;

    if (argc < 1) {
        rb_raise(rb_eArgError, "must be at least one key");
    }
    if (argc == 1) {
        VALUE keys = RARRAY_PTR(argv)[0];
        switch(TYPE(keys)) {
            case T_ARRAY:
                /* array of keys as a first argument */
                params->cmd.remove.array = 1;
                cb_params_remove_alloc(params, RARRAY_LEN(keys));
                for (ii = 0; ii < params->cmd.remove.num; ++ii) {
                    cb_params_remove_init_item(params, ii, RARRAY_PTR(keys)[ii], params->cmd.remove.cas);
                }
                break;
            case T_HASH:
                /* key-cas pairs */
                cb_params_remove_alloc(params, RHASH_SIZE(keys));
                rb_hash_foreach(keys, cb_params_remove_extract_keys_i,
                        (VALUE)params);
                break;
            default:
                /* single key */
                cb_params_remove_alloc(params, 1);
                cb_params_remove_init_item(params, 0, keys, params->cmd.remove.cas);
        }
    } else {
        /* just list of arguments */
        cb_params_remove_alloc(params, argc);
        for (ii = 0; ii < params->cmd.remove.num; ++ii) {
            cb_params_remove_init_item(params, ii, RARRAY_PTR(argv)[ii], params->cmd.remove.cas);
        }
    }
}


/* STORE */
    static void
cb_params_store_alloc(struct cb_params_st *params, lcb_size_t size)
{
    _alloc_data_for(store, lcb_store_cmd_t);
}

    static void
cb_params_store_init_item(struct cb_params_st *params, lcb_size_t idx,
        VALUE key_obj, VALUE value_obj, lcb_uint32_t flags, lcb_cas_t cas,
        lcb_time_t exptime)
{
    key_obj = cb_unify_key(params->bucket, key_obj, 1);
    value_obj = cb_encode_value(params->cmd.store.transcoder, value_obj, &flags, params->cmd.store.transcoder_opts);
    if (rb_obj_is_kind_of(value_obj, rb_eStandardError)) {
        VALUE exc_str = rb_funcall(value_obj, cb_id_to_s, 0);
        VALUE msg = rb_funcall(rb_mKernel, cb_id_sprintf, 3,
                rb_str_new2("unable to convert value for key \"%s\": %s"), key_obj, exc_str);
        VALUE exc = rb_exc_new3(cb_eValueFormatError, msg);
        rb_ivar_set(exc, cb_id_iv_inner_exception, value_obj);
        rb_exc_raise(exc);
    }
    /* the value must be string after conversion */
    if (TYPE(value_obj) != T_STRING) {
        VALUE val = rb_any_to_s(value_obj);
        rb_raise(cb_eValueFormatError, "unable to convert value for key \"%s\" to string: %s", RSTRING_PTR(key_obj), RSTRING_PTR(val));
    }
    rb_ary_push(params->ensurance, key_obj);
    rb_ary_push(params->ensurance, value_obj);
    params->cmd.store.items[idx].v.v0.datatype = params->cmd.store.datatype;
    params->cmd.store.items[idx].v.v0.operation = params->cmd.store.operation;
    params->cmd.store.items[idx].v.v0.key = RSTRING_PTR(key_obj);
    params->cmd.store.items[idx].v.v0.nkey = RSTRING_LEN(key_obj);
    params->cmd.store.items[idx].v.v0.bytes = RSTRING_PTR(value_obj);
    params->cmd.store.items[idx].v.v0.nbytes = RSTRING_LEN(value_obj);
    params->cmd.store.items[idx].v.v0.flags = flags;
    params->cmd.store.items[idx].v.v0.cas = cas;
    params->cmd.store.items[idx].v.v0.exptime = exptime;
    params->npayload += RSTRING_LEN(key_obj) + RSTRING_LEN(value_obj) + sizeof(flags) + sizeof(exptime);
}

    static int
cb_params_store_extract_keys_i(VALUE key, VALUE value, VALUE arg)
{
    struct cb_params_st *params = (struct cb_params_st *)arg;
    cb_params_store_init_item(params, params->idx++, key, value,
            params->cmd.store.flags, 0, params->cmd.store.ttl);
    return ST_CONTINUE;
}

    static void
cb_params_store_parse_options(struct cb_params_st *params, VALUE options)
{
    VALUE tmp;

    if (NIL_P(options)) {
        return;
    }
    tmp = rb_hash_aref(options, cb_sym_flags);
    if (tmp != Qnil) {
        params->cmd.store.flags = (lcb_uint32_t)NUM2ULONG(tmp);
    }
    tmp = rb_hash_aref(options, cb_sym_ttl);
    if (tmp != Qnil) {
        params->cmd.store.ttl = NUM2ULONG(tmp);
    }
    tmp = rb_hash_aref(options, cb_sym_cas);
    if (tmp != Qnil) {
        params->cmd.store.cas = NUM2ULL(tmp);
    }
    tmp = rb_hash_aref(options, cb_sym_observe);
    if (tmp != Qnil) {
        Check_Type(tmp, T_HASH);
        rb_funcall(params->bucket->self, cb_id_verify_observe_options, 1, tmp);
        params->cmd.store.observe = tmp;
    }
    tmp = rb_hash_aref(options, cb_sym_format);
    if (tmp != Qnil) {
        params->cmd.store.transcoder = get_transcoder(params->bucket,
                tmp, 1, params->cmd.store.transcoder_opts);
    }
    tmp = rb_hash_lookup2(options, cb_sym_transcoder, Qundef);
    if (tmp != Qundef) {
        params->cmd.store.transcoder = get_transcoder(params->bucket,
                tmp, 0, params->cmd.store.transcoder_opts);
    }
}

    static void
cb_params_store_parse_arguments(struct cb_params_st *params, int argc, VALUE argv)
{
    VALUE keys;

    if (argc < 1) {
        rb_raise(rb_eArgError, "the key and value must be specified");
    }
    switch (argc) {
        case 1:
            keys = RARRAY_PTR(argv)[0];
            switch(TYPE(keys)) {
                case T_HASH:
                    /* key-value pairs */
                    cb_params_store_alloc(params, RHASH_SIZE(keys));
                    rb_hash_foreach(keys, cb_params_store_extract_keys_i,
                            (VALUE)params);
                    break;
                default:
                    rb_raise(rb_eArgError, "there must be either Hash with key-value pairs"
                            " or two separate arguments: key and value");
            }
            break;
        case 2:
            /* just key and value */
            cb_params_store_alloc(params, 1);
            cb_params_store_init_item(params, 0, RARRAY_PTR(argv)[0], RARRAY_PTR(argv)[1],
                    params->cmd.store.flags, params->cmd.store.cas, params->cmd.store.ttl);
            break;
        default:
            rb_raise(rb_eArgError, "too many arguments");
    }
}


/* GET */
    static void
cb_params_get_alloc(struct cb_params_st *params, lcb_size_t size)
{
    if (RTEST(params->cmd.get.replica)) {
        _alloc_data_for_s(get, lcb_get_replica_cmd_t, size, items_gr, ptr_gr);
    } else {
        _alloc_data_for(get, lcb_get_cmd_t);
    }
}

    static void
cb_params_get_init_item(struct cb_params_st *params, lcb_size_t idx,
        VALUE key_obj, lcb_time_t exptime)
{
    key_obj = cb_unify_key(params->bucket, key_obj, 1);
    rb_ary_push(params->ensurance, key_obj);
    if (RTEST(params->cmd.get.replica)) {
        params->cmd.get.items_gr[idx].version = 1;
        params->cmd.get.items_gr[idx].v.v1.key = RSTRING_PTR(key_obj);
        params->cmd.get.items_gr[idx].v.v1.nkey = RSTRING_LEN(key_obj);
        if (params->cmd.get.replica == cb_sym_first || params->cmd.get.replica == Qtrue) {
            params->cmd.get.items_gr[idx].v.v1.strategy = LCB_REPLICA_FIRST;
        } else if (params->cmd.get.replica == cb_sym_all) {
            params->cmd.get.items_gr[idx].v.v1.strategy = LCB_REPLICA_ALL;
        } else {
            params->cmd.get.items_gr[idx].v.v1.strategy = LCB_REPLICA_SELECT;
            params->cmd.get.items_gr[idx].v.v1.index = FIX2INT(params->cmd.get.replica);
        }
    } else {
        params->cmd.get.items[idx].v.v0.key = RSTRING_PTR(key_obj);
        params->cmd.get.items[idx].v.v0.nkey = RSTRING_LEN(key_obj);
        params->cmd.get.items[idx].v.v0.exptime = exptime;
        params->cmd.get.items[idx].v.v0.lock = params->cmd.get.lock;
        params->npayload += sizeof(exptime);
    }
    params->npayload += RSTRING_LEN(key_obj);
}

    static int
cb_params_get_extract_keys_i(VALUE key, VALUE value, VALUE arg)
{
    struct cb_params_st *params = (struct cb_params_st *)arg;
    rb_ary_push(params->cmd.get.keys_ary, key);
    cb_params_get_init_item(params, params->idx++, key, NUM2ULONG(value));
    return ST_CONTINUE;
}

    static void
cb_params_get_parse_options(struct cb_params_st *params, VALUE options)
{
    VALUE tmp;

    if (NIL_P(options)) {
        return;
    }
    tmp = rb_hash_aref(options, cb_sym_replica);
    if (tmp == Qtrue || tmp == cb_sym_all || tmp == cb_sym_first) {
        params->cmd.get.replica = tmp;
    } else if (TYPE(tmp) == T_FIXNUM) {
        int nr = NUM2INT(tmp);
        int max = lcb_get_num_replicas(params->bucket->handle);
        if (nr < 0 || nr >= max) {
            rb_raise(rb_eArgError, "replica index should be in interval 0...%d", max);
        }
        params->cmd.get.replica = tmp;
    }
    params->cmd.get.extended = RTEST(rb_hash_aref(options, cb_sym_extended));
    params->cmd.get.assemble_hash = RTEST(rb_hash_aref(options, cb_sym_assemble_hash));
    tmp = rb_hash_lookup2(options, cb_sym_quiet, Qundef);
    if (tmp != Qundef) {
        params->cmd.get.quiet = RTEST(tmp);
    }
    tmp = rb_hash_aref(options, cb_sym_format);
    if (tmp != Qnil) {
        params->cmd.get.transcoder = get_transcoder(params->bucket,
                tmp, 1, params->cmd.get.transcoder_opts);
    }
    tmp = rb_hash_lookup2(options, cb_sym_transcoder, Qundef);
    if (tmp != Qundef) {
        params->cmd.get.transcoder = get_transcoder(params->bucket,
                tmp, 0, params->cmd.get.transcoder_opts);
    }
    tmp = rb_hash_aref(options, cb_sym_ttl);
    if (tmp != Qnil) {
        params->cmd.get.ttl = NUM2ULONG(tmp);
    }
    /* boolean or number of seconds to lock */
    tmp = rb_hash_aref(options, cb_sym_lock);
    if (tmp != Qnil) {
        params->cmd.get.lock = RTEST(tmp);
        if (TYPE(tmp) == T_FIXNUM) {
            params->cmd.get.ttl = NUM2ULONG(tmp);
        }
    }
}

    static void
cb_params_get_parse_arguments(struct cb_params_st *params, int argc, VALUE argv)
{
    lcb_size_t ii;

    if (argc < 1) {
        rb_raise(rb_eArgError, "must be at least one key");
    }
    if (argc == 1) {
        VALUE keys = RARRAY_PTR(argv)[0];
        switch(TYPE(keys)) {
            case T_ARRAY:
                /* array of keys as a first argument */
                params->cmd.get.array = 1;
                cb_params_get_alloc(params, RARRAY_LEN(keys));
                for (ii = 0; ii < params->cmd.get.num; ++ii) {
                    rb_ary_push(params->cmd.get.keys_ary, RARRAY_PTR(keys)[ii]);
                    cb_params_get_init_item(params, ii, RARRAY_PTR(keys)[ii], params->cmd.get.ttl);
                }
                break;
            case T_HASH:
                /* key-ttl pairs */
                if (params->cmd.get.replica) {
                    rb_raise(rb_eArgError, "must be either list of key or single key");
                }
                params->cmd.get.gat = 1;
                cb_params_get_alloc(params, RHASH_SIZE(keys));
                rb_hash_foreach(keys, cb_params_get_extract_keys_i, (VALUE)params);
                break;
            default:
                /* single key */
                cb_params_get_alloc(params, 1);
                rb_ary_push(params->cmd.get.keys_ary, keys);
                cb_params_get_init_item(params, 0, keys, params->cmd.get.ttl);
        }
    } else {
        /* just list of arguments */
        cb_params_get_alloc(params, argc);
        for (ii = 0; ii < params->cmd.get.num; ++ii) {
            rb_ary_push(params->cmd.get.keys_ary, RARRAY_PTR(argv)[ii]);
            cb_params_get_init_item(params, ii, RARRAY_PTR(argv)[ii], params->cmd.get.ttl);
        }
    }
}


/* ARITH */
    static void
cb_params_arith_alloc(struct cb_params_st *params, lcb_size_t size)
{
    _alloc_data_for(arith, lcb_arithmetic_cmd_t);
}

    static void
cb_params_arith_init_item(struct cb_params_st *params, lcb_size_t idx,
        VALUE key_obj, lcb_int64_t delta)
{
    key_obj = cb_unify_key(params->bucket, key_obj, 1);
    rb_ary_push(params->ensurance, key_obj);
    params->cmd.arith.items[idx].v.v0.key = RSTRING_PTR(key_obj);
    params->cmd.arith.items[idx].v.v0.nkey = RSTRING_LEN(key_obj);
    params->cmd.arith.items[idx].v.v0.delta = delta * params->cmd.arith.sign;
    params->cmd.arith.items[idx].v.v0.exptime = params->cmd.arith.ttl;
    params->cmd.arith.items[idx].v.v0.create = params->cmd.arith.create;
    params->cmd.arith.items[idx].v.v0.initial = params->cmd.arith.initial;
    params->npayload += RSTRING_LEN(key_obj);
}

    static int
cb_params_arith_extract_keys_i(VALUE key, VALUE value, VALUE arg)
{
    struct cb_params_st *params = (struct cb_params_st *)arg;
    cb_params_arith_init_item(params, params->idx++, key, NUM2ULONG(value) & INT64_MAX);
    return ST_CONTINUE;
}

    static void
cb_params_arith_parse_options(struct cb_params_st *params, VALUE options)
{
    VALUE tmp;

    if (NIL_P(options)) {
        return;
    }
    tmp = rb_hash_aref(options, cb_sym_create);
    if (tmp != Qnil) {
        params->cmd.arith.create = RTEST(tmp);
    }
    params->cmd.arith.extended = RTEST(rb_hash_aref(options, cb_sym_extended));
    tmp = rb_hash_aref(options, cb_sym_ttl);
    if (tmp != Qnil) {
        params->cmd.arith.ttl = NUM2ULONG(tmp);
    }
    tmp = rb_hash_aref(options, cb_sym_initial);
    if (tmp != Qnil) {
        params->cmd.arith.initial = NUM2ULL(tmp);
        params->cmd.arith.create = 1;
    }
    tmp = rb_hash_aref(options, cb_sym_delta);
    if (tmp != Qnil) {
        params->cmd.arith.delta = NUM2ULL(tmp) & INT64_MAX;
    }
    tmp = rb_hash_aref(options, cb_sym_format);
    if (tmp != Qnil) {
        params->cmd.arith.transcoder = get_transcoder(params->bucket,
                tmp, 1, params->cmd.arith.transcoder_opts);
    }
    tmp = rb_hash_lookup2(options, cb_sym_transcoder, Qundef);
    if (tmp != Qundef) {
        params->cmd.arith.transcoder = get_transcoder(params->bucket,
                tmp, 0, params->cmd.arith.transcoder_opts);
    }
}

    static void
cb_params_arith_parse_arguments(struct cb_params_st *params, int argc, VALUE argv)
{
    lcb_size_t ii;

    if (argc < 1) {
        rb_raise(rb_eArgError, "must be at least one key");
    }
    if (argc == 1) {
        VALUE keys = RARRAY_PTR(argv)[0];
        switch(TYPE(keys)) {
            case T_ARRAY:
                /* array of keys as a first argument */
                params->cmd.arith.array = 1;
                cb_params_arith_alloc(params, RARRAY_LEN(keys));
                for (ii = 0; ii < params->cmd.arith.num; ++ii) {
                    cb_params_arith_init_item(params, ii, RARRAY_PTR(keys)[ii], params->cmd.arith.delta);
                }
                break;
            case T_HASH:
                /* key-delta pairs */
                cb_params_arith_alloc(params, RHASH_SIZE(keys));
                rb_hash_foreach(keys, cb_params_arith_extract_keys_i, (VALUE)params);
                break;
            default:
                /* single key */
                cb_params_arith_alloc(params, 1);
                cb_params_arith_init_item(params, 0, keys, params->cmd.arith.delta);
        }
    } else {
        /* just list of arguments */
        cb_params_arith_alloc(params, argc);
        for (ii = 0; ii < params->cmd.arith.num; ++ii) {
            cb_params_arith_init_item(params, ii, RARRAY_PTR(argv)[ii], params->cmd.arith.delta);
        }
    }
}


/* STATS */
    static void
cb_params_stats_alloc(struct cb_params_st *params, lcb_size_t size)
{
    _alloc_data_for(stats, lcb_server_stats_cmd_t);
}

    static void
cb_params_stats_init_item(struct cb_params_st *params, lcb_size_t idx,
        VALUE key_obj)
{
    key_obj = cb_unify_key(params->bucket, key_obj, 1);
    rb_ary_push(params->ensurance, key_obj);
    params->cmd.stats.items[idx].v.v0.name = RSTRING_PTR(key_obj);
    params->cmd.stats.items[idx].v.v0.nname = RSTRING_LEN(key_obj);
    params->npayload += RSTRING_LEN(key_obj);
}

    static void
cb_params_stats_parse_arguments(struct cb_params_st *params, int argc, VALUE argv)
{
    lcb_size_t ii;

    if (argc == 1) {
        VALUE keys = RARRAY_PTR(argv)[0];
        switch(TYPE(keys)) {
            case T_ARRAY:
                /* array of keys as a first argument */
                params->cmd.stats.array = 1;
                cb_params_stats_alloc(params, RARRAY_LEN(keys));
                for (ii = 0; ii < params->cmd.stats.num; ++ii) {
                    cb_params_stats_init_item(params, ii, RARRAY_PTR(keys)[ii]);
                }
                break;
            default:
                /* single key */
                cb_params_stats_alloc(params, 1);
                cb_params_stats_init_item(params, 0, keys);
        }
    } else if (argc == 0) {
        /* stat without argument (single empty struct) */
        cb_params_stats_alloc(params, 1);
    } else {
        /* just list of arguments */
        cb_params_stats_alloc(params, argc);
        for (ii = 0; ii < params->cmd.stats.num; ++ii) {
            cb_params_stats_init_item(params, ii, RARRAY_PTR(argv)[ii]);
        }
    }
}


/* REMOVE */

    static void
cb_params_observe_alloc(struct cb_params_st *params, lcb_size_t size)
{
    _alloc_data_for(observe, lcb_observe_cmd_t);
}

    static void
cb_params_observe_init_item(struct cb_params_st *params, lcb_size_t idx, VALUE key_obj)
{
    key_obj = cb_unify_key(params->bucket, key_obj, 1);
    rb_ary_push(params->ensurance, key_obj);
    params->cmd.observe.items[idx].v.v0.key = RSTRING_PTR(key_obj);
    params->cmd.observe.items[idx].v.v0.nkey = RSTRING_LEN(key_obj);
    params->npayload += RSTRING_LEN(key_obj);
}

    static void
cb_params_observe_parse_arguments(struct cb_params_st *params, int argc, VALUE argv)
{
    lcb_size_t ii;

    if (argc < 1) {
        rb_raise(rb_eArgError, "must be at least one key");
    }
    if (argc == 1) {
        VALUE keys = RARRAY_PTR(argv)[0];
        switch(TYPE(keys)) {
            case T_ARRAY:
                /* array of keys as a first argument */
                params->cmd.observe.array = 1;
                cb_params_observe_alloc(params, RARRAY_LEN(keys));
                for (ii = 0; ii < params->cmd.observe.num; ++ii) {
                    cb_params_observe_init_item(params, ii, RARRAY_PTR(keys)[ii]);
                }
                break;
            default:
                /* single key */
                cb_params_observe_alloc(params, 1);
                cb_params_observe_init_item(params, 0, keys);
        }
    } else {
        /* just list of arguments */
        cb_params_observe_alloc(params, argc);
        for (ii = 0; ii < params->cmd.observe.num; ++ii) {
            cb_params_observe_init_item(params, ii, RARRAY_PTR(argv)[ii]);
        }
    }
}


/* UNLOCK */
    static void
cb_params_unlock_alloc(struct cb_params_st *params, lcb_size_t size)
{
    _alloc_data_for(unlock, lcb_unlock_cmd_t);
}

    static void
cb_params_unlock_init_item(struct cb_params_st *params, lcb_size_t idx, VALUE key_obj, lcb_cas_t cas)
{
    key_obj = cb_unify_key(params->bucket, key_obj, 1);
    rb_ary_push(params->ensurance, key_obj);
    params->cmd.unlock.items[idx].v.v0.key = RSTRING_PTR(key_obj);
    params->cmd.unlock.items[idx].v.v0.nkey = RSTRING_LEN(key_obj);
    params->cmd.unlock.items[idx].v.v0.cas = cas;
    params->npayload += RSTRING_LEN(key_obj);
}

    static int
cb_params_unlock_extract_keys_i(VALUE key, VALUE value, VALUE arg)
{
    struct cb_params_st *params = (struct cb_params_st *)arg;
    cb_params_unlock_init_item(params, params->idx++, key, NUM2ULL(value));
    return ST_CONTINUE;
}

    static void
cb_params_unlock_parse_options(struct cb_params_st *params, VALUE options)
{
    VALUE tmp;

    if (NIL_P(options)) {
        return;
    }
    tmp = rb_hash_aref(options, cb_sym_cas);
    if (tmp != Qnil) {
        params->cmd.unlock.cas = NUM2ULL(tmp);
    }
    tmp = rb_hash_lookup2(options, cb_sym_quiet, Qundef);
    if (tmp != Qundef) {
        params->cmd.unlock.quiet = RTEST(tmp);
    }
}

    static void
cb_params_unlock_parse_arguments(struct cb_params_st *params, int argc, VALUE argv)
{
    if (argc == 1) {
        VALUE keys = RARRAY_PTR(argv)[0];
        switch(TYPE(keys)) {
            case T_HASH:
                /* key-cas pairs */
                cb_params_unlock_alloc(params, RHASH_SIZE(keys));
                rb_hash_foreach(keys, cb_params_unlock_extract_keys_i, (VALUE)params);
                break;
            default:
                /* single key */
                cb_params_unlock_alloc(params, 1);
                cb_params_unlock_init_item(params, 0, keys, params->cmd.unlock.cas);
        }
    } else {
        rb_raise(rb_eArgError, "must be either Hash or single key with cas option");
    }
}


/* VERSION */
    static void
cb_params_version_alloc(struct cb_params_st *params)
{
    params->cmd.version.num = 1;
    _alloc_data_for_s(version, lcb_server_version_cmd_t, 1, items, ptr);
}


/* common stuff */
    void
cb_params_destroy(struct cb_params_st *params)
{
    rb_ary_clear(params->ensurance);
    params->ensurance = Qfalse;
    params->args = Qfalse;
    switch (params->type) {
        case cb_cmd_get:
            _release_data_for(get);
            _release_data_for_s(get, items_gr, ptr_gr);
            break;
        case cb_cmd_touch:
            _release_data_for(touch);
            break;
        case cb_cmd_arith:
            _release_data_for(arith);
            break;
        case cb_cmd_remove:
            _release_data_for(remove);
            break;
        case cb_cmd_store:
            _release_data_for(store);
            break;
        case cb_cmd_stats:
            _release_data_for(stats);
            break;
        case cb_cmd_version:
            _release_data_for(version);
            break;
        case cb_cmd_observe:
            _release_data_for(observe);
            break;
        case cb_cmd_unlock:
            _release_data_for(unlock);
            break;
    }
}

    static VALUE
do_params_build(VALUE ptr)
{
    VALUE opts;
    /* unpack arguments */
    struct cb_params_st *params = (struct cb_params_st*)ptr;
    int argc = RARRAY_LEN(params->args);
    VALUE argv = params->args;

    /* extract options */
    if (argc > 1 && TYPE(RARRAY_PTR(argv)[argc-1]) == T_HASH) {
        opts = rb_ary_pop(argv);
        --argc;
    } else {
        opts = Qnil;
    }

    params->npayload = CB_PACKET_HEADER_SIZE; /* size of packet header */
    switch (params->type) {
        case cb_cmd_touch:
            params->cmd.touch.quiet = params->bucket->quiet;
            params->cmd.touch.ttl = params->bucket->default_ttl;
            cb_params_touch_parse_options(params, opts);
            cb_params_touch_parse_arguments(params, argc, argv);
            break;
        case cb_cmd_remove:
            params->cmd.remove.quiet = params->bucket->quiet;
            if (argc == 2) {
                int type = TYPE(RARRAY_PTR(argv)[1]);
                if (type == T_FIXNUM || type == T_BIGNUM) {
                    /* allow form delete("foo", 0xdeadbeef) */
                    --argc;
                    params->cmd.remove.cas = NUM2ULL(rb_ary_pop(argv));
                }
            }
            cb_params_remove_parse_options(params, opts);
            cb_params_remove_parse_arguments(params, argc, argv);
            break;
        case cb_cmd_store:
            if (argc == 1 && opts != Qnil) {
                /* put last hash back because it is the value */
                rb_ary_push(argv, opts);
                opts = Qnil;
                ++argc;
            }
            params->cmd.store.datatype = 0x00;
            params->cmd.store.ttl = params->bucket->default_ttl;
            params->cmd.store.flags = params->bucket->default_flags;
            params->cmd.store.observe = Qnil;
            params->cmd.store.transcoder = params->bucket->transcoder;
            params->cmd.store.transcoder_opts = rb_hash_new();
            cb_params_store_parse_options(params, opts);
            cb_params_store_parse_arguments(params, argc, argv);
            break;
        case cb_cmd_get:
            params->cmd.get.quiet = params->bucket->quiet;
            params->cmd.get.transcoder = params->bucket->transcoder;
            params->cmd.get.transcoder_opts = rb_hash_new();
            params->cmd.get.replica = Qfalse;
            cb_params_get_parse_options(params, opts);
            cb_params_get_parse_arguments(params, argc, argv);
            break;
        case cb_cmd_arith:
            params->cmd.arith.transcoder = params->bucket->transcoder;
            params->cmd.arith.transcoder_opts = rb_hash_new();
            params->cmd.arith.create = params->bucket->default_arith_create;
            params->cmd.arith.initial = params->bucket->default_arith_init;
            params->cmd.arith.delta = 1;
            params->cmd.arith.ttl = params->bucket->default_ttl;
            if (argc == 2 && TYPE(RARRAY_PTR(argv)[1]) == T_FIXNUM) {
                /* allow form incr("foo", 1) */
                --argc;
                params->cmd.arith.delta = NUM2ULL(rb_ary_pop(argv)) & INT64_MAX;
            }
            cb_params_arith_parse_options(params, opts);
            cb_params_arith_parse_arguments(params, argc, argv);
            break;
        case cb_cmd_stats:
            cb_params_stats_parse_arguments(params, argc, argv);
            break;
        case cb_cmd_version:
            cb_params_version_alloc(params);
            break;
        case cb_cmd_observe:
            cb_params_observe_parse_arguments(params, argc, argv);
            break;
        case cb_cmd_unlock:
            params->cmd.unlock.quiet = params->bucket->quiet;
            if (argc == 2) {
                int type = TYPE(RARRAY_PTR(argv)[1]);
                if (type == T_FIXNUM || type == T_BIGNUM) {
                    /* allow form unlock("foo", 0xdeadbeef) */
                    --argc;
                    params->cmd.unlock.cas = NUM2ULL(rb_ary_pop(argv));
                }
            }
            cb_params_unlock_parse_options(params, opts);
            cb_params_unlock_parse_arguments(params, argc, argv);
            break;
    }

    return Qnil;
}

    void
cb_params_build(struct cb_params_st *params)
{
    int fail = 0;
    params->ensurance = rb_ary_new();

    rb_protect(do_params_build, (VALUE)params, &fail);
    if (fail) {
        cb_params_destroy(params);
        /* raise exception from protected block */
        rb_jump_tag(fail);
    }
}
