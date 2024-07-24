/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#ifndef COUCHBASE_RUBY_RCB_UTILS_HXX
#define COUCHBASE_RUBY_RCB_UTILS_HXX

#include <couchbase/cas.hxx>
#include <couchbase/durability_level.hxx>
#include <couchbase/persist_to.hxx>
#include <couchbase/replicate_to.hxx>
#include <couchbase/store_semantics.hxx>

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include <ruby/ruby.h>
#include <ruby/thread.h>

#include "rcb_exceptions.hxx"
#include "rcb_logger.hxx"

namespace couchbase::ruby
{
template<typename Future>
inline auto
cb_wait_for_future(Future&& f) -> decltype(f.get())
{
  struct arg_pack {
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
    Future&& f;
    decltype(f.get()) res{};
  } arg{ std::forward<Future>(f) };
  rb_thread_call_without_gvl(
    [](void* param) -> void* {
      auto* pack = static_cast<arg_pack*>(param);
      pack->res = std::move(pack->f.get());
      return nullptr;
    },
    &arg,
    nullptr,
    nullptr);
  flush_logger();
  return std::move(arg.res);
}

template<typename StringLike>
inline VALUE
cb_str_new(const StringLike str)
{
  return rb_external_str_new(std::data(str), static_cast<long>(std::size(str)));
}

void
cb_check_type(VALUE object, int type);

std::string
cb_string_new(VALUE str);

std::vector<std::byte>
cb_binary_new(VALUE str);

VALUE
cb_str_new(const std::vector<std::byte>& binary);

VALUE
cb_str_new(const std::byte* data, std::size_t size);

VALUE
cb_str_new(const char* data);

VALUE
cb_str_new(const std::optional<std::string>& str);

namespace options
{
std::optional<bool>
get_bool(VALUE options, VALUE name);

std::optional<std::chrono::milliseconds>
get_milliseconds(VALUE options, VALUE name);

std::optional<std::size_t>
get_size_t(VALUE options, VALUE name);

std::optional<std::uint16_t>
get_uint16_t(VALUE options, VALUE name);

std::optional<VALUE>
get_symbol(VALUE options, VALUE name);

std::optional<std::string>
get_string(VALUE options, VALUE name);
} // namespace options

template<typename Request>
inline void
cb_extract_timeout(Request& req, VALUE options)
{
  if (!NIL_P(options)) {
    switch (TYPE(options)) {
      case T_HASH:
        return cb_extract_timeout(req, rb_hash_aref(options, rb_id2sym(rb_intern("timeout"))));
      case T_FIXNUM:
      case T_BIGNUM:
        req.timeout = std::chrono::milliseconds(NUM2ULL(options));
        break;
      default:
        throw ruby_exception(
          rb_eArgError, rb_sprintf("timeout must be an Integer, but given %+" PRIsVALUE, options));
    }
  }
}

template<typename Field>
inline void
cb_extract_duration(Field& field, VALUE options, const char* name)
{
  if (!NIL_P(options)) {
    switch (TYPE(options)) {
      case T_HASH:
        return cb_extract_duration(field, rb_hash_aref(options, rb_id2sym(rb_intern(name))), name);
      case T_FIXNUM:
      case T_BIGNUM:
        field = std::chrono::milliseconds(NUM2ULL(options));
        break;
      default:
        throw ruby_exception(
          rb_eArgError, rb_sprintf("%s must be an Integer, but given %+" PRIsVALUE, name, options));
    }
  }
}

void
cb_extract_timeout(std::chrono::milliseconds& field, VALUE options);

void
cb_extract_timeout(std::optional<std::chrono::milliseconds>& field, VALUE options);

void
cb_extract_option_symbol(VALUE& val, VALUE options, const char* name);

void
cb_extract_option_string(VALUE& val, VALUE options, const char* name);

void
cb_extract_option_string(std::string& target, VALUE options, const char* name);

void
cb_extract_option_string(std::optional<std::string>& target, VALUE options, const char* name);

template<typename Boolean>
inline void
cb_extract_option_bool(Boolean& field, VALUE options, const char* name)
{
  if (!NIL_P(options) && TYPE(options) == T_HASH) {
    VALUE val = rb_hash_aref(options, rb_id2sym(rb_intern(name)));
    if (NIL_P(val)) {
      return;
    }
    switch (TYPE(val)) {
      case T_TRUE:
        field = true;
        break;
      case T_FALSE:
        field = false;
        break;
      default:
        throw ruby_exception(rb_eArgError,
                             rb_sprintf("%s must be a Boolean, but given %+" PRIsVALUE, name, val));
    }
  }
}

void
cb_extract_option_bignum(VALUE& val, VALUE options, const char* name);

template<typename T>
inline void
cb_extract_option_uint64(T& field, VALUE options, const char* name)
{
  VALUE val = Qnil;
  cb_extract_option_bignum(val, options, name);
  if (!NIL_P(val)) {
    field = NUM2ULL(val);
  }
}

template<typename Integer>
inline void
cb_extract_option_number(Integer& field, VALUE options, const char* name)
{
  if (!NIL_P(options) && TYPE(options) == T_HASH) {
    VALUE val = rb_hash_aref(options, rb_id2sym(rb_intern(name)));
    if (NIL_P(val)) {
      return;
    }
    switch (TYPE(val)) {
      case T_FIXNUM:
        field = static_cast<Integer>(FIX2ULONG(val));
        break;
      case T_BIGNUM:
        field = static_cast<Integer>(NUM2ULL(val));
        break;
      default:
        throw ruby_exception(rb_eArgError,
                             rb_sprintf("%s must be a Integer, but given %+" PRIsVALUE, name, val));
    }
  }
}

void
cb_extract_option_array(VALUE& val, VALUE options, const char* name);

void
cb_extract_cas(couchbase::cas& field, VALUE cas);

VALUE
cb_cas_to_num(const couchbase::cas& cas);

couchbase::cas
cb_num_to_cas(VALUE num);

VALUE
to_cas_value(couchbase::cas cas);

template<typename Response>
inline VALUE
to_mutation_result_value(Response resp)
{
  VALUE res = rb_hash_new();
  rb_hash_aset(res, rb_id2sym(rb_intern("cas")), to_cas_value(resp.cas()));
  if (resp.mutation_token()) {
    VALUE token = rb_hash_new();
    rb_hash_aset(token,
                 rb_id2sym(rb_intern("partition_uuid")),
                 ULL2NUM(resp.mutation_token()->partition_uuid()));
    rb_hash_aset(token,
                 rb_id2sym(rb_intern("sequence_number")),
                 ULL2NUM(resp.mutation_token()->sequence_number()));
    rb_hash_aset(
      token, rb_id2sym(rb_intern("partition_id")), UINT2NUM(resp.mutation_token()->partition_id()));
    rb_hash_aset(
      token, rb_id2sym(rb_intern("bucket_name")), cb_str_new(resp.mutation_token()->bucket_name()));
    rb_hash_aset(res, rb_id2sym(rb_intern("mutation_token")), token);
  }
  return res;
}

template<typename CommandOptions>
inline void
set_timeout(CommandOptions& opts, VALUE options)
{
  static VALUE property_name = rb_id2sym(rb_intern("timeout"));
  if (!NIL_P(options)) {
    if (TYPE(options) != T_HASH) {
      throw ruby_exception(rb_eArgError,
                           rb_sprintf("expected options to be Hash, given %+" PRIsVALUE, options));
    }
    VALUE val = rb_hash_aref(options, property_name);
    if (NIL_P(val)) {
      return;
    }
    switch (TYPE(val)) {
      case T_FIXNUM:
      case T_BIGNUM:
        opts.timeout(std::chrono::milliseconds(NUM2ULL(val)));
        break;

      default:
        throw ruby_exception(rb_eArgError,
                             rb_sprintf("timeout must be an Integer, but given %+" PRIsVALUE, val));
    }
  }
}

enum class expiry_type {
  none,
  relative,
  absolute,
};

std::pair<expiry_type, std::chrono::seconds>
unpack_expiry(VALUE val, bool allow_nil = true);

template<typename CommandOptions>
inline void
set_expiry(CommandOptions& opts, VALUE options)
{
  static VALUE property_name = rb_id2sym(rb_intern("expiry"));
  if (!NIL_P(options)) {
    if (TYPE(options) != T_HASH) {
      throw ruby_exception(rb_eArgError,
                           rb_sprintf("expected options to be Hash, given %+" PRIsVALUE, options));
    }
    VALUE val = rb_hash_aref(options, property_name);
    if (NIL_P(val)) {
      return;
    }

    switch (auto [type, duration] = unpack_expiry(val); type) {
      case expiry_type::relative:
        opts.expiry(duration);
        break;

      case expiry_type::absolute:
        opts.expiry(std::chrono::system_clock::time_point(duration));
        break;

      case expiry_type::none:
        break;
    }
  }
}

template<typename CommandOptions>
inline void
set_access_deleted(CommandOptions& opts, VALUE options)
{
  static VALUE property_name = rb_id2sym(rb_intern("access_deleted"));
  if (!NIL_P(options)) {
    if (TYPE(options) != T_HASH) {
      throw ruby_exception(rb_eArgError,
                           rb_sprintf("expected options to be Hash, given %+" PRIsVALUE, options));
    }
    VALUE val = rb_hash_aref(options, property_name);
    if (NIL_P(val)) {
      return;
    }
    switch (TYPE(val)) {
      case T_TRUE:
        opts.access_deleted(true);
        break;
      case T_FALSE:
        opts.access_deleted(false);
        break;

      default:
        throw ruby_exception(
          rb_eArgError,
          rb_sprintf("access_deleted must be an Boolean, but given %+" PRIsVALUE, val));
    }
  }
}

template<typename CommandOptions>
inline void
set_create_as_deleted(CommandOptions& opts, VALUE options)
{
  static VALUE property_name = rb_id2sym(rb_intern("create_as_deleted"));
  if (!NIL_P(options)) {
    if (TYPE(options) != T_HASH) {
      throw ruby_exception(rb_eArgError,
                           rb_sprintf("expected options to be Hash, given %+" PRIsVALUE, options));
    }
    VALUE val = rb_hash_aref(options, property_name);
    if (NIL_P(val)) {
      return;
    }
    switch (TYPE(val)) {
      case T_TRUE:
        opts.create_as_deleted(true);
        break;
      case T_FALSE:
        opts.create_as_deleted(false);
        break;

      default:
        throw ruby_exception(
          rb_eArgError,
          rb_sprintf("create_as_deleted must be an Boolean, but given %+" PRIsVALUE, val));
    }
  }
}

template<typename CommandOptions>
inline void
set_preserve_expiry(CommandOptions& opts, VALUE options)
{
  static VALUE property_name = rb_id2sym(rb_intern("preserve_expiry"));
  if (!NIL_P(options)) {
    if (TYPE(options) != T_HASH) {
      throw ruby_exception(rb_eArgError,
                           rb_sprintf("expected options to be Hash, given %+" PRIsVALUE, options));
    }
    VALUE val = rb_hash_aref(options, property_name);
    if (NIL_P(val)) {
      return;
    }
    switch (TYPE(val)) {
      case T_TRUE:
        opts.preserve_expiry(true);
        break;
      case T_FALSE:
        opts.preserve_expiry(false);
        break;
      default:
        throw ruby_exception(
          rb_eArgError,
          rb_sprintf("preserve_expiry must be a Boolean, but given %+" PRIsVALUE, val));
    }
  }
}

template<typename CommandOptions>
inline void
set_cas(CommandOptions& opts, VALUE options)
{
  static VALUE property_name = rb_id2sym(rb_intern("cas"));
  if (!NIL_P(options)) {
    if (TYPE(options) != T_HASH) {
      throw ruby_exception(rb_eArgError,
                           rb_sprintf("expected options to be Hash, given %+" PRIsVALUE, options));
    }
    VALUE val = rb_hash_aref(options, property_name);
    if (NIL_P(val)) {
      return;
    }
    switch (TYPE(val)) {
      case T_FIXNUM:
      case T_BIGNUM:
        opts.cas(couchbase::cas{ static_cast<std::uint64_t>(NUM2ULL(val)) });
        break;

      default:
        throw ruby_exception(rb_eArgError,
                             rb_sprintf("cas must be an Integer, but given %+" PRIsVALUE, val));
    }
  }
}

template<typename CommandOptions>
inline void
set_delta(CommandOptions& opts, VALUE options)
{
  static VALUE property_name = rb_id2sym(rb_intern("delta"));
  if (!NIL_P(options)) {
    if (TYPE(options) != T_HASH) {
      throw ruby_exception(rb_eArgError,
                           rb_sprintf("expected options to be Hash, given %+" PRIsVALUE, options));
    }
    VALUE val = rb_hash_aref(options, property_name);
    if (NIL_P(val)) {
      return;
    }
    switch (TYPE(val)) {
      case T_FIXNUM:
      case T_BIGNUM:
        opts.delta(NUM2ULL(val));
        break;

      default:
        throw ruby_exception(rb_eArgError,
                             rb_sprintf("delta must be an Integer, but given %+" PRIsVALUE, val));
    }
  }
}

template<typename CommandOptions>
inline void
set_initial_value(CommandOptions& opts, VALUE options)
{
  static VALUE property_name = rb_id2sym(rb_intern("initial_value"));
  if (!NIL_P(options)) {
    if (TYPE(options) != T_HASH) {
      throw ruby_exception(rb_eArgError,
                           rb_sprintf("expected options to be Hash, given %+" PRIsVALUE, options));
    }
    VALUE val = rb_hash_aref(options, property_name);
    if (NIL_P(val)) {
      return;
    }
    switch (TYPE(val)) {
      case T_FIXNUM:
      case T_BIGNUM:
        opts.initial(NUM2ULL(val));
        break;

      default:
        throw ruby_exception(
          rb_eArgError,
          rb_sprintf("initial_value must be an Integer, but given %+" PRIsVALUE, val));
    }
  }
}

std::optional<couchbase::durability_level>
extract_durability_level(VALUE options);

std::optional<std::pair<couchbase::persist_to, couchbase::replicate_to>>
extract_legacy_durability_constraints(VALUE options);

template<typename CommandOptions>
inline void
set_durability(CommandOptions& opts, VALUE options)
{
  if (!NIL_P(options)) {
    if (TYPE(options) != T_HASH) {
      throw ruby_exception(rb_eArgError,
                           rb_sprintf("expected options to be Hash, given %+" PRIsVALUE, options));
    }
    if (auto level = extract_durability_level(options); level.has_value()) {
      opts.durability(level.value());
    }
    if (auto constraints = extract_legacy_durability_constraints(options);
        constraints.has_value()) {
      auto [persist_to, replicate_to] = constraints.value();
      opts.durability(persist_to, replicate_to);
    }
  }
}

template<typename CommandOptions>
inline void
set_store_semantics(CommandOptions& opts, VALUE options)
{
  static VALUE property_name = rb_id2sym(rb_intern("store_semantics"));
  if (!NIL_P(options)) {
    if (TYPE(options) != T_HASH) {
      throw ruby_exception(rb_eArgError,
                           rb_sprintf("expected options to be Hash, given %+" PRIsVALUE, options));
    }

    VALUE val = rb_hash_aref(options, property_name);
    if (NIL_P(val)) {
      return;
    }
    if (TYPE(val) != T_SYMBOL) {
      throw ruby_exception(
        rb_eArgError, rb_sprintf("store_semantics must be a Symbol, but given %+" PRIsVALUE, val));
    }

    if (ID mode = rb_sym2id(val); mode == rb_intern("replace")) {
      opts.store_semantics(store_semantics::replace);
    } else if (mode == rb_intern("insert")) {
      opts.store_semantics(store_semantics::insert);
    } else if (mode == rb_intern("upsert")) {
      opts.store_semantics(store_semantics::upsert);
    } else {
      throw ruby_exception(rb_eArgError,
                           rb_sprintf("unexpected store_semantics, given %+" PRIsVALUE, val));
    }
  }
}

} // namespace couchbase::ruby

#endif
