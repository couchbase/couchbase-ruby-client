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

#include <couchbase/expiry.hxx>

#include <core/utils/binary.hxx>

#include "rcb_exceptions.hxx"
#include "rcb_utils.hxx"

#include <cstdint>
#include <ruby.h>

namespace couchbase::ruby
{
namespace
{
VALUE
cb_displaying_class_of(VALUE x)
{
  switch (x) {
    case Qfalse:
      return rb_str_new_cstr("false");
    case Qnil:
      return rb_str_new_cstr("nil");
    case Qtrue:
      return rb_str_new_cstr("true");
    default:
      return rb_obj_class(x);
  }
}

const char*
cb_builtin_type_name(int type)
{
  switch (type) {
    case RUBY_T_OBJECT:
      return "Object";
    case RUBY_T_CLASS:
      return "Class";
    case RUBY_T_MODULE:
      return "Module";
    case RUBY_T_FLOAT:
      return "Float";
    case RUBY_T_STRING:
      return "String";
    case RUBY_T_REGEXP:
      return "Regexp";
    case RUBY_T_ARRAY:
      return "Array";
    case RUBY_T_HASH:
      return "Hash";
    case RUBY_T_STRUCT:
      return "Struct";
    case RUBY_T_BIGNUM:
      return "Integer";
    case RUBY_T_FILE:
      return "File";
    case RUBY_T_DATA:
      return "Data";
    case RUBY_T_MATCH:
      return "MatchData";
    case RUBY_T_COMPLEX:
      return "Complex";
    case RUBY_T_RATIONAL:
      return "Rational";
    case RUBY_T_NIL:
      return "nil";
    case RUBY_T_TRUE:
      return "true";
    case RUBY_T_FALSE:
      return "false";
    case RUBY_T_SYMBOL:
      return "Symbol";
    case RUBY_T_FIXNUM:
      return "Integer";
    default:
      break;
  }
  return "unknown or system-reserved type";
}

std::optional<couchbase::replicate_to>
extract_legacy_durability_replicate_to(VALUE options)
{
  static VALUE property_name{ rb_id2sym(rb_intern("replicate_to")) };
  if (VALUE val = rb_hash_aref(options, property_name); !NIL_P(val)) {
    ID mode = rb_sym2id(val);
    if (mode == rb_intern("none")) {
      return {};
    }
    if (mode == rb_intern("one")) {
      return couchbase::replicate_to::one;
    }
    if (mode == rb_intern("two")) {
      return couchbase::replicate_to::two;
    }
    if (mode == rb_intern("three")) {
      return couchbase::replicate_to::three;
    }
    throw ruby_exception(exc_invalid_argument(),
                         rb_sprintf("unknown replicate_to: %+" PRIsVALUE, val));
  }
  return couchbase::replicate_to::none;
}

std::optional<couchbase::persist_to>
extract_legacy_durability_persist_to(VALUE options)
{
  static VALUE property_name{ rb_id2sym(rb_intern("persist_to")) };
  if (VALUE val = rb_hash_aref(options, property_name); !NIL_P(val)) {
    ID mode = rb_sym2id(val);
    if (mode == rb_intern("none")) {
      return {};
    }
    if (mode == rb_intern("active")) {
      return couchbase::persist_to::active;
    }
    if (mode == rb_intern("one")) {
      return couchbase::persist_to::one;
    }
    if (mode == rb_intern("two")) {
      return couchbase::persist_to::two;
    }
    if (mode == rb_intern("three")) {
      return couchbase::persist_to::three;
    }
    if (mode == rb_intern("four")) {
      return couchbase::persist_to::four;
    }
    throw ruby_exception(exc_invalid_argument(),
                         rb_sprintf("unknown persist_to value: %+" PRIsVALUE, val));
  }
  return couchbase::persist_to::none;
}

} // namespace

/*
 * Destructor-friendly rb_check_type from error.c
 *
 * Throws C++ exception instead of Ruby exception.
 */
void
cb_check_type(VALUE object, int type)
{
  if (RB_UNLIKELY(object == Qundef)) {
    rb_bug("undef leaked to the Ruby space");
  }

  if (auto object_type = TYPE(object);
      object_type != type || (object_type == T_DATA && RTYPEDDATA_P(object))) {
    throw ruby_exception(rb_eTypeError,
                         rb_sprintf("wrong argument type %" PRIsVALUE " (expected %s)",
                                    cb_displaying_class_of(object),
                                    cb_builtin_type_name(type)));
  }
}

std::string
cb_string_new(VALUE str)
{
  return { RSTRING_PTR(str), static_cast<std::size_t>(RSTRING_LEN(str)) };
}

std::vector<std::byte>
cb_binary_new(VALUE str)
{
  return core::utils::to_binary(static_cast<const char*>(RSTRING_PTR(str)),
                                static_cast<std::size_t>(RSTRING_LEN(str)));
}

VALUE
cb_str_new(const std::vector<std::byte>& binary)
{
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  return rb_external_str_new(reinterpret_cast<const char*>(binary.data()),
                             static_cast<long>(binary.size()));
}

VALUE
cb_str_new(const std::byte* data, std::size_t size)
{
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  return rb_external_str_new(reinterpret_cast<const char*>(data), static_cast<long>(size));
}

VALUE
cb_str_new(const char* data)
{
  return rb_external_str_new_cstr(data);
}

VALUE
cb_str_new(const std::optional<std::string>& str)
{
  if (str) {
    return rb_external_str_new(str->data(), static_cast<long>(str->size()));
  }
  return Qnil;
}

void
cb_extract_content(std::vector<std::byte>& field, VALUE content)
{
  cb_check_type(content, T_STRING);
  field = cb_binary_new(content);
}

void
cb_extract_flags(std::uint32_t& field, VALUE flags)
{
  cb_check_type(flags, T_FIXNUM);
  field = FIX2UINT(flags);
}

void
cb_extract_timeout(std::chrono::milliseconds& field, VALUE options)
{
  cb_extract_duration(field, options, "timeout");
}

void
cb_extract_timeout(std::optional<std::chrono::milliseconds>& field, VALUE options)
{
  cb_extract_duration(field, options, "timeout");
}

void
cb_extract_option_symbol(VALUE& val, VALUE options, const char* name)
{
  if (!NIL_P(options) && TYPE(options) == T_HASH) {
    val = rb_hash_aref(options, rb_id2sym(rb_intern(name)));
    if (NIL_P(val)) {
      return;
    }
    if (TYPE(val) == T_SYMBOL) {
      return;
    }
    throw couchbase::ruby::ruby_exception(
      rb_eArgError, rb_sprintf("%s must be an Symbol, but given %+" PRIsVALUE, name, val));
  }
}

void
cb_extract_option_string(VALUE& val, VALUE options, const char* name)
{
  if (!NIL_P(options) && TYPE(options) == T_HASH) {
    val = rb_hash_aref(options, rb_id2sym(rb_intern(name)));
    if (NIL_P(val)) {
      return;
    }
    if (TYPE(val) == T_STRING) {
      return;
    }
    throw couchbase::ruby::ruby_exception(
      rb_eArgError, rb_sprintf("%s must be an String, but given %+" PRIsVALUE, name, val));
  }
}

void
cb_extract_option_string(std::string& target, VALUE options, const char* name)
{
  if (!NIL_P(options) && TYPE(options) == T_HASH) {
    VALUE val = rb_hash_aref(options, rb_id2sym(rb_intern(name)));
    if (NIL_P(val)) {
      return;
    }
    if (TYPE(val) == T_STRING) {
      target = couchbase::ruby::cb_string_new(val);
      return;
    }
    throw couchbase::ruby::ruby_exception(
      rb_eArgError, rb_sprintf("%s must be an String, but given %+" PRIsVALUE, name, val));
  }
}

void
cb_extract_option_string(std::optional<std::string>& target, VALUE options, const char* name)
{
  if (!NIL_P(options) && TYPE(options) == T_HASH) {
    VALUE val = rb_hash_aref(options, rb_id2sym(rb_intern(name)));
    if (NIL_P(val)) {
      return;
    }
    if (TYPE(val) == T_STRING) {
      target.emplace(couchbase::ruby::cb_string_new(val));
      return;
    }
    throw couchbase::ruby::ruby_exception(
      rb_eArgError, rb_sprintf("%s must be an String, but given %+" PRIsVALUE, name, val));
  }
}

void
cb_extract_option_bignum(VALUE& val, VALUE options, const char* name)
{
  if (!NIL_P(options) && TYPE(options) == T_HASH) {
    val = rb_hash_aref(options, rb_id2sym(rb_intern(name)));
    if (NIL_P(val)) {
      return;
    }
    switch (TYPE(val)) {
      case T_FIXNUM:
      case T_BIGNUM:
        return;
      default:
        break;
    }
    throw couchbase::ruby::ruby_exception(
      rb_eArgError, rb_sprintf("%s must be an Integer, but given %+" PRIsVALUE, name, val));
  }
}

void
cb_extract_option_array(VALUE& val, VALUE options, const char* name)
{
  if (!NIL_P(options) && TYPE(options) == T_HASH) {
    val = rb_hash_aref(options, rb_id2sym(rb_intern(name)));
    if (NIL_P(val)) {
      return;
    }
    if (TYPE(val) == T_ARRAY) {
      return;
    }
    throw couchbase::ruby::ruby_exception(
      rb_eArgError, rb_sprintf("%s must be an Array, but given %+" PRIsVALUE, name, val));
  }
}

VALUE
cb_cas_to_num(const couchbase::cas& cas)
{
  return ULL2NUM(cas.value());
}

couchbase::cas
cb_num_to_cas(VALUE num)
{
  return couchbase::cas{ static_cast<std::uint64_t>(NUM2ULL(num)) };
}

VALUE
to_cas_value(couchbase::cas cas)
{
  return ULL2NUM(cas.value());
}

void
cb_extract_cas(couchbase::cas& field, VALUE cas)
{
  switch (TYPE(cas)) {
    case T_FIXNUM:
    case T_BIGNUM:
      field = cb_num_to_cas(cas);
      break;
    default:
      throw couchbase::ruby::ruby_exception(
        rb_eArgError, rb_sprintf("CAS must be an Integer, but given %+" PRIsVALUE, cas));
  }
}

void
cb_extract_expiry(std::uint32_t& field, VALUE options)
{
  if (NIL_P(options)) {
    return;
  }
  Check_Type(options, T_HASH);

  static VALUE property_name = rb_id2sym(rb_intern("expiry"));
  VALUE val = rb_hash_aref(options, property_name);
  if (NIL_P(val)) {
    return;
  }
  const auto [type, duration] = unpack_expiry(val);
  switch (type) {
    case expiry_type::relative:
      field = couchbase::core::impl::expiry_relative(duration);
      break;
    case expiry_type::absolute:
      field =
        couchbase::core::impl::expiry_absolute(std::chrono::system_clock::time_point(duration));
      break;
    case expiry_type::none:
      field = couchbase::core::impl::expiry_none();
      break;
  }
}

void
cb_extract_expiry(std::optional<std::uint32_t>& field, VALUE options)
{
  if (NIL_P(options)) {
    return;
  }
  Check_Type(options, T_HASH);

  static VALUE property_name = rb_id2sym(rb_intern("expiry"));
  VALUE val = rb_hash_aref(options, property_name);
  if (NIL_P(val)) {
    return;
  }
  const auto [type, duration] = unpack_expiry(val);
  switch (type) {
    case expiry_type::relative:
      field = couchbase::core::impl::expiry_relative(duration);
      break;
    case expiry_type::absolute:
      field =
        couchbase::core::impl::expiry_absolute(std::chrono::system_clock::time_point(duration));
      break;
    case expiry_type::none:
      field = couchbase::core::impl::expiry_none();
      break;
  }
}

std::pair<expiry_type, std::chrono::seconds>
unpack_expiry(VALUE val, bool allow_nil)
{
  if (TYPE(val) == T_FIXNUM || TYPE(val) == T_BIGNUM) {
    return { expiry_type::relative, std::chrono::seconds(NUM2ULL(val)) };
  }
  if (TYPE(val) != T_ARRAY || RARRAY_LEN(val) != 2) {
    throw ruby_exception(
      rb_eArgError,
      rb_sprintf("expected expiry to be Array[Symbol, Integer|nil], given %+" PRIsVALUE, val));
  }
  VALUE expiry = rb_ary_entry(val, 1);
  if (NIL_P(expiry)) {
    if (allow_nil) {
      return { expiry_type::none, {} };
    }
    throw ruby_exception(rb_eArgError, "expiry value must be nil");
  }
  if (TYPE(expiry) != T_FIXNUM && TYPE(expiry) != T_BIGNUM) {
    throw ruby_exception(
      rb_eArgError, rb_sprintf("expiry value must be an Integer, but given %+" PRIsVALUE, expiry));
  }
  auto duration = std::chrono::seconds(NUM2ULL(expiry));

  VALUE type = rb_ary_entry(val, 0);
  if (TYPE(type) != T_SYMBOL) {
    throw ruby_exception(rb_eArgError,
                         rb_sprintf("expiry type must be a Symbol, but given %+" PRIsVALUE, type));
  }
  static VALUE duration_type = rb_id2sym(rb_intern("duration"));
  static VALUE time_point_type = rb_id2sym(rb_intern("time_point"));
  if (type == duration_type) {
    return { expiry_type::relative, duration };
  }
  if (type == time_point_type) {
    return { expiry_type::absolute, duration };
  }
  throw ruby_exception(rb_eArgError, rb_sprintf("unknown expiry type: %+" PRIsVALUE, type));
}

std::optional<couchbase::durability_level>
extract_durability_level(VALUE options)
{
  static VALUE property_name{ rb_id2sym(rb_intern("durability_level")) };
  if (VALUE val = rb_hash_aref(options, property_name); !NIL_P(val)) {
    ID level = rb_sym2id(val);
    if (level == rb_intern("none")) {
      return {};
    }
    if (level == rb_intern("majority")) {
      return couchbase::durability_level::majority;
    }
    if (level == rb_intern("majority_and_persist_to_active")) {
      return couchbase::durability_level::majority_and_persist_to_active;
    }
    if (level == rb_intern("persist_to_majority")) {
      return couchbase::durability_level::persist_to_majority;
    }
    throw ruby_exception(exc_invalid_argument(),
                         rb_sprintf("unknown durability level: %+" PRIsVALUE, val));
  }
  return couchbase::durability_level::none;
}

std::optional<std::pair<couchbase::persist_to, couchbase::replicate_to>>
extract_legacy_durability_constraints(VALUE options)
{
  const auto replicate_to = extract_legacy_durability_replicate_to(options);
  const auto persist_to = extract_legacy_durability_persist_to(options);
  if (!persist_to && !replicate_to) {
    return {};
  }
  return { {
    persist_to.value_or(couchbase::persist_to::none),
    replicate_to.value_or(couchbase::replicate_to::none),
  } };
}

namespace options
{
std::optional<bool>
get_bool(VALUE options, VALUE name)
{
  if (!NIL_P(options) && TYPE(options) == T_HASH) {
    cb_check_type(name, T_SYMBOL);
    VALUE val = rb_hash_aref(options, name);
    if (NIL_P(val)) {
      return {};
    }
    switch (TYPE(val)) {
      case T_TRUE:
        return true;
      case T_FALSE:
        return false;
      default:
        throw ruby_exception(
          rb_eArgError,
          rb_sprintf("%+" PRIsVALUE " must be a Boolean, but given %+" PRIsVALUE, name, val));
    }
  }
  return {};
}

std::optional<std::chrono::milliseconds>
get_milliseconds(VALUE options, VALUE name)
{
  if (!NIL_P(options) && TYPE(options) == T_HASH) {
    cb_check_type(name, T_SYMBOL);
    VALUE val = rb_hash_aref(options, name);
    if (NIL_P(val)) {
      return {};
    }
    switch (TYPE(val)) {
      case T_FIXNUM:
        return std::chrono::milliseconds(FIX2ULONG(val));
      case T_BIGNUM:
        return std::chrono::milliseconds(NUM2ULL(val));
      default:
        throw ruby_exception(
          rb_eArgError,
          rb_sprintf("%+" PRIsVALUE
                     " must be a Integer representing milliseconds, but given %+" PRIsVALUE,
                     name,
                     val));
    }
  }
  return {};
}

std::optional<std::size_t>
get_size_t(VALUE options, VALUE name)
{
  if (!NIL_P(options) && TYPE(options) == T_HASH) {
    cb_check_type(name, T_SYMBOL);
    VALUE val = rb_hash_aref(options, name);
    if (NIL_P(val)) {
      return {};
    }
    switch (TYPE(val)) {
      case T_FIXNUM:
        return static_cast<std::size_t>(FIX2ULONG(val));
      case T_BIGNUM:
        return static_cast<std::size_t>(NUM2ULL(val));
      default:
        throw ruby_exception(
          rb_eArgError,
          rb_sprintf("%+" PRIsVALUE " must be a Integer, but given %+" PRIsVALUE, name, val));
    }
  }
  return {};
}

std::optional<std::uint16_t>
get_uint16_t(VALUE options, VALUE name)
{
  if (!NIL_P(options) && TYPE(options) == T_HASH) {
    cb_check_type(name, T_SYMBOL);
    VALUE val = rb_hash_aref(options, name);
    if (NIL_P(val)) {
      return {};
    }
    switch (TYPE(val)) {
      case T_FIXNUM:
        if (FIX2ULONG(val) > std::numeric_limits<std::uint16_t>::max()) {
          throw ruby_exception(
            rb_eArgError,
            rb_sprintf("%+" PRIsVALUE " must not be larger than %d , but given %+" PRIsVALUE,
                       name,
                       static_cast<int>(std::numeric_limits<std::uint16_t>::max()),
                       val));
        }
        return static_cast<std::uint16_t>(FIX2ULONG(val));
      case T_BIGNUM:
        if (NUM2ULL(val) > std::numeric_limits<std::uint16_t>::max()) {
          throw ruby_exception(
            rb_eArgError,
            rb_sprintf("%+" PRIsVALUE " must not be larger than %d , but given %+" PRIsVALUE,
                       name,
                       static_cast<int>(std::numeric_limits<std::uint16_t>::max()),
                       val));
        }
        return static_cast<std::uint16_t>(NUM2ULL(val));
      default:
        throw ruby_exception(
          rb_eArgError,
          rb_sprintf("%+" PRIsVALUE " must be a Integer, but given %+" PRIsVALUE, name, val));
    }
  }
  return {};
}

std::optional<VALUE>
get_symbol(VALUE options, VALUE name)
{
  if (!NIL_P(options) && TYPE(options) == T_HASH) {
    cb_check_type(name, T_SYMBOL);
    VALUE val = rb_hash_aref(options, name);
    if (NIL_P(val)) {
      return {};
    }
    if (TYPE(val) == T_SYMBOL) {
      return val;
    }
    throw couchbase::ruby::ruby_exception(
      rb_eArgError,
      rb_sprintf("%+" PRIsVALUE " must be an Symbol, but given %+" PRIsVALUE, name, val));
  }
  return {};
}

std::optional<VALUE>
get_hash(VALUE options, VALUE name)
{
  if (!NIL_P(options) && TYPE(options) == T_HASH) {
    cb_check_type(name, T_SYMBOL);
    VALUE val = rb_hash_aref(options, name);
    if (NIL_P(val)) {
      return {};
    }
    if (TYPE(val) == T_HASH) {
      return val;
    }
    throw couchbase::ruby::ruby_exception(
      rb_eArgError,
      rb_sprintf("%+" PRIsVALUE " must be a Hash, but given %+" PRIsVALUE, name, val));
  }
  return {};
}

std::optional<std::string>
get_string(VALUE options, VALUE name)
{
  if (!NIL_P(options) && TYPE(options) == T_HASH) {
    cb_check_type(name, T_SYMBOL);
    VALUE val = rb_hash_aref(options, name);
    if (NIL_P(val)) {
      return {};
    }
    if (TYPE(val) == T_STRING) {
      return cb_string_new(val);
    }
    throw couchbase::ruby::ruby_exception(
      rb_eArgError,
      rb_sprintf("%+" PRIsVALUE " must be an String, but given %+" PRIsVALUE, name, val));
  }
  return {};
}
} // namespace options
} // namespace couchbase::ruby
