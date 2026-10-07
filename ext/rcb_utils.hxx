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
#include <couchbase/expiry.hxx>
#include <couchbase/persist_to.hxx>
#include <couchbase/read_preference.hxx>
#include <couchbase/replicate_to.hxx>
#include <couchbase/store_semantics.hxx>

#include <chrono>
#include <cstdint>
#include <exception>
#include <optional>
#include <string>
#include <vector>

#include <ruby/ruby.h>
#include <ruby/thread.h>

#include "rcb_exceptions.hxx"
#include "rcb_logger.hxx"

namespace couchbase::ruby
{
// Runs fn, which calls Ruby code, under rb_protect. A Ruby non-local exit from fn stops at
// rb_protect and continues as ruby_jump, so the C++ frames between the caller and cb_method
// unwind. The longjmp still skips objects held in fn's own frames. fn must not throw; the
// rb_protect callback is noexcept, so an exception terminates instead of crossing rb_protect's
// frame.
template<typename Callable>
inline void
cb_protect(const Callable& fn)
{
  int state = 0;
  rb_protect(
    [](VALUE arg) noexcept -> VALUE {
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
      (*reinterpret_cast<const Callable*>(arg))();
      return Qnil;
    },
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    reinterpret_cast<VALUE>(&fn),
    &state);
  if (state != 0) {
    throw ruby_jump(state);
  }
}

// Wraps Method, which may throw ruby_exception or ruby_jump, for rb_define_method. Method's
// frames unwind first; the Ruby raise or jump happens here, where no C++ object is live and no
// catch handler is active.
template<auto Method, typename = decltype(Method)>
struct cb_method;

template<auto Method, typename... Args>
struct cb_method<Method, VALUE (*)(Args...)> {
  static auto invoke(Args... args) -> VALUE
  {
    int state = 0;
    VALUE exc = Qnil;
    try {
      return Method(args...);
    } catch (const ruby_jump& e) {
      state = e.state();
    } catch (const ruby_exception& e) {
      exc = e.exception_object();
    }
    if (state != 0) {
      rb_jump_tag(state);
    }
    rb_exc_raise(exc);
  }
};

template<typename Future>
inline auto
cb_wait_for_future(Future&& f) -> decltype(f.get())
{
  struct arg_pack {
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
    Future&& f;
    decltype(f.get()) res{};
    std::exception_ptr error{};
    bool done{ false };

    // Runs without the GVL, inside Ruby C frames, so no exception leaves it.
    static auto wait(void* param) -> void*
    {
      auto* pack = static_cast<arg_pack*>(param);
      try {
        pack->res = std::move(pack->f.get());
      } catch (...) {
        pack->error = std::current_exception();
      }
      pack->done = true;
      return nullptr;
    }

    // No unblocking function: the wait ends only when the operation completes or times out.
    // Interrupts are checked before and after it, and a pending one raises there.
    static auto protected_wait(VALUE param) -> VALUE
    {
      rb_thread_call_without_gvl(
        wait,
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast,performance-no-int-to-ptr)
        reinterpret_cast<void*>(param),
        nullptr,
        nullptr);
      return Qnil;
    }
  } arg{ std::forward<Future>(f) };

  // rb_protect stops an interrupt (Timeout, Thread#raise, Thread#kill, a signal) raised at the
  // checks around the wait. One raised before the wait leaves the operation in flight, so the
  // wait is retried until the operation completes; no attempt waits with the GVL held. A later
  // interrupt replaces the earlier one, which is discarded.
  int state = 0;
  VALUE interrupt = Qnil;
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  rb_protect(arg_pack::protected_wait, reinterpret_cast<VALUE>(&arg), &state);
  if (state != 0) {
    interrupt = rb_errinfo();
  }
  while (!arg.done) {
    int next = 0;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    rb_protect(arg_pack::protected_wait, reinterpret_cast<VALUE>(&arg), &next);
    if (next != 0) {
      state = next;
      interrupt = rb_errinfo();
    }
  }
  if (state != 0) {
    // A trap handler run by a retry's interrupt check can clear errinfo by rescuing, so a raised
    // exception is restored before ruby_jump carries the interrupt to cb_method. Trap handlers run
    // only on the main thread, which never receives a kill interrupt. errinfo holds an Exception
    // only after a raise: a kill leaves an Integer, and a throw (Timeout on Ruby 3.2 uses catch
    // and throw) leaves internal throw data, which rb_obj_is_kind_of cannot read.
    if (RB_TYPE_P(interrupt, T_OBJECT) && rb_obj_is_kind_of(interrupt, rb_eException) == Qtrue) {
      rb_set_errinfo(interrupt);
    }
    throw ruby_jump(state);
  }
  // Only when no interrupt was stopped: the logger runs Ruby code, which would replace errinfo.
  // Otherwise its messages stay queued for the next flush.
  cb_protect([] {
    flush_logger();
  });
  if (arg.error) {
    // The promises waited on here are fulfilled only with set_value, so a future fails only with
    // std::future_error, when its promise is destroyed unfulfilled.
    std::string message{ "unable to wait for the operation result" };
    try {
      std::rethrow_exception(arg.error);
    } catch (const std::exception& e) {
      message.append(": ").append(e.what());
    } catch (...) {
    }
    throw ruby_exception(cb_exc_new(exc_couchbase_error(), message));
  }
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

std::optional<VALUE>
get_hash(VALUE options, VALUE name);

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

template<typename Request>
inline void
cb_extract_durability_level(Request& req, VALUE options)
{
  if (NIL_P(options)) {
    return;
  }

  if (TYPE(options) != T_HASH) {
    throw ruby_exception(rb_eArgError,
                         rb_sprintf("expected options to be Hash, given %+" PRIsVALUE, options));
  }

  static VALUE property_name = rb_id2sym(rb_intern("durability_level"));

  VALUE val = rb_hash_aref(options, property_name);
  if (NIL_P(val)) {
    return;
  }
  if (TYPE(val) != T_SYMBOL) {
    throw ruby_exception(
      rb_eArgError, rb_sprintf("durability_level must be a Symbol, but given %+" PRIsVALUE, val));
  }
  if (ID level = rb_sym2id(val); level == rb_intern("none")) {
    req.durability_level = couchbase::durability_level::none;
  } else if (level == rb_intern("majority")) {
    req.durability_level = couchbase::durability_level::majority;
  } else if (level == rb_intern("majority_and_persist_to_active")) {
    req.durability_level = couchbase::durability_level::majority_and_persist_to_active;
  } else if (level == rb_intern("persist_to_majority")) {
    req.durability_level = couchbase::durability_level::persist_to_majority;
  } else {
    throw ruby_exception(rb_eArgError,
                         rb_sprintf("unexpected durability_level, given %+" PRIsVALUE, val));
  }
}

template<typename Request>
inline void
cb_extract_read_preference(Request& req, VALUE options)
{
  static VALUE property_name = rb_id2sym(rb_intern("read_preference"));
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
        rb_eArgError, rb_sprintf("read_preference must be a Symbol, but given %+" PRIsVALUE, val));
    }

    if (ID mode = rb_sym2id(val); mode == rb_intern("no_preference")) {
      req.read_preference = couchbase::read_preference::no_preference;
    } else if (mode == rb_intern("selected_server_group")) {
      req.read_preference = couchbase::read_preference::selected_server_group;
    } else {
      throw ruby_exception(rb_eArgError,
                           rb_sprintf("unexpected read_preference, given %+" PRIsVALUE, val));
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
cb_extract_content(std::vector<std::byte>& field, VALUE content);

template<typename Request>
inline void
cb_extract_content(Request& req, VALUE options)
{
  cb_extract_content(req.value, options);
}

void
cb_extract_flags(std::uint32_t& field, VALUE flags);

template<typename Request>
inline void
cb_extract_flags(Request& req, VALUE options)
{
  cb_extract_flags(req.flags, options);
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

template<typename Request>
inline void
cb_extract_cas(Request& req, VALUE options)
{
  if (NIL_P(options) || TYPE(options) != T_HASH) {
    return;
  }
  static VALUE property_name = rb_id2sym(rb_intern("cas"));
  VALUE cas_value = rb_hash_aref(options, property_name);
  if (NIL_P(cas_value)) {
    return;
  }
  cb_extract_cas(req.cas, cas_value);
}

void
cb_extract_expiry(std::uint32_t& field, VALUE options);

void
cb_extract_expiry(std::optional<std::uint32_t>& field, VALUE options);

template<typename Request>
inline void
cb_extract_expiry(Request& req, VALUE options)
{
  cb_extract_expiry(req.expiry, options);
}

template<typename Request>
inline void
cb_extract_preserve_expiry(Request& req, VALUE options)
{
  cb_extract_option_bool(req.preserve_expiry, options, "preserve_expiry");
}

VALUE
cb_cas_to_num(const couchbase::cas& cas);

couchbase::cas
cb_num_to_cas(VALUE num);

VALUE
to_cas_value(couchbase::cas cas);

template<typename Response>
inline VALUE
cb_create_mutation_result(Response resp)
{
  VALUE res = rb_hash_new();
  rb_hash_aset(res, rb_id2sym(rb_intern("cas")), to_cas_value(resp.cas));

  VALUE token = rb_hash_new();
  rb_hash_aset(token, rb_id2sym(rb_intern("partition_uuid")), ULL2NUM(resp.token.partition_uuid()));
  rb_hash_aset(
    token, rb_id2sym(rb_intern("sequence_number")), ULL2NUM(resp.token.sequence_number()));
  rb_hash_aset(token, rb_id2sym(rb_intern("partition_id")), UINT2NUM(resp.token.partition_id()));
  rb_hash_aset(token, rb_id2sym(rb_intern("bucket_name")), cb_str_new(resp.token.bucket_name()));
  rb_hash_aset(res, rb_id2sym(rb_intern("mutation_token")), token);

  return res;
}

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

template<typename Request>
inline void
cb_extract_store_semantics(Request& req, VALUE options)
{
  static VALUE property_name = rb_id2sym(rb_intern("store_semantics"));

  if (NIL_P(options)) {
    return;
  }
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
    req.store_semantics = couchbase::store_semantics::replace;
  } else if (mode == rb_intern("insert")) {
    req.store_semantics = couchbase::store_semantics::insert;
  } else if (mode == rb_intern("upsert")) {
    req.store_semantics = couchbase::store_semantics::upsert;
  } else {
    throw ruby_exception(rb_eArgError,
                         rb_sprintf("unexpected store_semantics, given %+" PRIsVALUE, val));
  }
}
} // namespace couchbase::ruby

#endif
