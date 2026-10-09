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

#ifndef COUCHBASE_RUBY_RCB_EXCEPTIONS_HXX
#define COUCHBASE_RUBY_RCB_EXCEPTIONS_HXX

#include <stdexcept>
#include <string>
#include <system_error>

#include <ruby/internal/value.h>

namespace couchbase
{
class error;
namespace core
{
class key_value_error_context;
class subdocument_error_context;
namespace error_context
{
class query;
class analytics;
class view;
class http;
class search;
} // namespace error_context
} // namespace core
} // namespace couchbase

namespace couchbase::ruby
{
class ruby_exception : public std::runtime_error
{
public:
  explicit ruby_exception(VALUE exc);
  // These build the exception through cb_exc_new, so they may be thrown only from a method body
  // registered through cb_method.
  ruby_exception(VALUE exc_type, VALUE exc_message);
  ruby_exception(VALUE exc_type, const std::string& exc_message);

  [[nodiscard]] auto exception_object() const -> VALUE;

private:
  VALUE exc_;
};

// A Ruby non-local exit (raise, throw, Thread#kill) stopped by rb_protect, carried through C++
// frames so that they unwind until cb_method resumes it with rb_jump_tag. What the exit carries
// stays in ec->errinfo, which GC marks. No Ruby code may run between the failed rb_protect and
// cb_method: code that rescues replaces errinfo, and rb_jump_tag would resume that instead.
class ruby_jump : public std::exception
{
public:
  explicit ruby_jump(int state);

  [[nodiscard]] auto state() const -> int;

private:
  int state_;
};

auto
exc_feature_not_available() -> VALUE;

auto
exc_couchbase_error() -> VALUE;

auto
exc_cluster_closed() -> VALUE;

auto
exc_invalid_argument() -> VALUE;

// Builds an exception of exc_type under cb_protect: the exception classes define initialize in
// Ruby. Call it only from a method body registered through cb_method.
[[nodiscard]] auto
cb_exc_new(VALUE exc_type, const std::string& message) -> VALUE;

[[nodiscard]] auto
cb_exc_new(VALUE exc_type, VALUE message) -> VALUE;

[[nodiscard]] auto
cb_map_error_code(std::error_code ec,
                  const std::string& message,
                  bool include_error_code = true) -> VALUE;

[[nodiscard]] VALUE
cb_map_error(const core::key_value_error_context& ctx, const std::string& message);

[[nodiscard]] VALUE
cb_map_error(const error& err, const std::string& message);

[[noreturn]] void
cb_throw_error_code(std::error_code ec, const std::string& message);

[[noreturn]] void
cb_throw_error(const error& ctx, const std::string& message);

[[noreturn]] void
cb_throw_error(const core::key_value_error_context& ctx, const std::string& message);

[[noreturn]] void
cb_throw_error(const core::error_context::query& ctx, const std::string& message);

[[noreturn]] void
cb_throw_error(const core::error_context::analytics& ctx, const std::string& message);

[[noreturn]] void
cb_throw_error(const core::error_context::view& ctx, const std::string& message);

[[noreturn]] void
cb_throw_error(const core::error_context::http& ctx, const std::string& message);

[[noreturn]] void
cb_throw_error(const core::error_context::search& ctx, const std::string& message);

void
init_exceptions(VALUE mCouchbase);
} // namespace couchbase::ruby

#endif
