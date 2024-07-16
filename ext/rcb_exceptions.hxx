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
class key_value_error_context;
class subdocument_error_context;
class manager_error_context;
namespace core::error_context
{
class query;
class analytics;
class view;
class http;
class search;
} // namespace core::error_context
} // namespace couchbase

namespace couchbase::ruby
{
class ruby_exception : public std::runtime_error
{
  public:
    explicit ruby_exception(VALUE exc);
    ruby_exception(VALUE exc_type, VALUE exc_message);
    ruby_exception(VALUE exc_type, const std::string& exc_message);

    [[nodiscard]] auto exception_object() const -> VALUE;

  private:
    VALUE exc_;
};

auto
exc_feature_not_available() -> VALUE;

auto
exc_couchbase_error() -> VALUE;

auto
exc_cluster_closed() -> VALUE;

auto
exc_invalid_argument() -> VALUE;

[[nodiscard]] auto
cb_map_error_code(std::error_code ec, const std::string& message, bool include_error_code = true) -> VALUE;

[[nodiscard]] VALUE
cb_map_error(const key_value_error_context& ctx, const std::string& message);

[[noreturn]] void
cb_throw_error_code(std::error_code ec, const std::string& message);

[[noreturn]] void
cb_throw_error(const key_value_error_context& ctx, const std::string& message);

[[noreturn]] void
cb_throw_error(const subdocument_error_context& ctx, const std::string& message);

[[noreturn]] void
cb_throw_error(const manager_error_context& ctx, const std::string& message);

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
