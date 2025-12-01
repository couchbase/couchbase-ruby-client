/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2025-Present Couchbase, Inc.
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

#include <core/tracing/wrapper_sdk_tracer.hxx>

#include <memory>

namespace couchbase::ruby
{
template<typename Request>
inline auto
cb_create_parent_span(Request& req, VALUE backend)
  -> std::shared_ptr<couchbase::core::tracing::wrapper_sdk_span>
{
  // TODO(Tracing): Conditionally set the parent span only if tracing is enabled
  auto span = std::make_shared<couchbase::core::tracing::wrapper_sdk_span>();
  req.parent_span = span;
  return span;
}

void
cb_add_core_spans(VALUE observability_handler,
                  std::shared_ptr<couchbase::core::tracing::wrapper_sdk_span> parent_span,
                  std::size_t retry_attempts);
} // namespace couchbase::ruby
