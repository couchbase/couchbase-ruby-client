/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

#pragma once

#include <tao/json.hpp>

#include <version.hxx>
#include <error_context/http.hxx>

namespace couchbase::operations
{
struct http_noop_response {
    error_context::http ctx;
};

struct http_noop_request {
    using response_type = http_noop_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;
    using error_context_type = error_context::http;

    service_type type;
    std::chrono::milliseconds timeout;

    std::string client_context_id{ uuid::to_string(uuid::random()) };

    [[nodiscard]] std::error_code encode_to(encoded_request_type& encoded, http_context& /* context */)
    {
        encoded.headers["connection"] = "keep-alive";
        encoded.method = "GET";
        switch (type) {
            case service_type::query:
                timeout = timeout_defaults::query_timeout;
                encoded.path = "/admin/ping";
                break;
            case service_type::analytics:
                timeout = timeout_defaults::analytics_timeout;
                encoded.path = "/admin/ping";
                break;
            case service_type::search:
                timeout = timeout_defaults::search_timeout;
                encoded.path = "/api/ping";
                break;
            case service_type::view:
                timeout = timeout_defaults::view_timeout;
                encoded.path = "/";
                break;
            case service_type::management:
            case service_type::key_value:
                return error::common_errc::feature_not_available;
        }
        return {};
    }
};

http_noop_response
make_response(error_context::http&& ctx, const http_noop_request& /* request */, http_noop_request::encoded_response_type&&)
{
    http_noop_response response{ std::move(ctx) };
    return response;
}

} // namespace couchbase::operations
