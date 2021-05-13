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
#include <operations/bucket_settings.hxx>
#include <error_context/http.hxx>

namespace couchbase::operations
{

struct bucket_get_all_response {
    error_context::http ctx;
    std::vector<bucket_settings> buckets{};
};

struct bucket_get_all_request {
    using response_type = bucket_get_all_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;
    using error_context_type = error_context::http;

    static const inline service_type type = service_type::management;
    std::chrono::milliseconds timeout{ timeout_defaults::management_timeout };
    std::string client_context_id{ uuid::to_string(uuid::random()) };

    [[nodiscard]] std::error_code encode_to(encoded_request_type& encoded, http_context& /* context */)
    {
        encoded.method = "GET";
        encoded.path = "/pools/default/buckets";
        return {};
    }
};

bucket_get_all_response
make_response(error_context::http&& ctx, bucket_get_all_request& /* request */, bucket_get_all_request::encoded_response_type&& encoded)
{
    bucket_get_all_response response{ ctx };
    if (!response.ctx.ec) {
        tao::json::value payload{};
        try {
            payload = tao::json::from_string(encoded.body);
        } catch (tao::json::pegtl::parse_error& e) {
            response.ctx.ec = error::common_errc::parsing_failure;
            return response;
        }
        const auto& entries = payload.get_array();
        response.buckets.reserve(entries.size());
        for (const auto& entry : entries) {
            response.buckets.emplace_back(entry.as<bucket_settings>());
        }
    }
    return response;
}

} // namespace couchbase::operations
