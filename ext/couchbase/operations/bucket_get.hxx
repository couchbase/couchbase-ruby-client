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

#include <error_context/http.hxx>
#include <operations/bucket_settings.hxx>

namespace couchbase::operations
{

struct bucket_get_response {
    error_context::http ctx;
    bucket_settings bucket{};
};

struct bucket_get_request {
    using response_type = bucket_get_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;
    using error_context_type = error_context::http;

    static const inline service_type type = service_type::management;

    std::string name;
    std::chrono::milliseconds timeout{ timeout_defaults::management_timeout };
    std::string client_context_id{ uuid::to_string(uuid::random()) };

    [[nodiscard]] std::error_code encode_to(encoded_request_type& encoded, http_context& /* context */)
    {
        encoded.method = "GET";
        encoded.path = fmt::format("/pools/default/buckets/{}", name);
        return {};
    }
};

bucket_get_response
make_response(error_context::http&& ctx, bucket_get_request& /* request */, bucket_get_request::encoded_response_type&& encoded)
{
    bucket_get_response response{ ctx };
    if (!response.ctx.ec) {
        switch (encoded.status_code) {
            case 404:
                response.ctx.ec = error::common_errc::bucket_not_found;
                break;
            case 200:
                try {
                    response.bucket = tao::json::from_string(encoded.body).as<bucket_settings>();
                } catch (tao::json::pegtl::parse_error& e) {
                    response.ctx.ec = error::common_errc::parsing_failure;
                    return response;
                }
                break;
            default:
                response.ctx.ec = error::common_errc::internal_server_failure;
                break;
        }
    }
    return response;
}

} // namespace couchbase::operations
