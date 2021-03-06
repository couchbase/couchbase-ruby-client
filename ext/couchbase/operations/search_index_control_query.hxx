/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-2021 Couchbase, Inc.
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

namespace couchbase::operations
{
struct search_index_control_query_response {
    error_context::http ctx;
    std::string status{};
    std::string error{};
};

struct search_index_control_query_request {
    using response_type = search_index_control_query_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;
    using error_context_type = error_context::http;

    static const inline service_type type = service_type::search;

    std::string client_context_id{ uuid::to_string(uuid::random()) };
    std::chrono::milliseconds timeout{ timeout_defaults::management_timeout };

    std::string index_name;
    bool allow;

    [[nodiscard]] std::error_code encode_to(encoded_request_type& encoded, http_context&)
    {
        encoded.method = "POST";
        encoded.path = fmt::format("/api/index/{}/queryControl/{}", index_name, allow ? "allow" : "disallow");
        return {};
    }
};

search_index_control_query_response
make_response(error_context::http&& ctx,
              search_index_control_query_request&,
              search_index_control_query_request::encoded_response_type&& encoded)
{
    search_index_control_query_response response{ ctx };
    if (!response.ctx.ec) {
        if (encoded.status_code == 200) {
            tao::json::value payload{};
            try {
                payload = tao::json::from_string(encoded.body);
            } catch (tao::json::pegtl::parse_error& e) {
                response.ctx.ec = std::make_error_code(error::common_errc::parsing_failure);
                return response;
            }
            response.status = payload.at("status").get_string();
            if (response.status == "ok") {
                return response;
            }
        } else if (encoded.status_code == 400) {
            tao::json::value payload{};
            try {
                payload = tao::json::from_string(encoded.body);
            } catch (tao::json::pegtl::parse_error& e) {
                response.ctx.ec = std::make_error_code(error::common_errc::parsing_failure);
                return response;
            }
            response.status = payload.at("status").get_string();
            response.error = payload.at("error").get_string();
            if (response.error.find("index not found") != std::string::npos) {
                response.ctx.ec = std::make_error_code(error::common_errc::index_not_found);
                return response;
            }
        }
        response.ctx.ec = std::make_error_code(error::common_errc::internal_server_failure);
    }
    return response;
}

} // namespace couchbase::operations
