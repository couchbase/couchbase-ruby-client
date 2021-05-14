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
#include <operations/rbac.hxx>
#include <utils/url_codec.hxx>

namespace couchbase::operations
{

struct role_get_all_response {
    error_context::http ctx;
    std::vector<rbac::role_and_description> roles{};
};

struct role_get_all_request {
    using response_type = role_get_all_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;
    using error_context_type = error_context::http;

    static const inline service_type type = service_type::management;

    std::chrono::milliseconds timeout{ timeout_defaults::management_timeout };
    std::string client_context_id{ uuid::to_string(uuid::random()) };

    [[nodiscard]] std::error_code encode_to(encoded_request_type& encoded, http_context& /* context */) const
    {
        encoded.method = "GET";
        encoded.path = "/settings/rbac/roles";
        encoded.headers["content-type"] = "application/x-www-form-urlencoded";
        return {};
    }
};

role_get_all_response
make_response(error_context::http&& ctx, const role_get_all_request& /* request */, role_get_all_request::encoded_response_type&& encoded)
{
    role_get_all_response response{ std::move(ctx) };
    if (!response.ctx.ec) {
        if (encoded.status_code == 200) {
            tao::json::value payload{};
            try {
                payload = tao::json::from_string(encoded.body);
            } catch (const tao::json::pegtl::parse_error& e) {
                response.ctx.ec = error::common_errc::parsing_failure;
                return response;
            }
            for (const auto& entry : payload.get_array()) {
                response.roles.emplace_back(entry.as<rbac::role_and_description>());
            }
        } else {
            response.ctx.ec = error::common_errc::internal_server_failure;
        }
    }
    return response;
}

} // namespace couchbase::operations
