/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc.
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

struct group_get_all_response {
    error_context::http ctx;
    std::vector<rbac::group> groups{};
};

struct group_get_all_request {
    using response_type = group_get_all_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;
    using error_context_type = error_context::http;

    static const inline service_type type = service_type::management;

    std::chrono::milliseconds timeout{ timeout_defaults::management_timeout };
    std::string client_context_id{ uuid::to_string(uuid::random()) };

    [[nodiscard]] std::error_code encode_to(encoded_request_type& encoded, http_context&)
    {
        encoded.method = "GET";
        encoded.path = fmt::format("/settings/rbac/groups");
        encoded.headers["content-type"] = "application/x-www-form-urlencoded";
        return {};
    }
};

group_get_all_response
make_response(error_context::http&& ctx, group_get_all_request&, group_get_all_request::encoded_response_type&& encoded)
{
    group_get_all_response response{ ctx };
    if (!response.ctx.ec) {
        if (encoded.status_code == 200) {
            tao::json::value payload = tao::json::from_string(encoded.body);
            for (const auto& entry : payload.get_array()) {
                response.groups.emplace_back(entry.as<rbac::group>());
            }
        } else {
            response.ctx.ec = std::make_error_code(error::common_errc::internal_server_failure);
        }
    }
    return response;
}

} // namespace couchbase::operations
