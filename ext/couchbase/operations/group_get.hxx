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

struct group_get_response {
    std::string client_context_id;
    std::error_code ec;
    rbac::group group{};
};

struct group_get_request {
    using response_type = group_get_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;

    static const inline service_type type = service_type::management;

    std::string name{};
    std::chrono::milliseconds timeout{ timeout_defaults::management_timeout };
    std::string client_context_id{ uuid::to_string(uuid::random()) };

    void encode_to(encoded_request_type& encoded)
    {
        encoded.method = "GET";
        encoded.path = fmt::format("/settings/rbac/groups/{}", name);
        encoded.headers["content-type"] = "application/x-www-form-urlencoded";
    }
};

group_get_response
make_response(std::error_code ec, group_get_request& request, group_get_request::encoded_response_type encoded)
{
    group_get_response response{ request.client_context_id, ec };
    if (!ec) {
        switch (encoded.status_code) {
            case 200: {
                tao::json::value payload = tao::json::from_string(encoded.body);
                response.group = payload.as<rbac::group>();
            } break;
            case 404:
                response.ec = std::make_error_code(error::management_errc::group_not_found);
                break;
            default:
                response.ec = std::make_error_code(error::common_errc::internal_server_failure);
                break;
        }
    }
    return response;
}

} // namespace couchbase::operations
