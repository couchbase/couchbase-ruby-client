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

#include <operations/rbac.hxx>
#include <utils/url_codec.hxx>
#include <version.hxx>

namespace couchbase::operations
{

struct user_drop_response {
    error_context::http ctx;
};

struct user_drop_request {
    using response_type = user_drop_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;
    using error_context_type = error_context::http;

    static const inline service_type type = service_type::management;

    std::string username{};
    rbac::auth_domain domain{ rbac::auth_domain::local };
    std::chrono::milliseconds timeout{ timeout_defaults::management_timeout };
    std::string client_context_id{ uuid::to_string(uuid::random()) };

    [[nodiscard]] std::error_code encode_to(encoded_request_type& encoded, http_context&)
    {
        encoded.method = "DELETE";
        encoded.path = fmt::format("/settings/rbac/users/{}/{}", domain, username);
        return {};
    }
};

user_drop_response
make_response(error_context::http&& ctx, user_drop_request&, user_drop_request::encoded_response_type&& encoded)
{
    user_drop_response response{ ctx };
    if (!response.ctx.ec) {
        switch (encoded.status_code) {
            case 200:
                break;
            case 404:
                response.ctx.ec = std::make_error_code(error::management_errc::user_not_found);
                break;
            default:
                response.ctx.ec = std::make_error_code(error::common_errc::internal_server_failure);
                break;
        }
    }
    return response;
}

} // namespace couchbase::operations
