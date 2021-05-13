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
#include <spdlog/spdlog.h>

#include <platform/uuid.h>
#include <io/http_message.hxx>
#include <operations/rbac.hxx>
#include <utils/url_codec.hxx>
#include <timeout_defaults.hxx>
#include <errors.hxx>

namespace couchbase::operations
{

struct user_upsert_response {
    error_context::http ctx;
    std::vector<std::string> errors{};
};

struct user_upsert_request {
    using response_type = user_upsert_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;
    using error_context_type = error_context::http;

    static const inline service_type type = service_type::management;

    rbac::auth_domain domain{ rbac::auth_domain::local };
    rbac::user user{};
    std::chrono::milliseconds timeout{ timeout_defaults::management_timeout };
    std::string client_context_id{ uuid::to_string(uuid::random()) };

    [[nodiscard]] std::error_code encode_to(encoded_request_type& encoded, http_context& /* context */)
    {
        encoded.method = "PUT";
        encoded.path = fmt::format("/settings/rbac/users/{}/{}", domain, user.username);
        std::vector<std::string> params{};
        if (user.display_name) {
            std::string param{};
            utils::string_codec::url_encode(user.display_name.value(), param);
            params.push_back(fmt::format("name={}", param));
        }
        if (user.password) {
            std::string param{};
            utils::string_codec::url_encode(user.password.value(), param);
            params.push_back(fmt::format("password={}", param));
        }
        {
            std::string encoded_groups = fmt::format("{}", fmt::join(user.groups, ","));
            std::string param{};
            utils::string_codec::url_encode(encoded_groups, param);
            params.push_back(fmt::format("groups={}", param));
        }
        std::vector<std::string> encoded_roles{};
        encoded_roles.reserve(user.roles.size());
        for (const auto& role : user.roles) {
            std::string spec = role.name;
            if (role.bucket) {
                spec += fmt::format("[{}", role.bucket.value());
                if (role.scope) {
                    spec += fmt::format(":{}", role.scope.value());
                    if (role.collection) {
                        spec += fmt::format(":{}", role.collection.value());
                    }
                }
                spec += "]";
            }
            encoded_roles.push_back(spec);
        }
        if (!encoded_roles.empty()) {
            std::string concatenated = fmt::format("{}", fmt::join(encoded_roles, ","));
            std::string param{};
            utils::string_codec::url_encode(concatenated, param);
            params.push_back(fmt::format("roles={}", param));
        }
        encoded.body = fmt::format("{}", fmt::join(params, "&"));
        encoded.headers["content-type"] = "application/x-www-form-urlencoded";
        return {};
    }
};

user_upsert_response
make_response(error_context::http&& ctx, user_upsert_request& /* request */, user_upsert_request::encoded_response_type&& encoded)
{
    user_upsert_response response{ ctx };
    if (!response.ctx.ec) {
        switch (encoded.status_code) {
            case 200:
                break;
            case 400: {
                tao::json::value payload{};
                try {
                    payload = tao::json::from_string(encoded.body);
                } catch (tao::json::pegtl::parse_error& e) {
                    response.ctx.ec = error::common_errc::parsing_failure;
                    return response;
                }
                response.ctx.ec = error::common_errc::invalid_argument;
                const auto* errors = payload.find("errors");
                if (errors != nullptr && errors->is_object()) {
                    for (const auto& entry : errors->get_object()) {
                        response.errors.emplace_back(fmt::format("{}: {}", entry.first, entry.second.get_string()));
                    }
                }
            } break;
            default:
                response.ctx.ec = error::common_errc::internal_server_failure;
                break;
        }
    }
    return response;
}

} // namespace couchbase::operations
