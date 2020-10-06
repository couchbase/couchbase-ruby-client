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
#include <spdlog/spdlog.h>

#include <platform/uuid.h>
#include <io/http_message.hxx>
#include <operations/rbac.hxx>
#include <utils/url_codec.hxx>
#include <timeout_defaults.hxx>
#include <errors.hxx>

namespace couchbase::operations
{

struct group_upsert_response {
    std::string client_context_id;
    std::error_code ec;
    std::vector<std::string> errors{};
};

struct group_upsert_request {
    using response_type = group_upsert_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;

    static const inline service_type type = service_type::management;

    rbac::group group{};
    std::chrono::milliseconds timeout{ timeout_defaults::management_timeout };
    std::string client_context_id{ uuid::to_string(uuid::random()) };

    [[nodiscard]] std::error_code encode_to(encoded_request_type& encoded, http_context&)
    {
        encoded.method = "PUT";
        encoded.path = fmt::format("/settings/rbac/groups/{}", group.name);
        std::vector<std::string> params{};
        if (group.description) {
            std::string param{};
            utils::string_codec::url_encode(group.description.value(), param);
            params.push_back(fmt::format("description={}", param));
        }
        if (group.ldap_group_reference) {
            std::string param{};
            utils::string_codec::url_encode(group.ldap_group_reference.value(), param);
            params.push_back(fmt::format("ldap_group_ref={}", param));
        }
        std::vector<std::string> encoded_roles{};
        encoded_roles.reserve(group.roles.size());
        for (const auto& role : group.roles) {
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

group_upsert_response
make_response(std::error_code ec, group_upsert_request& request, group_upsert_request::encoded_response_type encoded)
{
    group_upsert_response response{ request.client_context_id, ec };
    if (!ec) {
        switch (encoded.status_code) {
            case 200:
                break;
            case 400: {
                response.ec = std::make_error_code(error::common_errc::invalid_argument);
                tao::json::value payload = tao::json::from_string(encoded.body);
                const auto* errors = payload.find("errors");
                if (errors != nullptr && errors->is_object()) {
                    for (const auto& entry : errors->get_object()) {
                        response.errors.emplace_back(fmt::format("{}: {}", entry.first, entry.second.get_string()));
                    }
                }
            } break;
            default:
                response.ec = std::make_error_code(error::common_errc::internal_server_failure);
                break;
        }
    }
    return response;
}

} // namespace couchbase::operations
