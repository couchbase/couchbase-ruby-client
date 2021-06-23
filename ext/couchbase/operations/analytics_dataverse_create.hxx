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
#include <utils/name_codec.hxx>

namespace couchbase::operations
{
struct analytics_dataverse_create_response {
    struct problem {
        std::uint32_t code;
        std::string message;
    };

    error_context::http ctx;
    std::string status{};
    std::vector<problem> errors{};
};

struct analytics_dataverse_create_request {
    using response_type = analytics_dataverse_create_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;
    using error_context_type = error_context::http;

    static const inline service_type type = service_type::analytics;

    std::string client_context_id{ uuid::to_string(uuid::random()) };
    std::chrono::milliseconds timeout{ timeout_defaults::management_timeout };

    std::string dataverse_name;

    bool ignore_if_exists{ false };

    [[nodiscard]] std::error_code encode_to(encoded_request_type& encoded, http_context& /* context */) const
    {
        std::string if_not_exists_clause = ignore_if_exists ? "IF NOT EXISTS" : "";

        tao::json::value body{
            { "statement", fmt::format("CREATE DATAVERSE {} {}", utils::analytics::uncompound_name(dataverse_name), if_not_exists_clause) },
        };
        encoded.headers["content-type"] = "application/json";
        encoded.method = "POST";
        encoded.path = "/analytics/service";
        encoded.body = tao::json::to_string(body);
        return {};
    }
};

analytics_dataverse_create_response
make_response(error_context::http&& ctx,
              const analytics_dataverse_create_request& /* request */,
              analytics_dataverse_create_request::encoded_response_type&& encoded)
{
    analytics_dataverse_create_response response{ std::move(ctx) };
    if (!response.ctx.ec) {
        tao::json::value payload{};
        try {
            payload = tao::json::from_string(encoded.body);
        } catch (const tao::json::pegtl::parse_error& e) {
            response.ctx.ec = error::common_errc::parsing_failure;
            return response;
        }
        response.status = payload.at("status").get_string();

        if (response.status != "success") {
            bool dataverse_exists = false;

            if (auto* errors = payload.find("errors"); errors != nullptr && errors->is_array()) {
                for (const auto& error : errors->get_array()) {
                    analytics_dataverse_create_response::problem err{
                        error.at("code").as<std::uint32_t>(),
                        error.at("msg").get_string(),
                    };
                    switch (err.code) {
                        case 24039: /* A dataverse with this name [string] already exists. */
                            dataverse_exists = true;
                            break;
                    }
                    response.errors.emplace_back(err);
                }
            }
            if (dataverse_exists) {
                response.ctx.ec = error::analytics_errc::dataverse_exists;
            } else {
                response.ctx.ec = error::common_errc::internal_server_failure;
            }
        }
    }
    return response;
}

} // namespace couchbase::operations
