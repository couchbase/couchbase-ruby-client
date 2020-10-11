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

namespace couchbase::operations
{
struct search_index_upsert_response {
    std::string client_context_id;
    std::error_code ec;
    std::string status{};
    std::string error{};
};

struct search_index_upsert_request {
    using response_type = search_index_upsert_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;

    static const inline service_type type = service_type::search;

    std::string client_context_id{ uuid::to_string(uuid::random()) };
    std::chrono::milliseconds timeout{ timeout_defaults::management_timeout };

    search_index index;

    [[nodiscard]] std::error_code encode_to(encoded_request_type& encoded, http_context&)
    {
        encoded.method = "PUT";
        encoded.headers["cache-control"] = "no-cache";
        encoded.headers["content-type"] = "application/json";
        encoded.path = fmt::format("/api/index/{}", index.name);
        tao::json::value body{
            { "name", index.name },
            { "type", index.type },
            { "sourceType", index.source_type },
        };
        if (!index.uuid.empty()) {
            body["uuid"] = index.uuid;
        }
        if (!index.params_json.empty()) {
            body["params"] = tao::json::from_string(index.params_json);
        }
        if (!index.source_name.empty()) {
            body["sourceName"] = index.source_name;
        }
        if (!index.source_uuid.empty()) {
            body["sourceUUID"] = index.source_uuid;
        }
        if (!index.source_params_json.empty()) {
            body["sourceParams"] = tao::json::from_string(index.source_params_json);
        }
        if (!index.plan_params_json.empty()) {
            body["planParams"] = tao::json::from_string(index.plan_params_json);
        }
        encoded.body = tao::json::to_string(body);
        return {};
    }
};

search_index_upsert_response
make_response(std::error_code ec, search_index_upsert_request& request, search_index_upsert_request::encoded_response_type&& encoded)
{
    search_index_upsert_response response{ request.client_context_id, ec };
    if (!ec) {
        if (encoded.status_code == 200) {
            auto payload = tao::json::from_string(encoded.body);
            response.status = payload.at("status").get_string();
            if (response.status == "ok") {
                return response;
            }
        } else if (encoded.status_code == 400) {
            auto payload = tao::json::from_string(encoded.body);
            response.status = payload.at("status").get_string();
            response.error = payload.at("error").get_string();
            if (response.error.find("index not found") != std::string::npos) {
                response.ec = std::make_error_code(error::common_errc::index_not_found);
                return response;
            } else if (response.error.find("index with the same name already exists") != std::string::npos) {
                response.ec = std::make_error_code(error::common_errc::index_exists);
                return response;
            }
        }
        response.ec = std::make_error_code(error::common_errc::internal_server_failure);
    }
    return response;
}

} // namespace couchbase::operations
