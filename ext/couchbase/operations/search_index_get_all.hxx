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
#include <operations/search_index.hxx>

#include <version.hxx>

namespace couchbase::operations
{
struct search_index_get_all_response {
    error_context::http ctx;
    std::string status{};
    std::string impl_version{};
    std::vector<search_index> indexes{};
};

struct search_index_get_all_request {
    using response_type = search_index_get_all_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;
    using error_context_type = error_context::http;

    static const inline service_type type = service_type::search;

    std::string client_context_id{ uuid::to_string(uuid::random()) };
    std::chrono::milliseconds timeout{ timeout_defaults::management_timeout };

    std::string index_name;

    [[nodiscard]] std::error_code encode_to(encoded_request_type& encoded, http_context&)
    {
        encoded.method = "GET";
        encoded.path = fmt::format("/api/index");
        return {};
    }
};

search_index_get_all_response
make_response(error_context::http&& ctx, search_index_get_all_request&, search_index_get_all_request::encoded_response_type&& encoded)
{
    search_index_get_all_response response{ ctx };
    if (!response.ctx.ec) {
        if (encoded.status_code == 200) {
            auto payload = tao::json::from_string(encoded.body);
            response.status = payload.at("status").get_string();
            if (response.status == "ok") {
                const auto* indexDefs = payload.find("indexDefs");
                if (indexDefs != nullptr && indexDefs->is_object()) {
                    const auto* impl_ver = indexDefs->find("implVersion");
                    if (impl_ver != nullptr && impl_ver->is_string()) {
                        response.impl_version = impl_ver->get_string();
                    }
                    const auto* indexes = indexDefs->find("indexDefs");
                    for (const auto& entry : indexes->get_object()) {
                        response.indexes.emplace_back(entry.second.as<search_index>());
                    }
                }
                return response;
            }
        }
        return response;
    }
    return response;
}

} // namespace couchbase::operations
