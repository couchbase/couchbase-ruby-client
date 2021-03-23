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

namespace couchbase::operations
{
struct query_index_get_all_response {
    struct query_index {
        bool is_primary{ false };
        std::string id;
        std::string name;
        std::string state;
        std::string datastore_id;
        std::string keyspace_id;
        std::string namespace_id;
        std::string collection_name;
        std::string type;
        std::vector<std::string> index_key{};
        std::optional<std::string> condition{};
        std::optional<std::string> bucket_id{};
        std::optional<std::string> scope_id{};
    };
    error_context::http ctx;
    std::string status{};
    std::vector<query_index> indexes{};
};

struct query_index_get_all_request {
    using response_type = query_index_get_all_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;
    using error_context_type = error_context::http;

    static const inline service_type type = service_type::query;

    std::string client_context_id{ uuid::to_string(uuid::random()) };
    std::string bucket_name;
    std::chrono::milliseconds timeout{ timeout_defaults::management_timeout };

    [[nodiscard]] std::error_code encode_to(encoded_request_type& encoded, http_context&)
    {
        encoded.headers["content-type"] = "application/json";
        tao::json::value body{
            { "statement",
              fmt::format(
                R"(SELECT idx.* FROM system:indexes AS idx WHERE ((keyspace_id = "{}" AND bucket_id IS MISSING) OR (bucket_id = "{}")) AND `using`="gsi" ORDER BY is_primary DESC, name ASC)",
                bucket_name,
                bucket_name) },
            { "client_context_id", client_context_id }
        };
        encoded.method = "POST";
        encoded.path = "/query/service";
        encoded.body = tao::json::to_string(body);
        return {};
    }
};

query_index_get_all_response
make_response(error_context::http&& ctx, query_index_get_all_request&, query_index_get_all_request::encoded_response_type&& encoded)
{
    query_index_get_all_response response{ ctx };
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
            if (response.status == "success") {
                for (const auto& entry : payload.at("results").get_array()) {
                    query_index_get_all_response::query_index index;
                    index.id = entry.at("id").get_string();
                    index.datastore_id = entry.at("datastore_id").get_string();
                    index.namespace_id = entry.at("namespace_id").get_string();
                    index.keyspace_id = entry.at("keyspace_id").get_string();
                    index.type = entry.at("using").get_string();
                    index.name = entry.at("name").get_string();
                    index.state = entry.at("state").get_string();
                    if (const auto* prop = entry.find("bucket_id")) {
                        index.bucket_id = prop->get_string();
                    }
                    if (const auto* prop = entry.find("scope_id")) {
                        index.scope_id = prop->get_string();
                    }
                    if (const auto* prop = entry.find("is_primary")) {
                        index.is_primary = prop->get_boolean();
                    }
                    if (const auto* prop = entry.find("condition")) {
                        index.condition = prop->get_string();
                    }
                    for (const auto& key : entry.at("index_key").get_array()) {
                        index.index_key.emplace_back(key.get_string());
                    }
                    response.indexes.emplace_back(index);
                }
            }
        }
    }
    return response;
}

} // namespace couchbase::operations
