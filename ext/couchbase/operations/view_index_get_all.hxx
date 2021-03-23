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
#include <operations/design_document.hxx>

namespace couchbase::operations
{
struct view_index_get_all_response {
    error_context::http ctx;
    std::vector<design_document> design_documents{};
};

struct view_index_get_all_request {
    using response_type = view_index_get_all_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;
    using error_context_type = error_context::http;

    static const inline service_type type = service_type::management;

    std::string client_context_id{ uuid::to_string(uuid::random()) };
    std::chrono::milliseconds timeout{ timeout_defaults::management_timeout };

    std::string bucket_name;
    design_document::name_space name_space;

    [[nodiscard]] std::error_code encode_to(encoded_request_type& encoded, http_context&)
    {
        encoded.method = "GET";
        encoded.path = fmt::format("/pools/default/buckets/{}/ddocs", bucket_name);
        return {};
    }
};

view_index_get_all_response
make_response(error_context::http&& ctx, view_index_get_all_request& request, view_index_get_all_request::encoded_response_type&& encoded)
{
    view_index_get_all_response response{ ctx };
    if (!response.ctx.ec) {
        if (encoded.status_code == 200) {
            tao::json::value payload{};
            try {
                payload = tao::json::from_string(encoded.body);
            } catch (tao::json::pegtl::parse_error& e) {
                response.ctx.ec = std::make_error_code(error::common_errc::parsing_failure);
                return response;
            }
            auto* rows = payload.find("rows");
            if (rows != nullptr && rows->is_array()) {
                for (const auto& entry : rows->get_array()) {
                    const auto* dd = entry.find("doc");
                    if (dd == nullptr || !dd->is_object()) {
                        continue;
                    }
                    const auto* meta = dd->find("meta");
                    if (meta == nullptr || !meta->is_object()) {
                        continue;
                    }

                    design_document document{};
                    document.rev = meta->at("rev").get_string();
                    auto id = meta->at("id").get_string();
                    static const std::string prefix = "_design/";
                    if (id.find(prefix) == 0) {
                        document.name = id.substr(prefix.size());
                    } else {
                        document.name = id; // fall back, should not happen
                    }
                    static const std::string name_space_prefix = "dev_";
                    if (document.name.find(name_space_prefix) == 0) {
                        document.name = document.name.substr(name_space_prefix.size());
                        document.ns = couchbase::operations::design_document::name_space::development;
                    } else {
                        document.ns = couchbase::operations::design_document::name_space::production;
                    }
                    if (document.ns != request.name_space) {
                        continue;
                    }

                    const auto* json = dd->find("json");
                    if (json == nullptr || !json->is_object()) {
                        continue;
                    }
                    const auto* views = json->find("views");
                    if (views != nullptr && views->is_object()) {
                        for (const auto& view_entry : views->get_object()) {
                            couchbase::operations::design_document::view view;
                            view.name = view_entry.first;
                            if (view_entry.second.is_object()) {
                                const auto* map = view_entry.second.find("map");
                                if (map != nullptr && map->is_string()) {
                                    view.map = map->get_string();
                                }
                                const auto* reduce = view_entry.second.find("reduce");
                                if (reduce != nullptr && reduce->is_string()) {
                                    view.reduce = reduce->get_string();
                                }
                            }
                            document.views[view.name] = view;
                        }
                    }

                    response.design_documents.emplace_back(document);
                }
            }
        } else if (encoded.status_code == 404) {
            response.ctx.ec = std::make_error_code(error::common_errc::bucket_not_found);
        } else {
            response.ctx.ec = std::make_error_code(error::common_errc::internal_server_failure);
        }
    }
    return response;
}

} // namespace couchbase::operations
