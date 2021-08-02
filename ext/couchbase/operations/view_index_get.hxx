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
struct view_index_get_response {
    error_context::http ctx;
    design_document document{};
};

struct view_index_get_request {
    using response_type = view_index_get_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;
    using error_context_type = error_context::http;

    static const inline service_type type = service_type::view;

    std::string client_context_id{ uuid::to_string(uuid::random()) };
    std::chrono::milliseconds timeout{ timeout_defaults::management_timeout };

    std::string bucket_name;
    std::string document_name;
    design_document::name_space name_space;

    [[nodiscard]] std::error_code encode_to(encoded_request_type& encoded, http_context& /* context */) const
    {
        encoded.method = "GET";
        encoded.path =
          fmt::format("/{}/_design/{}{}", bucket_name, name_space == design_document::name_space::development ? "dev_" : "", document_name);
        return {};
    }
};

view_index_get_response
make_response(error_context::http&& ctx, const view_index_get_request& request, view_index_get_request::encoded_response_type&& encoded)
{
    view_index_get_response response{ std::move(ctx) };
    if (!response.ctx.ec) {
        if (encoded.status_code == 200) {
            response.document.name = request.document_name;
            response.document.ns = request.name_space;

            tao::json::value payload{};
            try {
                payload = tao::json::from_string(encoded.body);
            } catch (const tao::json::pegtl::parse_error& e) {
                response.ctx.ec = error::common_errc::parsing_failure;
                return response;
            }
            const auto* views = payload.find("views");
            if (views != nullptr && views->is_object()) {
                for (const auto& [name, view_entry] : views->get_object()) {
                    couchbase::operations::design_document::view view;
                    view.name = name;
                    if (view_entry.is_object()) {
                        if (const auto* map = view_entry.find("map"); map != nullptr && map->is_string()) {
                            view.map = map->get_string();
                        }
                        if (const auto* reduce = view_entry.find("reduce"); reduce != nullptr && reduce->is_string()) {
                            view.reduce = reduce->get_string();
                        }
                    }
                    response.document.views[view.name] = view;
                }
            }
        } else if (encoded.status_code == 404) {
            response.ctx.ec = error::view_errc::design_document_not_found;
        } else {
            response.ctx.ec = error::common_errc::internal_server_failure;
        }
    }
    return response;
}

} // namespace couchbase::operations
