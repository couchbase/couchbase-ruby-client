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

namespace couchbase::operations
{
struct view_index_upsert_response {
    std::string client_context_id;
    std::error_code ec;
};

struct view_index_upsert_request {
    using response_type = view_index_upsert_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;

    static const inline service_type type = service_type::views;

    std::string client_context_id{ uuid::to_string(uuid::random()) };
    std::chrono::milliseconds timeout{ timeout_defaults::management_timeout };

    std::string bucket_name;
    design_document document;

    void encode_to(encoded_request_type& encoded, http_context&)
    {
        tao::json::value body;
        body["views"] = tao::json::empty_object;
        for (const auto& view : document.views) {
            tao::json::value view_def;
            if (view.second.map) {
                view_def["map"] = *view.second.map;
            }
            if (view.second.reduce) {
                view_def["reduce"] = *view.second.reduce;
            }
            body["views"][view.first] = view_def;
        }

        encoded.headers["content-type"] = "application/json";
        encoded.method = "PUT";
        encoded.path = fmt::format(
          "/{}/_design/{}{}", bucket_name, document.ns == design_document::name_space::development ? "dev_" : "", document.name);
        encoded.body = tao::json::to_string(body);
    }
};

view_index_upsert_response
make_response(std::error_code ec, view_index_upsert_request& request, view_index_upsert_request::encoded_response_type encoded)
{
    view_index_upsert_response response{ request.client_context_id, ec };
    if (!ec) {
        switch (encoded.status_code) {
            case 200:
            case 201:
                break;
            case 400:
                response.ec = std::make_error_code(error::common_errc::invalid_argument);
                break;
            case 404:
                response.ec = std::make_error_code(error::view_errc::design_document_not_found);
                break;
            default:
                response.ec = std::make_error_code(error::common_errc::internal_server_failure);
        }
    }
    return response;
}

} // namespace couchbase::operations
