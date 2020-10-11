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
struct view_index_drop_response {
    std::string client_context_id;
    std::error_code ec;
};

struct view_index_drop_request {
    using response_type = view_index_drop_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;

    static const inline service_type type = service_type::views;

    std::string client_context_id{ uuid::to_string(uuid::random()) };
    std::chrono::milliseconds timeout{ timeout_defaults::management_timeout };

    std::string bucket_name;
    std::string document_name;
    design_document::name_space name_space;

    [[nodiscard]] std::error_code encode_to(encoded_request_type& encoded, http_context&)
    {
        encoded.method = "DELETE";
        encoded.path =
          fmt::format("/{}/_design/{}{}", bucket_name, name_space == design_document::name_space::development ? "dev_" : "", document_name);
        return {};
    }
};

view_index_drop_response
make_response(std::error_code ec, view_index_drop_request& request, view_index_drop_request::encoded_response_type&& encoded)
{
    view_index_drop_response response{ request.client_context_id, ec };
    if (!ec) {
        if (encoded.status_code == 200) {

        } else if (encoded.status_code == 404) {
            response.ec = std::make_error_code(error::view_errc::design_document_not_found);
        } else {
            response.ec = std::make_error_code(error::common_errc::internal_server_failure);
        }
    }
    return response;
}

} // namespace couchbase::operations
