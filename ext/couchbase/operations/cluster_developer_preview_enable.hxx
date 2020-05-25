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
#include <operations/bucket_settings.hxx>

namespace couchbase::operations
{

struct cluster_developer_preview_enable_response {
    std::error_code ec;
};

struct cluster_developer_preview_enable_request {
    using response_type = cluster_developer_preview_enable_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;

    static const inline service_type type = service_type::management;

    void encode_to(encoded_request_type& encoded)
    {
        encoded.method = "POST";
        encoded.headers["content-type"] = "application/x-www-form-urlencoded";
        encoded.path = "/settings/developerPreview";
        encoded.body = "enabled=true";
    }
};

cluster_developer_preview_enable_response
make_response(std::error_code ec, cluster_developer_preview_enable_request&, scope_get_all_request::encoded_response_type encoded)
{
    cluster_developer_preview_enable_response response{ ec };
    if (!ec) {
        if (encoded.status_code != 200) {
            response.ec = std::make_error_code(error::common_errc::internal_server_failure);
        }
    }
    return response;
}

} // namespace couchbase::operations
