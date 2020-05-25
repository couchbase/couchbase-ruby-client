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

struct scope_get_all_response {
    std::error_code ec;
    collections_manifest manifest{};
};

struct scope_get_all_request {
    using response_type = scope_get_all_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;

    static const inline service_type type = service_type::management;

    std::string bucket_name;

    void encode_to(encoded_request_type& encoded)
    {
        encoded.method = "GET";
        encoded.path = fmt::format("/pools/default/buckets/{}/collections", bucket_name);
    }
};

scope_get_all_response
make_response(std::error_code ec, scope_get_all_request&, scope_get_all_request::encoded_response_type encoded)
{
    scope_get_all_response response{ ec };
    if (!ec) {
        switch (encoded.status_code) {
            case 400:
                response.ec = std::make_error_code(error::common_errc::unsupported_operation);
                break;
            case 404:
                response.ec = std::make_error_code(error::common_errc::bucket_not_found);
                break;
            case 200:
                response.manifest = tao::json::from_string(encoded.body).as<collections_manifest>();
                break;
            default:
                response.ec = std::make_error_code(error::common_errc::internal_server_failure);
                break;
        }
    }
    return response;
}

} // namespace couchbase::operations
