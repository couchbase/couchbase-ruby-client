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

struct bucket_get_all_response {
    std::error_code ec;
    std::vector<bucket_settings> buckets{};
};

struct bucket_get_all_request {
    using response_type = bucket_get_all_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;

    static const inline service_type type = service_type::management;

    void encode_to(encoded_request_type& encoded)
    {
        encoded.method = "GET";
        encoded.path = "/pools/default/buckets";
    }
};

bucket_get_all_response
make_response(std::error_code ec, bucket_get_all_request&, bucket_get_all_request::encoded_response_type encoded)
{
    bucket_get_all_response response{ ec };
    if (!ec) {
        auto payload = tao::json::from_string(encoded.body);
        const auto& entries = payload.get_array();
        response.buckets.reserve(entries.size());
        for (const auto& entry : entries) {
            response.buckets.emplace_back(entry.as<bucket_settings>());
        }
    }
    return response;
}

} // namespace couchbase::operations
