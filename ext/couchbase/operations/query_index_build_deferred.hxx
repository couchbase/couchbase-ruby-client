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
struct query_index_build_deferred_response {
    struct query_problem {
        std::uint64_t code;
        std::string message;
    };
    std::string client_context_id;
    std::error_code ec;
    std::string status{};
    std::vector<query_problem> errors{};
};

struct query_index_build_deferred_request {
    using response_type = query_index_build_deferred_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;

    static const inline service_type type = service_type::query;

    std::string client_context_id{ uuid::to_string(uuid::random()) };
    std::string bucket_name;
    std::chrono::milliseconds timeout{ timeout_defaults::management_timeout };

    void encode_to(encoded_request_type& encoded, http_context&)
    {
        encoded.headers["content-type"] = "application/json";
        tao::json::value body{
            { "statement",
              fmt::format(R"(BUILD INDEX ON `{}` ((SELECT RAW name FROM system:indexes WHERE keyspace_id = "{}" AND state = "deferred")))",
                          bucket_name,
                          bucket_name) },
            { "client_context_id", client_context_id }
        };
        encoded.method = "POST";
        encoded.path = "/query/service";
        encoded.body = tao::json::to_string(body);
    }
};

query_index_build_deferred_response
make_response(std::error_code ec,
              query_index_build_deferred_request& request,
              query_index_build_deferred_request::encoded_response_type encoded)
{
    query_index_build_deferred_response response{ request.client_context_id, ec };
    if (!ec) {
        auto payload = tao::json::from_string(encoded.body);
        response.status = payload.at("status").get_string();
        if (response.status != "success") {
            for (const auto& entry : payload.at("errors").get_array()) {
                query_index_build_deferred_response::query_problem error;
                error.code = entry.at("code").get_unsigned();
                error.message = entry.at("msg").get_string();
                response.errors.emplace_back(error);
            }
            response.ec = std::make_error_code(error::common_errc::internal_server_failure);
        }
    }
    return response;
}

} // namespace couchbase::operations
