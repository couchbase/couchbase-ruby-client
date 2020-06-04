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
struct query_index_drop_response {
    struct query_problem {
        std::uint64_t code;
        std::string message;
    };
    uuid::uuid_t client_context_id;
    std::error_code ec;
    std::string status{};
    std::vector<query_problem> errors{};
};

struct query_index_drop_request {
    using response_type = query_index_drop_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;

    static const inline service_type type = service_type::query;

    uuid::uuid_t client_context_id{ uuid::random() };
    std::string bucket_name;
    std::string index_name;
    bool is_primary{ false };
    bool ignore_if_does_not_exist{ false };
    std::chrono::milliseconds timeout{ timeout_defaults::management_timeout };

    void encode_to(encoded_request_type& encoded)
    {
        encoded.headers["content-type"] = "application/json";
        tao::json::value body{ { "statement",
                                 is_primary ? fmt::format(R"(DROP PRIMARY INDEX ON `{}` USING GSI)", bucket_name)
                                            : fmt::format(R"(DROP INDEX `{}`.`{}` USING GSI)", bucket_name, index_name) },
                               { "client_context_id", uuid::to_string(client_context_id) } };
        encoded.method = "POST";
        encoded.path = "/query/service";
        encoded.body = tao::json::to_string(body);
    }
};

query_index_drop_response
make_response(std::error_code ec, query_index_drop_request& request, query_index_drop_request::encoded_response_type encoded)
{
    query_index_drop_response response{ request.client_context_id, ec };
    if (!ec) {
        auto payload = tao::json::from_string(encoded.body);
        response.status = payload.at("status").get_string();

        if (response.status != "success") {
            bool bucket_not_found = false;
            bool index_not_found = false;
            for (const auto& entry : payload.at("errors").get_array()) {
                query_index_drop_response::query_problem error;
                error.code = entry.at("code").get_unsigned();
                error.message = entry.at("msg").get_string();
                switch (error.code) {
                    case 5000: /* IKey: "Internal Error" */
                        if (error.message.find("not found.") != std::string::npos) {
                            index_not_found = true;
                        }
                        break;
                    case 12003: /* IKey: "datastore.couchbase.keyspace_not_found" */
                        bucket_not_found = true;
                        break;
                    case 12004: /* IKey: "datastore.couchbase.primary_idx_not_found" */
                    case 12006: /* IKey: "datastore.couchbase.keyspace_count_error" */
                        index_not_found = true;
                        break;
                }
                response.errors.emplace_back(error);
            }
            if (index_not_found) {
                if (!request.ignore_if_does_not_exist) {
                    response.ec = std::make_error_code(error::common_errc::index_not_found);
                }
            } else if (bucket_not_found) {
                response.ec = std::make_error_code(error::common_errc::bucket_not_found);
            } else if (!response.errors.empty()) {
                response.ec = std::make_error_code(error::common_errc::internal_server_failure);
            }
        }
    }
    return response;
}

} // namespace couchbase::operations
