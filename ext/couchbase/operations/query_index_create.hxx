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
struct query_index_create_response {
    struct query_problem {
        std::uint64_t code;
        std::string message;
    };
    uuid::uuid_t client_context_id;
    std::error_code ec;
    std::string status{};
    std::vector<query_problem> errors{};
};

struct query_index_create_request {
    using response_type = query_index_create_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;

    static const inline service_type type = service_type::query;

    uuid::uuid_t client_context_id{ uuid::random() };
    std::string bucket_name;
    std::string index_name{};
    std::vector<std::string> fields;
    bool is_primary{ false };
    bool ignore_if_exists{ false };
    std::optional<std::string> condition{};
    std::optional<bool> deferred{};
    std::optional<int> num_replicas{};

    void encode_to(encoded_request_type& encoded)
    {
        encoded.headers["content-type"] = "application/json";
        tao::json::value with{};
        if (deferred) {
            with["defer_build"] = *deferred;
        }
        if (num_replicas) {
            with["num_replica"] = *num_replicas; /* no 's' in key name */
        }
        std::string where_clause{};
        if (condition) {
            where_clause = fmt::format("WHERE {}", *condition);
        }
        std::string with_clause{};
        if (with) {
            with_clause = fmt::format("WITH {}", tao::json::to_string(with));
        }
        tao::json::value body{ { "statement",
                                 is_primary ? fmt::format(R"(CREATE PRIMARY INDEX {} ON `{}` USING GSI {})",
                                                          index_name.empty() ? "" : fmt::format("`{}`", index_name),
                                                          bucket_name,
                                                          with_clause)
                                            : fmt::format(R"(CREATE INDEX `{}` ON `{}`({}) {} USING GSI {})",
                                                          index_name,
                                                          bucket_name,
                                                          fmt::join(fields, ", "),
                                                          where_clause,
                                                          with_clause) },
                               { "client_context_id", uuid::to_string(client_context_id) } };
        encoded.method = "POST";
        encoded.path = "/query/service";
        encoded.body = tao::json::to_string(body);
    }
};

query_index_create_response
make_response(std::error_code ec, query_index_create_request& request, query_index_create_request::encoded_response_type encoded)
{
    query_index_create_response response{ request.client_context_id, ec };
    if (!ec) {
        auto payload = tao::json::from_string(encoded.body);
        response.status = payload.at("status").get_string();

        if (response.status != "success") {
            bool index_already_exists = false;
            bool bucket_not_found = false;
            for (const auto& entry : payload.at("errors").get_array()) {
                query_index_create_response::query_problem error;
                error.code = entry.at("code").get_unsigned();
                error.message = entry.at("msg").get_string();
                switch (error.code) {
                    case 5000: /* IKey: "Internal Error" */
                        if (error.message.find(" already exists") != std::string::npos) {
                            index_already_exists = true;
                        }
                        break;
                    case 12003: /* IKey: "datastore.couchbase.keyspace_not_found" */
                        bucket_not_found = true;
                        break;
                    case 4300: /* IKey: "plan.new_index_already_exists" */
                        index_already_exists = true;
                        break;
                }
                response.errors.emplace_back(error);
            }
            if (index_already_exists) {
                if (!request.ignore_if_exists) {
                    response.ec = std::make_error_code(error::common_errc::index_exists);
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
