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
struct analytics_get_pending_mutations_response {
    struct problem {
        std::uint32_t code;
        std::string message;
    };

    std::string client_context_id;
    std::error_code ec;
    std::string status{};
    std::vector<problem> errors{};
    std::map<std::string, std::uint64_t> stats{};
};

struct analytics_get_pending_mutations_request {
    using response_type = analytics_get_pending_mutations_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;

    static const inline service_type type = service_type::analytics;

    std::string client_context_id{ uuid::to_string(uuid::random()) };
    std::chrono::milliseconds timeout{ timeout_defaults::management_timeout };

    void encode_to(encoded_request_type& encoded, http_context&)
    {
        encoded.method = "GET";
        encoded.path = "/analytics/node/agg/stats/remaining";
    }
};

analytics_get_pending_mutations_response
make_response(std::error_code ec,
              analytics_get_pending_mutations_request& request,
              analytics_get_pending_mutations_request::encoded_response_type encoded)
{
    analytics_get_pending_mutations_response response{ request.client_context_id, ec };
    if (!ec) {
        auto payload = tao::json::from_string(encoded.body);
        if (encoded.status_code == 200) {
            if (payload.is_object()) {
                for (const auto& entry : payload.get_object()) {
                    std::string dataverse = entry.first + ".";
                    for (const auto& stats : entry.second.get_object()) {
                        std::string dataset = dataverse + stats.first;
                        response.stats.emplace(dataset, stats.second.get_unsigned());
                    }
                }
            }
            return response;
        }
        auto* errors = payload.find("errors");
        if (errors != nullptr && errors->is_array()) {
            for (const auto& error : errors->get_array()) {
                analytics_get_pending_mutations_response::problem err{
                    error.at("code").as<std::uint32_t>(),
                    error.at("msg").get_string(),
                };
                response.errors.emplace_back(err);
            }
        }
        response.ec = std::make_error_code(error::common_errc::internal_server_failure);
    }
    return response;
}

} // namespace couchbase::operations
