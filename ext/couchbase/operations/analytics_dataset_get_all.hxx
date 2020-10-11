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
struct analytics_dataset_get_all_response {
    struct dataset {
        std::string name;
        std::string dataverse_name;
        std::string link_name;
        std::string bucket_name;
    };

    struct problem {
        std::uint32_t code;
        std::string message;
    };

    std::string client_context_id;
    std::error_code ec;
    std::string status{};
    std::vector<dataset> datasets{};
    std::vector<problem> errors{};
};

struct analytics_dataset_get_all_request {
    using response_type = analytics_dataset_get_all_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;

    static const inline service_type type = service_type::analytics;

    std::string client_context_id{ uuid::to_string(uuid::random()) };
    std::chrono::milliseconds timeout{ timeout_defaults::management_timeout };

    [[nodiscard]] std::error_code encode_to(encoded_request_type& encoded, http_context&)
    {
        tao::json::value body{
            { "statement", "SELECT d.* FROM Metadata.`Dataset` d WHERE d.DataverseName <> \"Metadata\"" },
        };
        encoded.headers["content-type"] = "application/json";
        encoded.method = "POST";
        encoded.path = "/analytics/service";
        encoded.body = tao::json::to_string(body);
        return {};
    }
};

analytics_dataset_get_all_response
make_response(std::error_code ec,
              analytics_dataset_get_all_request& request,
              analytics_dataset_get_all_request::encoded_response_type&& encoded)
{
    analytics_dataset_get_all_response response{ request.client_context_id, ec };

    if (!ec) {
        auto payload = tao::json::from_string(encoded.body);
        response.status = payload.at("status").get_string();
        if (response.status == "success") {
            auto* results = payload.find("results");
            if (results != nullptr && results->is_array()) {
                for (const auto& res : results->get_array()) {
                    analytics_dataset_get_all_response::dataset ds;
                    ds.name = res.at("DatasetName").get_string();
                    ds.dataverse_name = res.at("DataverseName").get_string();
                    ds.link_name = res.at("LinkName").get_string();
                    ds.bucket_name = res.at("BucketName").get_string();
                    response.datasets.emplace_back(ds);
                }
            }
        } else {
            auto* errors = payload.find("errors");
            if (errors != nullptr && errors->is_array()) {
                for (const auto& error : errors->get_array()) {
                    analytics_dataset_get_all_response::problem err{
                        error.at("code").as<std::uint32_t>(),
                        error.at("msg").get_string(),
                    };
                    response.errors.emplace_back(err);
                }
            }
            response.ec = std::make_error_code(error::common_errc::internal_server_failure);
        }
    }
    return response;
}

} // namespace couchbase::operations
