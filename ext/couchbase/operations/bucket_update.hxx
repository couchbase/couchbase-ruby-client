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
#include <utils/url_codec.hxx>

namespace couchbase::operations
{

struct bucket_update_response {
    std::string client_context_id;
    std::error_code ec;
    bucket_settings bucket{};
    std::string error_message{};
};

struct bucket_update_request {
    using response_type = bucket_update_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;

    static const inline service_type type = service_type::management;
    std::chrono::milliseconds timeout{ timeout_defaults::management_timeout };
    std::string client_context_id{ uuid::to_string(uuid::random()) };

    bucket_settings bucket{};

    [[nodiscard]] std::error_code encode_to(encoded_request_type& encoded, http_context&)
    {
        encoded.method = "POST";
        encoded.path = fmt::format("/pools/default/buckets/{}", bucket.name);

        encoded.headers["content-type"] = "application/x-www-form-urlencoded";
        encoded.body.append(fmt::format("&ramQuotaMB={}", bucket.ram_quota_mb));
        encoded.body.append(fmt::format("&replicaNumber={}", bucket.num_replicas));
        encoded.body.append(fmt::format("&maxTTL={}", bucket.max_expiry));
        encoded.body.append(fmt::format("&replicaIndex={}", bucket.replica_indexes ? "1" : "0"));
        encoded.body.append(fmt::format("&flushEnabled={}", bucket.flush_enabled ? "1" : "0"));
        switch (bucket.eviction_policy) {
            case bucket_settings::eviction_policy::full:
                encoded.body.append("&evictionPolicy=fullEviction");
                break;
            case bucket_settings::eviction_policy::value_only:
                encoded.body.append("&evictionPolicy=valueOnly");
                break;
            case bucket_settings::eviction_policy::no_eviction:
                encoded.body.append("&evictionPolicy=noEviction");
                break;
            case bucket_settings::eviction_policy::not_recently_used:
                encoded.body.append("&evictionPolicy=nruEviction");
                break;
            case bucket_settings::eviction_policy::unknown:
                break;
        }
        switch (bucket.compression_mode) {
            case bucket_settings::compression_mode::off:
                encoded.body.append("&compressionMode=off");
                break;
            case bucket_settings::compression_mode::active:
                encoded.body.append("&compressionMode=active");
                break;
            case bucket_settings::compression_mode::passive:
                encoded.body.append("&compressionMode=passive");
                break;
            case bucket_settings::compression_mode::unknown:
                break;
        }
        return {};
    }
};

bucket_update_response
make_response(std::error_code ec, bucket_update_request& request, bucket_update_request::encoded_response_type encoded)
{
    bucket_update_response response{ request.client_context_id, ec };
    if (!ec) {
        switch (encoded.status_code) {
            case 404:
                response.ec = std::make_error_code(error::common_errc::bucket_not_found);
                break;
            case 400: {
                response.ec = std::make_error_code(error::common_errc::invalid_argument);
                auto payload = tao::json::from_string(encoded.body);
                auto* errors = payload.find("errors");
                if (errors != nullptr) {
                    std::vector<std::string> error_list{};
                    for (auto& err : errors->get_object()) {
                        error_list.emplace_back(err.second.get_string());
                    }
                    if (!error_list.empty()) {
                        response.error_message = fmt::format("{}", fmt::join(error_list.begin(), error_list.end(), ". "));
                    }
                }
            } break;
            case 200:
            case 202:
                break;
            default:
                response.ec = std::make_error_code(error::common_errc::internal_server_failure);
                break;
        }
    }
    return response;
}

} // namespace couchbase::operations
