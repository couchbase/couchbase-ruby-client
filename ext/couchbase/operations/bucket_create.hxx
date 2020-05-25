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

struct bucket_create_response {
    std::error_code ec;
    std::string error_message{};
};

struct bucket_create_request {
    using response_type = bucket_create_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;

    static const inline service_type type = service_type::management;

    bucket_settings bucket{};

    void encode_to(encoded_request_type& encoded)
    {
        encoded.method = "POST";
        encoded.path = fmt::format("/pools/default/buckets");

        encoded.headers["content-type"] = "application/x-www-form-urlencoded";
        encoded.body.append(fmt::format("name={}", utils::string_codec::form_encode(bucket.name)));
        switch (bucket.bucket_type) {
            case bucket_settings::bucket_type::couchbase:
                encoded.body.append("&bucketType=couchbase");
                break;
            case bucket_settings::bucket_type::memcached:
                encoded.body.append("&bucketType=memcached");
                break;
            case bucket_settings::bucket_type::ephemeral:
                encoded.body.append("&bucketType=ephemeral");
                break;
            case bucket_settings::bucket_type::unknown:
                break;
        }
        encoded.body.append(fmt::format("&ramQuotaMB={}", bucket.ram_quota_mb));
        encoded.body.append(fmt::format("&replicaNumber={}", bucket.num_replicas));
        encoded.body.append(fmt::format("&maxTTL={}", bucket.max_expiry));
        encoded.body.append(fmt::format("&replicaIndex={}", bucket.replica_indexes ? "1" : "0"));
        encoded.body.append(fmt::format("&flushEnabled={}", bucket.flush_enabled ? "1" : "0"));
        switch (bucket.ejection_policy) {
            case bucket_settings::ejection_policy::full:
                encoded.body.append("&evictionPolicy=fullEviction");
                break;
            case bucket_settings::ejection_policy::value_only:
                encoded.body.append("&evictionPolicy=valueOnly");
                break;
            case bucket_settings::ejection_policy::unknown:
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
        switch (bucket.conflict_resolution_type) {
            case bucket_settings::conflict_resolution_type::timestamp:
                encoded.body.append("&conflictResolutionType=lww");
                break;
            case bucket_settings::conflict_resolution_type::sequence_number:
                encoded.body.append("&conflictResolutionType=seqno");
                break;
            case bucket_settings::conflict_resolution_type::unknown:
                break;
        }
    }
};

bucket_create_response
make_response(std::error_code ec, bucket_create_request&, bucket_create_request::encoded_response_type encoded)
{
    bucket_create_response response{ ec };
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
