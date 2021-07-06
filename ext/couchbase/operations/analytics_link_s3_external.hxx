/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

#include <optional>
#include <sstream>
#include <string>

#include <errors.hxx>
#include <utils/url_codec.hxx>

namespace couchbase::operations::analytics_link
{
/**
 * An external analytics link which uses the AWS S3 service to access data.
 */
struct s3_external {
    /**
     * The name of this link.
     */
    std::string link_name{};

    /**
     * The dataverse that this link belongs to.
     */
    std::string dataverse{};

    /**
     * AWS S3 access key ID
     */
    std::string access_key_id{};

    /**
     * AWS S3 secret key
     */
    std::string secret_access_key{};

    /**
     * AWS S3 token if temporary credentials are provided. Only available in 7.0+
     */
    std::optional<std::string> session_token{};

    /**
     * AWS S3 region
     */
    std::string region{};

    /**
     * AWS S3 service endpoint
     */
    std::optional<std::string> service_endpoint{};

    [[nodiscard]] std::error_code validate() const
    {
        if (dataverse.empty() || link_name.empty() || access_key_id.empty() || secret_access_key.empty() || region.empty()) {
            return error::common_errc::invalid_argument;
        }
        return {};
    }

    [[nodiscard]] std::string encode() const
    {
        std::map<std::string, std::string> values{
            { "type", "s3" },
            { "accessKeyId", access_key_id },
            { "secretAccessKey", secret_access_key },
            { "region", region },
        };
        if (std::count(dataverse.begin(), dataverse.end(), '/') == 0) {
            values["dataverse"] = dataverse;
            values["name"] = link_name;
        }
        if (session_token) {
            values["sessionToken"] = session_token.value();
        }
        if (service_endpoint) {
            values["serviceEndpoint"] = service_endpoint.value();
        }
        return utils::string_codec::v2::form_encode(values);
    }
};
} // namespace couchbase::operations::analytics_link

namespace tao::json
{
template<>
struct traits<couchbase::operations::analytics_link::s3_external> {
    template<template<typename...> class Traits>
    static couchbase::operations::analytics_link::s3_external as(const tao::json::basic_value<Traits>& v)
    {
        couchbase::operations::analytics_link::s3_external result{};

        result.link_name = v.at("name").get_string();
        if (const auto* dataverse = v.find("dataverse"); dataverse != nullptr) {
            result.dataverse = dataverse->get_string();
        } else {
            result.dataverse = v.at("scope").get_string();
        }
        result.access_key_id = v.at("accessKeyId").get_string();
        result.region = v.at("region").get_string();
        if (const auto* service_endpoint = v.find("serviceEndpoint"); service_endpoint != nullptr && service_endpoint->is_string()) {
            result.service_endpoint.emplace(service_endpoint->get_string());
        }
        return result;
    }
};
} // namespace tao::json
