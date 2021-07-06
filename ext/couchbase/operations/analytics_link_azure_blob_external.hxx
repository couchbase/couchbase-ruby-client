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
#include <map>
#include <string>

#include <errors.hxx>
#include <utils/url_codec.hxx>

namespace couchbase::operations::analytics_link
{
/**
 * An external analytics link which uses the Microsoft Azure Blob Storage service.
 * Only available as of 7.0 Developer Preview.
 */
struct azure_blob_external {
    /**
     * The name of this link.
     */
    std::string link_name{};

    /**
     * The dataverse that this link belongs to.
     */
    std::string dataverse{};

    /**
     * The connection string can be used as an authentication method, connectionString contains other authentication methods embedded inside
     * the string. Only a single authentication method can be used. (e.g. "AccountName=myAccountName;AccountKey=myAccountKey").
     */
    std::optional<std::string> connection_string{};

    /**
     * Azure blob storage account name
     */
    std::optional<std::string> account_name{};

    /**
     * Azure blob storage account key
     */
    std::optional<std::string> account_key{};

    /**
     * Token that can be used for authentication
     */
    std::optional<std::string> shared_access_signature{};

    /**
     * Azure blob storage endpoint
     */
    std::optional<std::string> blob_endpoint{};

    /**
     * Azure blob endpoint suffix
     */
    std::optional<std::string> endpoint_suffix{};

    [[nodiscard]] std::error_code validate() const
    {
        if (dataverse.empty() || link_name.empty()) {
            return error::common_errc::invalid_argument;
        }
        if (connection_string.has_value() ||
            (account_name.has_value() && (account_key.has_value() || shared_access_signature.has_value()))) {
            return {};
        }
        return error::common_errc::invalid_argument;
    }

    [[nodiscard]] std::string encode() const
    {
        std::map<std::string, std::string> values{
            { "type", "azureblob" },
        };
        if (std::count(dataverse.begin(), dataverse.end(), '/') == 0) {
            values["dataverse"] = dataverse;
            values["name"] = link_name;
        }
        if (connection_string) {
            values["connectionString"] = connection_string.value();
        } else if (account_name) {
            values["accountName"] = account_name.value();
            if (account_key) {
                values["accountKey"] = account_key.value();
            } else if (shared_access_signature) {
                values["sharedAccessSignature"] = shared_access_signature.value();
            }
        }
        if (blob_endpoint) {
            values["blobEndpoint"] = blob_endpoint.value();
        }
        if (endpoint_suffix) {
            values["endpointSuffix"] = endpoint_suffix.value();
        }
        return utils::string_codec::v2::form_encode(values);
    }
};
} // namespace couchbase::operations::analytics_link

namespace tao::json
{
template<>
struct traits<couchbase::operations::analytics_link::azure_blob_external> {
    template<template<typename...> class Traits>
    static couchbase::operations::analytics_link::azure_blob_external as(const tao::json::basic_value<Traits>& v)
    {
        couchbase::operations::analytics_link::azure_blob_external result{};

        result.link_name = v.at("name").get_string();
        if (const auto* dataverse = v.find("dataverse"); dataverse != nullptr) {
            result.dataverse = dataverse->get_string();
        } else {
            result.dataverse = v.at("scope").get_string();
        }

        if (const auto* account_name = v.find("accountName"); account_name != nullptr && account_name->is_string()) {
            result.account_name.emplace(account_name->get_string());
        }
        if (const auto* blob_endpoint = v.find("blobEndpoint"); blob_endpoint != nullptr && blob_endpoint->is_string()) {
            result.blob_endpoint.emplace(blob_endpoint->get_string());
        }
        if (const auto* endpoint_suffix = v.find("endpointSuffix"); endpoint_suffix != nullptr && endpoint_suffix->is_string()) {
            result.endpoint_suffix.emplace(endpoint_suffix->get_string());
        }
        return result;
    }
};
} // namespace tao::json
