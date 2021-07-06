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
enum class encryption_level {
    /**
     * Connect to the remote Couchbase cluster using an unsecured channel. Send the password in plaintext.
     */
    none,

    /**
     * Connect to the remote Couchbase cluster using an unsecured channel. Send the password securely using SASL.
     */
    half,

    /**
     * Connect to the remote Couchbase cluster using a channel secured by TLS. If a password is used, it is sent over the secure channel.
     *
     * Requires specifying the certificate to trust.
     */
    full,
};

std::string
to_string(encryption_level level)
{
    switch (level) {
        case encryption_level::none:
            return "none";

        case encryption_level::half:
            return "half";

        case encryption_level::full:
            return "full";
    }
    return "none";
}

struct encryption_settings {
    /**
     * Specifies what level of encryption should be used.
     */
    encryption_level level{ encryption_level::none };

    /**
     * Provides a certificate to use for connecting when encryption level is set to 'full'.  Required when 'encryption_level' is set to
     * 'full'.
     */
    std::optional<std::string> certificate{};

    /**
     * Provides a client certificate to use for connecting when encryption level is set to 'full'.  Cannot be set if a username/password are
     * used.
     */
    std::optional<std::string> client_certificate{};

    /**
     * Provides a client key to use for connecting when encryption level is set to 'full'.  Cannot be set if a username/password are used.
     */
    std::optional<std::string> client_key{};
};

/**
 * A remote analytics link which uses a Couchbase data service that is not part of the same cluster as the Analytics Service.
 */
struct couchbase_remote {
    /**
     * The name of this link.
     */
    std::string link_name{};

    /**
     * The dataverse that this link belongs to.
     */
    std::string dataverse{};

    /**
     * The hostname of the target Couchbase cluster.
     */
    std::string hostname{};

    /**
     * The username to use for authentication with the remote cluster. Optional if client-certificate authentication is being used.
     */
    std::optional<std::string> username{};

    /**
     * The password to use for authentication with the remote cluster. Optional if client-certificate authentication is being used.
     */
    std::optional<std::string> password{};

    encryption_settings encryption{};

    [[nodiscard]] std::error_code validate() const
    {
        if (dataverse.empty() || link_name.empty() || hostname.empty()) {
            return error::common_errc::invalid_argument;
        }
        switch (encryption.level) {
            case encryption_level::none:
            case encryption_level::half:
                if (/* username and password must be provided */ username.has_value() && password.has_value() &&
                    /* and client certificate and key must be empty */
                    (!encryption.client_certificate.has_value() && !encryption.client_key.has_value())) {

                    return {};
                }
                return error::common_errc::invalid_argument;

            case encryption_level::full:
                if (/* certificate must be provided and */ encryption.certificate.has_value() &&
                    (/* either username/password must be set */ (username.has_value() && password.has_value() &&
                                                                 !encryption.client_certificate.has_value() &&
                                                                 !encryption.client_key.has_value()) ||
                     /* or client certificate/key must be set */ (!username.has_value() && !password.has_value() &&
                                                                  encryption.client_certificate.has_value() &&
                                                                  encryption.client_key.has_value()))) {
                    return {};
                }
                return error::common_errc::invalid_argument;
        }
        return {};
    }

    [[nodiscard]] std::string encode() const
    {
        std::map<std::string, std::string> values{
            { "type", "couchbase" },
            { "hostname", hostname },
            { "encryption", to_string(encryption.level) },
        };
        if (std::count(dataverse.begin(), dataverse.end(), '/') == 0) {
            values["dataverse"] = dataverse;
            values["name"] = link_name;
        }
        if (username) {
            values["username"] = username.value();
        }
        if (password) {
            values["password"] = password.value();
        }
        if (encryption.certificate) {
            values["certificate"] = encryption.certificate.value();
        }
        if (encryption.client_certificate) {
            values["clientCertificate"] = encryption.client_certificate.value();
        }
        if (encryption.client_key) {
            values["clientKey"] = encryption.client_key.value();
        }
        return utils::string_codec::v2::form_encode(values);
    }
};
} // namespace couchbase::operations::analytics_link

namespace tao::json
{
template<>
struct traits<couchbase::operations::analytics_link::couchbase_remote> {
    template<template<typename...> class Traits>
    static couchbase::operations::analytics_link::couchbase_remote as(const tao::json::basic_value<Traits>& v)
    {
        couchbase::operations::analytics_link::couchbase_remote result{};

        result.link_name = v.at("name").get_string();
        if (const auto* dataverse = v.find("dataverse"); dataverse != nullptr) {
            result.dataverse = dataverse->get_string();
        } else {
            result.dataverse = v.at("scope").get_string();
        }
        result.hostname = v.at("activeHostname").get_string();
        if (const auto* encryption = v.find("encryption"); encryption != nullptr && encryption->is_string()) {
            const auto& level = encryption->get_string();
            if (level == "none") {
                result.encryption.level = couchbase::operations::analytics_link::encryption_level::none;
            } else if (level == "half") {
                result.encryption.level = couchbase::operations::analytics_link::encryption_level::half;
            } else if (level == "full") {
                result.encryption.level = couchbase::operations::analytics_link::encryption_level::full;
            }
        }
        if (const auto* username = v.find("username"); username != nullptr && username->is_string()) {
            result.username.emplace(username->get_string());
        }
        if (const auto* certificate = v.find("certificate"); certificate != nullptr && certificate->is_string()) {
            result.encryption.certificate.emplace(certificate->get_string());
        }
        if (const auto* client_certificate = v.find("clientCertificate");
            client_certificate != nullptr && client_certificate->is_string()) {
            result.encryption.client_certificate.emplace(client_certificate->get_string());
        }
        return result;
    }
};
} // namespace tao::json
