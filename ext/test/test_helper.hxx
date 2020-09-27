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

#include <generated_config.hxx>

#include <string>
#include <regex>
#include <cstdlib>

#define CATCH_CONFIG_MAIN
#include <catch2/catch.hpp>

struct test_server_version {
    unsigned long major;
    unsigned long minor;
    unsigned long micro{ 0 };
    unsigned long build{ 0 };
    bool developer_preview{ false };

    static test_server_version parse(const std::string& str)
    {
        std::regex version_regex(R"((\d+).(\d+).(\d+(-(\d+))?)?)");
        std::smatch version_match;
        test_server_version ver{};
        if (std::regex_match(str, version_match, version_regex) && version_match.ready()) {
            ver.major = std::stoul(version_match[1]);
            ver.minor = std::stoul(version_match[2]);
            if (version_match.length(3) > 0) {
                ver.micro = std::stoul(version_match[3]);
                if (version_match.length(5) > 0) {
                    ver.build = std::stoul(version_match[5]);
                }
            }
        } else {
            ver.major = 6;
            ver.minor = 6;
            ver.micro = 0;
        }
        return ver;
    }

    [[nodiscard]] bool is_alice() const
    {
        // [6.0.0, 6.5.0)
        return major == 6 && minor < 5;
    }

    [[nodiscard]] bool is_mad_hatter() const
    {
        // [6.5.0, 7.0.0)
        return major == 6 && minor >= 5;
    }

    [[nodiscard]] bool is_cheshire_cat() const
    {
        // [7.0.0, inf)
        return major >= 7;
    }

    [[nodiscard]] bool supports_gcccp() const
    {
        return is_mad_hatter() || is_cheshire_cat();
    }

    [[nodiscard]] bool supports_sync_replication() const
    {
        return is_mad_hatter() || is_cheshire_cat();
    }

    [[nodiscard]] bool supports_scoped_queries() const
    {
        return is_cheshire_cat();
    }

    [[nodiscard]] bool supports_collections() const
    {
        return (is_mad_hatter() && developer_preview) || is_cheshire_cat();
    }
};

struct test_context {
    std::string connection_string{ "couchbase://127.0.0.1" };
    std::string username{ "Administrator" };
    std::string password{ "password" };
    std::string bucket{ "default" };
    test_server_version version{ 6, 6, 0 };

    static test_context load_from_environment()
    {
        test_context ctx{};

        const char* var = nullptr;

        var = getenv("TEST_CONNECTION_STRING");
        if (var != nullptr) {
            ctx.connection_string = var;
        }
        var = getenv("TEST_USERNAME");
        if (var != nullptr) {
            ctx.username = var;
        }
        var = getenv("TEST_PASSWORD");
        if (var != nullptr) {
            ctx.password = var;
        }
        var = getenv("TEST_BUCKET");
        if (var != nullptr) {
            ctx.bucket = var;
        }
        var = getenv("TEST_SERVER_VERSION");
        if (var != nullptr) {
            ctx.bucket = var;
        }
        var = getenv("TEST_DEVELOPER_PREVIEW");
        if (var != nullptr) {
            if (strcmp(var, "true") == 0 || strcmp(var, "yes") == 0 || strcmp(var, "1") == 0) {
                ctx.version.developer_preview = true;
            } else if (strcmp(var, "false") == 0 || strcmp(var, "no") == 0 || strcmp(var, "0") == 0) {
                ctx.version.developer_preview = false;
            }
        }

        return ctx;
    }
};
