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

#include <string>
#include <map>
#include <vector>
#include <chrono>

#include <spdlog/fmt/fmt.h>

#include <service_type.hxx>

namespace couchbase::diag
{
enum class cluster_state {
    /** all nodes and their sockets are reachable */
    online,
    /** at least one socket per service is reachable */
    degraded,
    /** not even one socket per service is reachable */
    offline,
};

enum class endpoint_state {
    /** the endpoint is not reachable */
    disconnected,
    /** currently connecting (includes auth, ...) */
    connecting,
    /** connected and ready */
    connected,
    /** disconnecting (after being connected) */
    disconnecting,
};

struct endpoint_diag_info {
    service_type type;
    std::string id;
    std::optional<std::chrono::microseconds> last_activity{};
    std::string remote;
    std::string local;
    endpoint_state state;
    /** serialized as "namespace" */
    std::optional<std::string> bucket{};
    std::optional<std::string> details{};
};

struct diagnostics_result {
    std::string id;
    std::string sdk;
    std::map<service_type, std::vector<endpoint_diag_info>> services{};

    const int version{ 2 };
};

enum class ping_state {
    ok,
    timeout,
    error,
};

struct endpoint_ping_info {
    service_type type;
    std::string id;
    std::chrono::microseconds latency;
    std::string remote;
    std::string local;
    ping_state state;
    /** serialized as "namespace" */
    std::optional<std::string> bucket{};
    /** if ping state is error, contains error message */
    std::optional<std::string> error{};
};

struct ping_result {
    std::string id;
    std::string sdk;
    std::map<service_type, std::vector<endpoint_ping_info>> services{};

    const int version{ 2 };
};
} // namespace couchbase::diag

template<>
struct fmt::formatter<couchbase::diag::cluster_state> : formatter<std::string_view> {
    template<typename FormatContext>
    auto format(couchbase::diag::cluster_state state, FormatContext& ctx)
    {
        string_view name = "unknown";
        switch (state) {
            case couchbase::diag::cluster_state::online:
                name = "online";
                break;

            case couchbase::diag::cluster_state::degraded:
                name = "degraded";
                break;

            case couchbase::diag::cluster_state::offline:
                name = "offline";
                break;
        }
        return formatter<string_view>::format(name, ctx);
    }
};

template<>
struct fmt::formatter<couchbase::diag::endpoint_state> : formatter<std::string_view> {
    template<typename FormatContext>
    auto format(couchbase::diag::endpoint_state state, FormatContext& ctx)
    {
        string_view name = "unknown";
        switch (state) {
            case couchbase::diag::endpoint_state::disconnected:
                name = "disconnected";
                break;

            case couchbase::diag::endpoint_state::connecting:
                name = "connecting";
                break;

            case couchbase::diag::endpoint_state::connected:
                name = "connected";
                break;

            case couchbase::diag::endpoint_state::disconnecting:
                name = "disconnecting";
                break;
        }
        return formatter<string_view>::format(name, ctx);
    }
};

template<>
struct fmt::formatter<couchbase::diag::ping_state> : formatter<std::string_view> {
    template<typename FormatContext>
    auto format(couchbase::diag::ping_state state, FormatContext& ctx)
    {
        string_view name = "unknown";
        switch (state) {
            case couchbase::diag::ping_state::ok:
                name = "ok";
                break;

            case couchbase::diag::ping_state::timeout:
                name = "timeout";
                break;

            case couchbase::diag::ping_state::error:
                name = "error";
                break;
        }
        return formatter<string_view>::format(name, ctx);
    }
};

namespace tao::json
{
template<>
struct traits<couchbase::diag::diagnostics_result> {
    template<template<typename...> class Traits>
    static void assign(tao::json::basic_value<Traits>& v, const couchbase::diag::diagnostics_result& r)
    {
        tao::json::value services = tao::json::empty_object;
        for (const auto& entry : r.services) {
            tao::json::value service{};
            for (const auto& endpoint : entry.second) {
                tao::json::value e = tao::json::empty_object;
                if (endpoint.last_activity) {
                    e["last_activity_us"] = endpoint.last_activity->count();
                }
                e["remote"] = endpoint.remote;
                e["local"] = endpoint.local;
                e["id"] = endpoint.id;
                e["state"] = fmt::format("{}", endpoint.state);
                if (endpoint.bucket) {
                    e["namespace"] = endpoint.bucket.value();
                }
                if (endpoint.details) {
                    e["details"] = endpoint.details.value();
                }
                service.push_back(e);
            }
            services[fmt::format("{}", entry.first)] = service;
        }

        v = {
            { "version", r.version },
            { "id", r.id },
            { "sdk", r.sdk },
            { "services", services },
        };
    }
};
} // namespace tao::json

namespace tao::json
{
template<>
struct traits<couchbase::diag::ping_result> {
    template<template<typename...> class Traits>
    static void assign(tao::json::basic_value<Traits>& v, const couchbase::diag::ping_result& r)
    {
        tao::json::value services{};
        for (const auto& entry : r.services) {
            tao::json::value service{};
            for (const auto& endpoint : entry.second) {
                tao::json::value e{};
                e["latency_us"] = endpoint.latency.count();
                e["remote"] = endpoint.remote;
                e["local"] = endpoint.local;
                e["id"] = endpoint.id;
                e["state"] = fmt::format("{}", endpoint.state);
                if (endpoint.bucket) {
                    e["namespace"] = endpoint.bucket.value();
                }
                if (endpoint.state == couchbase::diag::ping_state::error && endpoint.error) {
                    e["error"] = endpoint.error.value();
                }
                service.push_back(e);
            }
            services[fmt::format("{}", entry.first)] = service;
        }

        v = {
            { "version", r.version },
            { "id", r.id },
            { "sdk", r.sdk },
            { "services", services },
        };
    }
};
} // namespace tao::json
