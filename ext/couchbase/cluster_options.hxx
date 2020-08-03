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

#include <timeout_defaults.hxx>

namespace couchbase
{
struct cluster_options {
    std::chrono::milliseconds bootstrap_timeout = timeout_defaults::bootstrap_timeout;
    std::chrono::milliseconds connect_timeout = timeout_defaults::connect_timeout;
    std::chrono::milliseconds key_value_timeout = timeout_defaults::key_value_timeout;
    std::chrono::milliseconds key_value_durable_timeout = timeout_defaults::key_value_durable_timeout;
    std::chrono::milliseconds view_timeout = timeout_defaults::view_timeout;
    std::chrono::milliseconds query_timeout = timeout_defaults::query_timeout;
    std::chrono::milliseconds analytics_timeout = timeout_defaults::analytics_timeout;
    std::chrono::milliseconds search_timeout = timeout_defaults::search_timeout;
    std::chrono::milliseconds management_timeout = timeout_defaults::management_timeout;
    std::chrono::milliseconds dns_srv_timeout = timeout_defaults::dns_srv_timeout;

    bool enable_tls { false };
    std::string trust_certificate{};
    bool enable_mutation_tokens { true };
    bool enable_tcp_keep_alive { true };
    bool force_ipv4 { false };

    std::chrono::milliseconds tcp_keep_alive_interval = timeout_defaults::tcp_keep_alive_interval;
    std::chrono::milliseconds config_poll_interval = timeout_defaults::config_poll_interval;
    std::chrono::milliseconds config_poll_floor = timeout_defaults::config_poll_floor;
    std::chrono::milliseconds config_idle_redial_timeout = timeout_defaults::config_idle_redial_timeout;

    size_t max_http_connections {0};
    std::chrono::milliseconds idle_http_connection_timeout = timeout_defaults::idle_http_connection_timeout;
};

} // namespace couchbase
