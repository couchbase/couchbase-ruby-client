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

#include <tao/json/external/pegtl.hpp>
#include <tao/json/external/pegtl/contrib/uri.hpp>

#include <cluster_options.hxx>

namespace couchbase::utils
{

struct connection_string {
    enum class bootstrap_mode {
        unspecified,
        gcccp,
        http,
    };

    enum class address_type {
        ipv4,
        ipv6,
        dns,
    };

    struct node {
        std::string address;
        std::uint16_t port;
        address_type type;
        bootstrap_mode mode{ bootstrap_mode::unspecified };
    };

    std::string scheme{};
    bool tls{ false };
    std::map<std::string, std::string> params{};
    cluster_options options{};

    std::vector<node> bootstrap_nodes{};

    std::optional<std::string> default_bucket_name{};
    bootstrap_mode default_mode{ bootstrap_mode::unspecified };
    std::uint16_t default_port{ 0 };

    std::optional<std::string> error{};
};

namespace priv
{
using namespace tao::json::pegtl;

struct bucket_name : seq<uri::segment_nz> {
};
using param_key = star<sor<abnf::ALPHA, abnf::DIGIT, one<'_'>>>;
using param_value = star<sor<minus<uri::pchar, one<'=', '&', '?'>>, one<'/'>>>;
struct param : seq<param_key, one<'='>, param_value> {
};

using sub_delims = minus<uri::sub_delims, one<',', '='>>; // host and mode separators
struct reg_name : star<sor<uri::unreserved, uri::pct_encoded, sub_delims>> {
};
struct host : sor<uri::IP_literal, uri::IPv4address, reg_name> {
};

struct mode : sor<istring<'c', 'c', 'c', 'p'>, istring<'g', 'c', 'c', 'c', 'p'>, istring<'h', 't', 't', 'p'>, istring<'m', 'c', 'd'>> {
};
using node = seq<host, opt<uri::colon, uri::port>, opt<one<'='>, mode>>;

using opt_bucket_name = opt_must<one<'/'>, bucket_name>;
using opt_params = opt_must<one<'?'>, list_must<param, one<'&'>>>;
using opt_nodes = seq<list_must<node, one<',', ';'>>, opt_bucket_name>;

using grammar = must<seq<uri::scheme, one<':'>, uri::dslash, opt_nodes, opt_params, eof>>;

template<typename Rule>
struct action {
};

template<>
struct action<uri::scheme> {
    template<typename ActionInput>
    static void apply(const ActionInput& in, connection_string& cs, connection_string::node& /* cur_node */)
    {
        cs.scheme = in.string();
        if (cs.scheme == "couchbase") {
            cs.default_port = 11210;
            cs.default_mode = connection_string::bootstrap_mode::gcccp;
            cs.tls = false;
        } else if (cs.scheme == "couchbases") {
            cs.default_port = 11207;
            cs.default_mode = connection_string::bootstrap_mode::gcccp;
            cs.tls = true;
        } else if (cs.scheme == "http") {
            cs.default_port = 8091;
            cs.default_mode = connection_string::bootstrap_mode::http;
            cs.tls = false;
        } else if (cs.scheme == "https") {
            cs.default_port = 18091;
            cs.default_mode = connection_string::bootstrap_mode::http;
            cs.tls = true;
        }
    }
};

template<>
struct action<param> {
    template<typename ActionInput>
    static void apply(const ActionInput& in, connection_string& cs, connection_string::node& /* cur_node */)
    {
        const auto& pair = in.string();
        auto eq = pair.find('=');
        std::string key = pair.substr(0, eq);
        cs.params[key] = (eq == std::string::npos) ? "" : pair.substr(eq + 1);
    }
};

template<>
struct action<reg_name> {
    template<typename ActionInput>
    static void apply(const ActionInput& in, connection_string& /* cs */, connection_string::node& cur_node)
    {
        cur_node.type = connection_string::address_type::dns;
        cur_node.address = in.string_view();
    }
};

template<>
struct action<uri::IPv4address> {
    template<typename ActionInput>
    static void apply(const ActionInput& in, connection_string& /* cs */, connection_string::node& cur_node)
    {
        cur_node.type = connection_string::address_type::ipv4;
        cur_node.address = in.string_view();
    }
};

template<>
struct action<uri::IPv6address> {
    template<typename ActionInput>
    static void apply(const ActionInput& in, connection_string& /* cs */, connection_string::node& cur_node)
    {
        cur_node.type = connection_string::address_type::ipv6;
        cur_node.address = in.string_view();
    }
};

template<>
struct action<node> {
    template<typename ActionInput>
    static void apply(const ActionInput& /* in */, connection_string& cs, connection_string::node& cur_node)
    {
        cs.bootstrap_nodes.push_back(cur_node);
        cur_node = {};
    }
};

template<>
struct action<uri::port> {
    template<typename ActionInput>
    static void apply(const ActionInput& in, connection_string& /* cs */, connection_string::node& cur_node)
    {
        cur_node.port = static_cast<std::uint16_t>(std::stoul(in.string()));
    }
};

template<>
struct action<mode> {
    template<typename ActionInput>
    static void apply(const ActionInput& in, connection_string& /* cs */, connection_string::node& cur_node)
    {
        std::string mode = in.string();
        std::transform(mode.begin(), mode.end(), mode.begin(), [](unsigned char c) { return std::tolower(c); });
        if (mode == "mcd" || mode == "gcccp" || mode == "cccp") {
            cur_node.mode = connection_string::bootstrap_mode::gcccp;
        } else if (mode == "http") {
            cur_node.mode = connection_string::bootstrap_mode::http;
        }
    }
};

template<>
struct action<bucket_name> {
    template<typename ActionInput>
    static void apply(const ActionInput& in, connection_string& cs, connection_string::node& /* cur_node */)
    {
        cs.default_bucket_name = in.string();
    }
};
} // namespace priv

static void
extract_options(connection_string& connstr)
{
    connstr.options.enable_tls = connstr.tls;
    if (connstr.bootstrap_nodes.size() != 1 || connstr.bootstrap_nodes[0].type != connection_string::address_type::dns) {
        connstr.options.enable_dns_srv = false;
    }
    for (const auto& param : connstr.params) {
        try {
            if (param.first == "kv_connect_timeout") {
                /**
                 * Number of seconds the client should wait while attempting to connect to a node’s KV service via a socket.  Initial
                 * connection, reconnecting, node added, etc.
                 */
                connstr.options.connect_timeout = std::chrono::milliseconds(std::stoull(param.second));
            } else if (param.first == "kv_timeout") {
                /**
                 * Number of milliseconds to wait before timing out a KV operation by the client.
                 */
                connstr.options.key_value_timeout = std::chrono::milliseconds(std::stoull(param.second));
            } else if (param.first == "kv_durable_timeout") {
                /**
                 * Number of milliseconds to wait before timing out a KV operation that is either using synchronous durability or
                 * observe-based durability.
                 */
                connstr.options.key_value_durable_timeout = std::chrono::milliseconds(std::stoull(param.second));
            } else if (param.first == "view_timeout") {
                /**
                 * Number of seconds to wait before timing out a View request  by the client..
                 */
                connstr.options.view_timeout = std::chrono::milliseconds(std::stoull(param.second));
            } else if (param.first == "query_timeout") {
                /**
                 * Number of seconds to wait before timing out a Query or N1QL request by the client.
                 */
                connstr.options.query_timeout = std::chrono::milliseconds(std::stoull(param.second));
            } else if (param.first == "analytics_timeout") {
                /**
                 * Number of seconds to wait before timing out an Analytics request by the client.
                 */
                connstr.options.analytics_timeout = std::chrono::milliseconds(std::stoull(param.second));
            } else if (param.first == "search_timeout") {
                /**
                 * Number of seconds to wait before timing out a Search request by the client.
                 */
                connstr.options.search_timeout = std::chrono::milliseconds(std::stoull(param.second));
            } else if (param.first == "management_timeout") {
                /**
                 * Number of seconds to wait before timing out a Management API request by the client.
                 */
                connstr.options.management_timeout = std::chrono::milliseconds(std::stoull(param.second));
            } else if (param.first == "trust_certificate") {
                connstr.options.trust_certificate = param.second;
            } else if (param.first == "enable_mutation_tokens") {
                /**
                 * Request mutation tokens at connection negotiation time. Turning this off will save 16 bytes per operation response.
                 */
                if (param.second == "true" || param.second == "yes" || param.second == "on") {
                    connstr.options.enable_mutation_tokens = true;
                } else if (param.second == "false" || param.second == "no" || param.second == "off") {
                    connstr.options.enable_mutation_tokens = false;
                }
            } else if (param.first == "enable_tcp_keep_alive") {
                /**
                 * Gets or sets a value indicating whether enable TCP keep-alive.
                 */
                if (param.second == "true" || param.second == "yes" || param.second == "on") {
                    connstr.options.enable_tcp_keep_alive = true;
                } else if (param.second == "false" || param.second == "no" || param.second == "off") {
                    connstr.options.enable_tcp_keep_alive = false;
                }
            } else if (param.first == "tcp_keep_alive_interval") {
                /**
                 * Specifies the timeout, in milliseconds, with no activity until the first keep-alive packet is sent. This applies to all
                 * services, but is advisory: if the underlying platform does not support this on all connections, it will be applied only
                 * on those it can be.
                 */
                connstr.options.tcp_keep_alive_interval = std::chrono::milliseconds(std::stoull(param.second));
            } else if (param.first == "force_ipv4") {
                /**
                 * Sets the SDK configuration to do IPv4 Name Resolution
                 */
                if (param.second == "true" || param.second == "yes" || param.second == "on") {
                    connstr.options.force_ipv4 = true;
                } else if (param.second == "false" || param.second == "no" || param.second == "off") {
                    connstr.options.force_ipv4 = false;
                }
            } else if (param.first == "config_poll_interval") {
                connstr.options.config_poll_interval = std::chrono::milliseconds(std::stoull(param.second));
            } else if (param.first == "config_poll_floor") {
                connstr.options.config_poll_floor = std::chrono::milliseconds(std::stoull(param.second));
            } else if (param.first == "max_http_connections") {
                /**
                 * The maximum number of HTTP connections allowed on a per-host and per-port basis.  0 indicates an unlimited number of
                 * connections are permitted.
                 */
                connstr.options.max_http_connections = std::stoul(param.second);
            } else if (param.first == "idle_http_connection_timeout") {
                /**
                 * The period of time an HTTP connection can be idle before it is forcefully disconnected.
                 */
                connstr.options.idle_http_connection_timeout = std::chrono::milliseconds(std::stoull(param.second));
            } else if (param.first == "enable_dns_srv") {
                if (connstr.bootstrap_nodes.size() == 1) {
                    if (param.second == "true" || param.second == "yes" || param.second == "on") {
                        connstr.options.enable_dns_srv = true;
                    } else if (param.second == "false" || param.second == "no" || param.second == "off") {
                        connstr.options.enable_dns_srv = false;
                    }
                } else {
                    spdlog::warn(
                      R"(parameter "{}" require single entry in bootstrap nodes list of the connection string, ignoring (value "{}"))",
                      param.first,
                      param.second);
                }
            } else if (param.first == "network") {
                connstr.options.network = param.second; /* current known values are "auto", "default" and "external" */
            } else if (param.first == "show_queries") {
                /**
                 * Whether to display N1QL, Analytics, Search queries on info level (default false)
                 */
                if (param.second == "true" || param.second == "yes" || param.second == "on") {
                    connstr.options.show_queries = true;
                } else if (param.second == "false" || param.second == "no" || param.second == "off") {
                    connstr.options.show_queries = false;
                }
            } else {
                spdlog::warn(R"(unknown parameter "{}" in connection string (value "{}"))", param.first, param.second);
            }
        } catch (std::invalid_argument& ex1) {
            spdlog::warn(R"(unable to parse "{}" parameter in connection string (value "{}" cannot be converted): {})",
                         param.first,
                         param.second,
                         ex1.what());
        } catch (std::out_of_range& ex2) {
            spdlog::warn(R"(unable to parse "{}" parameter in connection string (value "{}" is out of range): {})",
                         param.first,
                         param.second,
                         ex2.what());
        }
    }
}

static connection_string
parse_connection_string(const std::string& input)
{
    connection_string res;

    if (input.empty()) {
        res.error = "failed to parse connection string: empty input";
        return res;
    }

    auto in = tao::json::pegtl::memory_input(input, __FUNCTION__);
    try {
        connection_string::node node{};
        tao::json::pegtl::parse<priv::grammar, priv::action>(in, res, node);
    } catch (tao::json::pegtl::parse_error& e) {
        for (const auto& position : e.positions) {
            if (position.source == __FUNCTION__) {
                res.error = fmt::format(
                  "failed to parse connection string (column: {}, trailer: \"{}\")", position.byte_in_line, input.substr(position.byte));
                break;
            }
        }
        if (!res.error) {
            res.error = e.what();
        }
    }
    extract_options(res);
    return res;
}
} // namespace couchbase::utils
