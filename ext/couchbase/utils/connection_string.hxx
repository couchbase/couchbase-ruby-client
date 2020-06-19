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
    return res;
}
} // namespace couchbase::utils
