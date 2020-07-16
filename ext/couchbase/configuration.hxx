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

#include <gsl/gsl_util>

#include <tao/json.hpp>
#include <spdlog/spdlog.h>
#include <utils/crc32.hxx>
#include <platform/uuid.h>

#include <service_type.hxx>

namespace couchbase
{
struct configuration {
    struct port_map {
        std::optional<std::uint16_t> key_value{};
        std::optional<std::uint16_t> management{};
        std::optional<std::uint16_t> analytics{};
        std::optional<std::uint16_t> search{};
        std::optional<std::uint16_t> views{};
        std::optional<std::uint16_t> query{};
    };
    struct alternate_address {
        std::string name{};
        std::string hostname{};
        port_map services_plain{};
        port_map services_tls{};
    };
    struct node {
        bool this_node{ false };
        size_t index{};
        std::string hostname{};
        port_map services_plain{};
        port_map services_tls{};
        std::map<std::string, alternate_address> alt{};

        [[nodiscard]] std::uint16_t port_or(service_type type, bool is_tls, std::uint16_t default_value) const
        {
            if (is_tls) {
                switch (type) {
                    case service_type::query:
                        return services_tls.query.value_or(default_value);

                    case service_type::analytics:
                        return services_tls.analytics.value_or(default_value);

                    case service_type::search:
                        return services_tls.search.value_or(default_value);

                    case service_type::views:
                        return services_tls.views.value_or(default_value);

                    case service_type::management:
                        return services_tls.management.value_or(default_value);

                    case service_type::kv:
                        return services_tls.key_value.value_or(default_value);
                }
            }
            switch (type) {
                case service_type::query:
                    return services_plain.query.value_or(default_value);

                case service_type::analytics:
                    return services_plain.analytics.value_or(default_value);

                case service_type::search:
                    return services_plain.search.value_or(default_value);

                case service_type::views:
                    return services_plain.views.value_or(default_value);

                case service_type::management:
                    return services_plain.management.value_or(default_value);

                case service_type::kv:
                    return services_plain.key_value.value_or(default_value);
            }
            return default_value;
        }

        [[nodiscard]] const std::string & hostname_for(const std::string& network) const
        {
            if (network == "default") {
                return hostname;
            }
            const auto& address = alt.find(network);
            if (address == alt.end()) {
                spdlog::warn(R"(requested network "{}" is not found, fallback to "default" host)", network);
                return hostname;
            }
            return address->second.hostname;
        }

        [[nodiscard]] std::uint16_t port_or(const std::string& network, service_type type, bool is_tls, std::uint16_t default_value) const
        {
            if (network == "default") {
                return port_or(type, is_tls, default_value);
            }
            const auto& address = alt.find(network);
            if (address == alt.end()) {
                spdlog::warn(R"(requested network "{}" is not found, fallback to "default" port of {} service)", network, type);
                return port_or(type, is_tls, default_value);
            }
            if (is_tls) {
                switch (type) {
                    case service_type::query:
                        return address->second.services_tls.query.value_or(default_value);

                    case service_type::analytics:
                        return address->second.services_tls.analytics.value_or(default_value);

                    case service_type::search:
                        return address->second.services_tls.search.value_or(default_value);

                    case service_type::views:
                        return address->second.services_tls.views.value_or(default_value);

                    case service_type::management:
                        return address->second.services_tls.management.value_or(default_value);

                    case service_type::kv:
                        return address->second.services_tls.key_value.value_or(default_value);
                }
            }
            switch (type) {
                case service_type::query:
                    return address->second.services_plain.query.value_or(default_value);

                case service_type::analytics:
                    return address->second.services_plain.analytics.value_or(default_value);

                case service_type::search:
                    return address->second.services_plain.search.value_or(default_value);

                case service_type::views:
                    return address->second.services_plain.views.value_or(default_value);

                case service_type::management:
                    return address->second.services_plain.management.value_or(default_value);

                case service_type::kv:
                    return address->second.services_plain.key_value.value_or(default_value);
            }
            return default_value;
        }
    };

    [[nodiscard]] std::string select_network(const std::string& bootstrap_hostname) const
    {
        for (const auto& n : nodes) {
            if (n.this_node) {
                if (n.hostname == bootstrap_hostname) {
                    return "default";
                }
                for (const auto& entry : n.alt) {
                    if (entry.second.hostname == bootstrap_hostname) {
                        return entry.first;
                    }
                }
            }
        }
        return "default";
    }

    using vbucket_map = typename std::vector<std::vector<std::int16_t>>;

    std::uint64_t rev{};
    couchbase::uuid::uuid_t id{};
    std::optional<std::uint32_t> num_replicas{};
    std::vector<node> nodes{};
    std::optional<std::string> uuid{};
    std::optional<std::string> bucket{};
    std::optional<vbucket_map> vbmap{};

    size_t index_for_endpoint(const asio::ip::tcp::endpoint& endpoint)
    {
        auto hostname = endpoint.address().to_string();
        for (const auto& n : nodes) {
            if (n.hostname == hostname) {
                return n.index;
            }
        }
        throw std::runtime_error("unable to locate node for the index");
    }

    [[nodiscard]] std::size_t index_for_this_node() const
    {
        for (const auto& n : nodes) {
            if (n.this_node) {
                return n.index;
            }
        }
        throw std::runtime_error("no nodes marked as this_node");
    }

    std::pair<std::uint16_t, std::int16_t> map_key(const std::string& key)
    {
        if (!vbmap.has_value()) {
            throw std::runtime_error("cannot map key: partition map is not available");
        }
        uint32_t crc = utils::hash_crc32(key.data(), key.size());
        uint16_t vbucket = uint16_t(crc % vbmap->size());
        return std::make_pair(vbucket, vbmap->at(vbucket)[0]);
    }
};

configuration
make_blank_configuration(const std::string& hostname, std::uint16_t plain_port, std::uint16_t tls_port)
{
    configuration result;
    result.id = couchbase::uuid::random();
    result.rev = 0;
    result.nodes.resize(1);
    result.nodes[0].hostname = hostname;
    result.nodes[0].this_node = true;
    result.nodes[0].services_plain.key_value = plain_port;
    result.nodes[0].services_tls.key_value = tls_port;
    return result;
}
} // namespace couchbase

template<>
struct fmt::formatter<couchbase::configuration::node> : formatter<std::string> {
    template<typename FormatContext>
    auto format(const couchbase::configuration::node& node, FormatContext& ctx)
    {
        std::vector<std::string> plain;
        if (node.services_plain.key_value) {
            plain.push_back(fmt::format("kv={}", *node.services_plain.key_value));
        }
        if (node.services_plain.management) {
            plain.push_back(fmt::format("mgmt={}", *node.services_plain.management));
        }
        if (node.services_plain.analytics) {
            plain.push_back(fmt::format("cbas={}", *node.services_plain.analytics));
        }
        if (node.services_plain.search) {
            plain.push_back(fmt::format("fts={}", *node.services_plain.search));
        }
        if (node.services_plain.query) {
            plain.push_back(fmt::format("n1ql={}", *node.services_plain.query));
        }
        if (node.services_plain.views) {
            plain.push_back(fmt::format("capi={}", *node.services_plain.views));
        }
        std::vector<std::string> tls;
        if (node.services_tls.key_value) {
            tls.push_back(fmt::format("kv={}", *node.services_tls.key_value));
        }
        if (node.services_tls.management) {
            tls.push_back(fmt::format("mgmt={}", *node.services_tls.management));
        }
        if (node.services_tls.analytics) {
            tls.push_back(fmt::format("cbas={}", *node.services_tls.analytics));
        }
        if (node.services_tls.search) {
            tls.push_back(fmt::format("fts={}", *node.services_tls.search));
        }
        if (node.services_tls.query) {
            tls.push_back(fmt::format("n1ql={}", *node.services_tls.query));
        }
        if (node.services_tls.views) {
            tls.push_back(fmt::format("capi={}", *node.services_tls.views));
        }
        std::vector<std::string> alternate_addresses{};
        if (!node.alt.empty()) {
            alternate_addresses.reserve(node.alt.size());
            for (const auto& entry : node.alt) {
                std::string network = fmt::format(R"(name="{}", host="{}")", entry.second.name, entry.second.hostname);
                {
                    std::vector<std::string> ports;
                    if (entry.second.services_plain.key_value) {
                        ports.push_back(fmt::format("kv={}", *entry.second.services_plain.key_value));
                    }
                    if (entry.second.services_plain.management) {
                        ports.push_back(fmt::format("mgmt={}", *entry.second.services_plain.management));
                    }
                    if (entry.second.services_plain.analytics) {
                        ports.push_back(fmt::format("cbas={}", *entry.second.services_plain.analytics));
                    }
                    if (entry.second.services_plain.search) {
                        ports.push_back(fmt::format("fts={}", *entry.second.services_plain.search));
                    }
                    if (entry.second.services_plain.query) {
                        ports.push_back(fmt::format("n1ql={}", *entry.second.services_plain.query));
                    }
                    if (entry.second.services_plain.views) {
                        ports.push_back(fmt::format("capi={}", *entry.second.services_plain.views));
                    }
                    if (!ports.empty()) {
                        network += fmt::format(", plain=({})", fmt::join(ports, ","));
                    }
                }
                {
                    std::vector<std::string> ports;
                    if (entry.second.services_tls.key_value) {
                        ports.push_back(fmt::format("kv={}", *entry.second.services_tls.key_value));
                    }
                    if (entry.second.services_tls.management) {
                        ports.push_back(fmt::format("mgmt={}", *entry.second.services_tls.management));
                    }
                    if (entry.second.services_tls.analytics) {
                        ports.push_back(fmt::format("cbas={}", *entry.second.services_tls.analytics));
                    }
                    if (entry.second.services_tls.search) {
                        ports.push_back(fmt::format("fts={}", *entry.second.services_tls.search));
                    }
                    if (entry.second.services_tls.query) {
                        ports.push_back(fmt::format("n1ql={}", *entry.second.services_tls.query));
                    }
                    if (entry.second.services_tls.views) {
                        ports.push_back(fmt::format("capi={}", *entry.second.services_tls.views));
                    }
                    if (!ports.empty()) {
                        network += fmt::format(", tls=({})", fmt::join(ports, ","));
                    }
                }
                alternate_addresses.emplace_back(network);
            }
        }
        format_to(ctx.out(),
                  R"(#<node:{} hostname="{}", plain=({}), tls=({}), alt=[{}]>)",
                  node.index,
                  node.hostname,
                  fmt::join(plain, ", "),
                  fmt::join(tls, ", "),
                  fmt::join(alternate_addresses, ", "));
        return formatter<std::string>::format("", ctx);
    }
};

template<>
struct fmt::formatter<couchbase::configuration> : formatter<std::string> {
    template<typename FormatContext>
    auto format(const couchbase::configuration& config, FormatContext& ctx)
    {
        format_to(ctx.out(),
                  R"(#<config:{} rev={}{}{}{}{}, nodes({})=[{}]>)",
                  couchbase::uuid::to_string(config.id),
                  config.rev,
                  config.uuid ? fmt::format(", uuid={}", *config.uuid) : "",
                  config.bucket ? fmt::format(", bucket={}", *config.bucket) : "",
                  config.num_replicas ? fmt::format(", replicas={}", *config.num_replicas) : "",
                  config.vbmap.has_value() ? fmt::format(", partitions={}", config.vbmap->size()) : "",
                  config.nodes.size(),
                  fmt::join(config.nodes, ", "));
        return formatter<std::string>::format("", ctx);
    }
};

namespace tao::json
{
template<>
struct traits<couchbase::configuration> {
    template<template<typename...> class Traits>
    static couchbase::configuration as(const tao::json::basic_value<Traits>& v)
    {
        couchbase::configuration result;
        result.id = couchbase::uuid::random();
        result.rev = v.at("rev").template as<std::uint64_t>();
        size_t index = 0;
        for (const auto& j : v.at("nodesExt").get_array()) {
            couchbase::configuration::node n;
            n.index = index++;
            const auto& o = j.get_object();
            const auto& this_node = o.find("thisNode");
            if (this_node != o.end() && this_node->second.get_boolean()) {
                n.this_node = true;
            }
            const auto& hostname = o.find("hostname");
            if (hostname != o.end()) {
                n.hostname = hostname->second.get_string();
            }
            const auto& s = o.at("services");
            n.services_plain.key_value = s.template optional<std::uint16_t>("kv");
            n.services_plain.management = s.template optional<std::uint16_t>("mgmt");
            n.services_plain.search = s.template optional<std::uint16_t>("fts");
            n.services_plain.analytics = s.template optional<std::uint16_t>("cbas");
            n.services_plain.query = s.template optional<std::uint16_t>("n1ql");
            n.services_plain.views = s.template optional<std::uint16_t>("capi");
            n.services_tls.key_value = s.template optional<std::uint16_t>("kvSSL");
            n.services_tls.management = s.template optional<std::uint16_t>("mgmtSSL");
            n.services_tls.search = s.template optional<std::uint16_t>("ftsSSL");
            n.services_tls.analytics = s.template optional<std::uint16_t>("cbasSSL");
            n.services_tls.query = s.template optional<std::uint16_t>("n1qlSSL");
            n.services_tls.views = s.template optional<std::uint16_t>("capiSSL");
            {
                const auto& alt = o.find("alternateAddresses");
                if (alt != o.end()) {
                    for (const auto& entry : alt->second.get_object()) {
                        couchbase::configuration::alternate_address addr;
                        addr.name = entry.first;
                        addr.hostname = entry.second.at("hostname").get_string();
                        const auto& ports = entry.second.find("ports");
                        addr.services_plain.key_value = ports->template optional<std::uint16_t>("kv");
                        addr.services_plain.management = ports->template optional<std::uint16_t>("mgmt");
                        addr.services_plain.search = ports->template optional<std::uint16_t>("fts");
                        addr.services_plain.analytics = ports->template optional<std::uint16_t>("cbas");
                        addr.services_plain.query = ports->template optional<std::uint16_t>("n1ql");
                        addr.services_plain.views = ports->template optional<std::uint16_t>("capi");
                        addr.services_tls.key_value = ports->template optional<std::uint16_t>("kvSSL");
                        addr.services_tls.management = ports->template optional<std::uint16_t>("mgmtSSL");
                        addr.services_tls.search = ports->template optional<std::uint16_t>("ftsSSL");
                        addr.services_tls.analytics = ports->template optional<std::uint16_t>("cbasSSL");
                        addr.services_tls.query = ports->template optional<std::uint16_t>("n1qlSSL");
                        addr.services_tls.views = ports->template optional<std::uint16_t>("capiSSL");
                        n.alt.emplace(entry.first, addr);
                    }
                }
            }
            result.nodes.emplace_back(n);
        }
        {
            const auto m = v.find("uuid");
            if (m != nullptr) {
                result.uuid = m->get_string();
            }
        }
        {
            const auto m = v.find("name");
            if (m != nullptr) {
                result.bucket = m->get_string();
            }
        }
        {
            const auto m = v.find("vBucketServerMap");
            if (m != nullptr) {
                const auto& o = m->get_object();
                {
                    const auto f = o.find("numReplicas");
                    if (f != o.end()) {
                        result.num_replicas = f->second.template as<std::uint32_t>();
                    }
                }
                {
                    const auto f = o.find("vBucketMap");
                    if (f != o.end()) {
                        const auto& vb = f->second.get_array();
                        couchbase::configuration::vbucket_map vbmap;
                        vbmap.resize(vb.size());
                        for (size_t i = 0; i < vb.size(); i++) {
                            const auto& p = vb[i].get_array();
                            vbmap[i].resize(p.size());
                            for (size_t n = 0; n < p.size(); n++) {
                                vbmap[i][n] = p[n].template as<std::int16_t>();
                            }
                        }
                        result.vbmap = vbmap;
                    }
                }
            }
        }
        return result;
    }
};
} // namespace tao::json
