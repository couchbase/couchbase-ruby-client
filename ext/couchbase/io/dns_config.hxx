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

#include <unistd.h>

#include <string>
#include <fstream>

#include <asio/ip/address.hpp>

#include <timeout_defaults.hxx>

namespace couchbase::io::dns
{
class dns_config
{
  public:
    static inline constexpr auto default_resolv_conf_path = "/etc/resolv.conf";
    static inline constexpr auto default_host = "8.8.8.8";
    static inline constexpr std::uint16_t default_port = 53;

    [[nodiscard]] const asio::ip::address& address() const
    {
        return address_;
    }

    [[nodiscard]] std::uint16_t port() const
    {
        return port_;
    }

    [[nodiscard]] std::chrono::milliseconds timeout() const
    {
        return timeout_;
    }

    static dns_config& get()
    {
        static dns_config instance{};

        instance.initialize();

        return instance;
    }

  private:
    void initialize()
    {
        if (!initialized_) {
            load_resolv_conf(default_resolv_conf_path);
            std::error_code ec;
            address_ = asio::ip::address::from_string(host_, ec);
            if (ec) {
                host_ = default_host;
                address_ = asio::ip::address::from_string(host_, ec);
            }
            initialized_ = true;
        }
    }

    void load_resolv_conf(const char* conf_path)
    {
        if (access(conf_path, R_OK) == 0) {
            std::ifstream conf(conf_path);
            while (conf.good()) {
                std::string line;
                std::getline(conf, line);
                if (line.empty()) {
                    continue;
                }
                std::size_t offset = 0;
                while (line[offset] == ' ') {
                    ++offset;
                }
                if (line[offset] == '#') {
                    continue;
                }
                std::size_t space = line.find(' ', offset);
                if (space == std::string::npos || space == offset || line.size() < space + 2) {
                    continue;
                }
                std::string keyword = line.substr(offset, space);
                if (keyword != "nameserver") {
                    continue;
                }
                offset = space + 1;
                space = line.find(' ', offset);
                host_ = line.substr(offset, space);
                break;
            }
        }
    }

    std::atomic_bool initialized_{ false };
    std::string host_{ default_host };
    asio::ip::address address_{};
    std::uint16_t port_{ default_port };
    std::chrono::milliseconds timeout_{ timeout_defaults::dns_srv_timeout };
};
} // namespace couchbase::io::dns
