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

#include <utils/connection_string.hxx>

namespace couchbase
{
struct origin {
    using node_list = std::vector<std::pair<std::string, std::string>>;

    origin() = default;

    origin(origin&& other) = default;

    origin(const origin& other)
      : options_(other.options_)
      , username_(other.username_)
      , password_(other.password_)
      , nodes_(other.nodes_)
      , next_node_(nodes_.begin())
    {
    }

    origin(std::string username, std::string password, const std::string& hostname, std::uint16_t port, const cluster_options& options)
      : options_(options)
      , username_(std::move(username))
      , password_(std::move(password))
      , nodes_{ { hostname, std::to_string(port) } }
      , next_node_(nodes_.begin())
    {
    }

    origin(std::string username, std::string password, const utils::connection_string& connstr)
      : options_(connstr.options)
      , username_(std::move(username))
      , password_(std::move(password))
    {
        nodes_.reserve(connstr.bootstrap_nodes.size());
        for (const auto& node : connstr.bootstrap_nodes) {
            nodes_.emplace_back(
              std::make_pair(node.address, node.port > 0 ? std::to_string(node.port) : std::to_string(connstr.default_port)));
        }
        next_node_ = nodes_.begin();
    }

    origin& operator=(const origin& other)
    {
        if (this != &other) {
            options_ = other.options_;
            username_ = other.username_;
            password_ = other.password_;
            nodes_ = other.nodes_;
            next_node_ = nodes_.begin();
            exhausted_ = false;
        }
        return *this;
    }

    [[nodiscard]] const std::string& get_username() const
    {
        return username_;
    }

    [[nodiscard]] const std::string& get_password() const
    {
        return password_;
    }

    [[nodiscard]] const node_list& get_nodes() const
    {
        return nodes_;
    }

    [[nodiscard]] std::pair<std::string, std::string> next_address()
    {
        if (exhausted_) {
            restart();
        }

        auto address = *next_node_;
        if (++next_node_ == nodes_.end()) {
            exhausted_ = true;
        }
        return address;
    }

    [[nodiscard]] bool exhausted() const
    {
        return exhausted_;
    }

    void restart()
    {
        exhausted_ = false;
        next_node_ = nodes_.begin();
    }

    [[nodiscard]] const couchbase::cluster_options& options()
    {
        return options_;
    }

  private:
    couchbase::cluster_options options_{};
    std::string username_{};
    std::string password_{};
    node_list nodes_{};
    node_list::iterator next_node_{};
    bool exhausted_{ false };
};

} // namespace couchbase
