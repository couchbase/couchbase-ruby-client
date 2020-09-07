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
struct cluster_credentials {
    std::string username;
    std::string password;
    std::string certificate_path;
    std::string key_path;

    [[nodiscard]] bool uses_certificate() const
    {
        return !certificate_path.empty();
    }
};

struct origin {
    using node_entry = std::pair<std::string, std::string>;
    using node_list = std::vector<node_entry>;

    origin() = default;

    origin(origin&& other) = default;

    origin(const origin& other)
      : options_(other.options_)
      , credentials_(std::move(other.credentials_))
      , nodes_(other.nodes_)
      , next_node_(nodes_.begin())
    {
    }

    origin(cluster_credentials auth, const std::string& hostname, std::uint16_t port, const cluster_options& options)
      : options_(options)
      , credentials_(std::move(auth))
      , nodes_{ { hostname, std::to_string(port) } }
      , next_node_(nodes_.begin())
    {
    }

    origin(cluster_credentials auth, const std::string& hostname, const std::string& port, const cluster_options& options)
      : options_(options)
      , credentials_(std::move(auth))
      , nodes_{ { hostname, port } }
      , next_node_(nodes_.begin())
    {
    }

    origin(cluster_credentials auth, const utils::connection_string& connstr)
      : options_(connstr.options)
      , credentials_(std::move(auth))
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
            credentials_ = other.credentials_;
            nodes_ = other.nodes_;
            next_node_ = nodes_.begin();
            exhausted_ = false;
        }
        return *this;
    }

    [[nodiscard]] const std::string& username() const
    {
        return credentials_.username;
    }

    [[nodiscard]] const std::string& password() const
    {
        return credentials_.password;
    }

    [[nodiscard]] const std::string& certificate_path() const
    {
        return credentials_.certificate_path;
    }

    [[nodiscard]] const std::string& key_path() const
    {
        return credentials_.key_path;
    }

    [[nodiscard]] std::vector<std::string> get_nodes() const
    {
        std::vector<std::string> res;
        res.reserve(nodes_.size());
        for (const auto& node : nodes_) {
            res.emplace_back(fmt::format("\"{}:{}\"", node.first, node.second));
        }
        return res;
    }

    void set_nodes(node_list nodes)
    {
        nodes_ = std::move(nodes);
        next_node_ = nodes_.begin();
        exhausted_ = false;
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

    [[nodiscard]] const couchbase::cluster_options& options() const
    {
        return options_;
    }

    [[nodiscard]] couchbase::cluster_options& options()
    {
        return options_;
    }

    [[nodiscard]] couchbase::cluster_credentials& credentials()
    {
        return credentials_;
    }

  private:
    couchbase::cluster_options options_{};
    cluster_credentials credentials_{};
    node_list nodes_{};
    node_list::iterator next_node_{};
    bool exhausted_{ false };
};

} // namespace couchbase
