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

#include <io/http_session.hxx>
#include <service_type.hxx>

#include <random>

namespace couchbase::io
{

class session_manager
{
  public:
    session_manager(uuid::uuid_t client_id, asio::io_context& ctx)
      : client_id_(client_id)
      , ctx_(ctx)
    {
    }

    void set_configuration(const configuration& config)
    {
        config_ = config;
        next_index_ = 0;
        if (config_.nodes.size() > 1) {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<std::size_t> dis(0, config_.nodes.size() - 1);
            next_index_ = dis(gen);
        }
    }

    std::shared_ptr<http_session> check_out(service_type type, const std::string& username, const std::string& password)
    {
        if (idle_sessions_[type].empty()) {
            std::string hostname;
            std::uint16_t port = 0;
            std::tie(hostname, port) = next_node(type);
            Expects(port != 0); // FIXME: service_not_available
            config_.nodes.size();
            auto session = std::make_shared<http_session>(client_id_, ctx_, username, password, hostname, std::to_string(port));
            busy_sessions_[type].push_back(session);
            return session;
        }
        auto session = idle_sessions_[type].front();
        idle_sessions_[type].pop_front();
        return session;
    }

    void check_in(service_type type, std::shared_ptr<http_session> session)
    {
        idle_sessions_[type].push_back(session);
    }

  private:
    std::pair<std::string, std::uint16_t> next_node(service_type type)
    {
        auto cur_index = next_index_;
        next_index_ = (next_index_ + 1) % config_.nodes.size();
        auto& node = config_.nodes[cur_index];
        switch (type) {
            case service_type::query:
                return { node.hostname, node.services_plain.query.value_or(0) };

            case service_type::analytics:
                return { node.hostname, node.services_plain.analytics.value_or(0) };

            case service_type::search:
                return { node.hostname, node.services_plain.search.value_or(0) };

            case service_type::views:
                return { node.hostname, node.services_plain.views.value_or(0) };

            case service_type::management:
                return { node.hostname, node.services_plain.management.value_or(0) };

            case service_type::kv:
                return { node.hostname, node.services_plain.key_value.value_or(0) };
        }
        return { "", 0 };
    }

    uuid::uuid_t client_id_;
    asio::io_context& ctx_;

    configuration config_{};
    std::map<service_type, std::list<std::shared_ptr<http_session>>> busy_sessions_{};
    std::map<service_type, std::list<std::shared_ptr<http_session>>> idle_sessions_{};
    std::size_t next_index_{ 0 };
};
} // namespace couchbase::io
