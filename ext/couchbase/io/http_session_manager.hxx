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

class http_session_manager : public std::enable_shared_from_this<http_session_manager>
{
  public:
    http_session_manager(const std::string& client_id, asio::io_context& ctx, asio::ssl::context& tls)
      : client_id_(client_id)
      , ctx_(ctx)
      , tls_(tls)
    {
    }

    void set_configuration(const configuration& config, const cluster_options& options)
    {
        options_ = options;
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
        std::scoped_lock lock(sessions_mutex_);
        if (idle_sessions_[type].empty()) {
            std::string hostname;
            std::uint16_t port = 0;
            std::tie(hostname, port) = next_node(type);
            if (port == 0) {
                return nullptr;
            }
            config_.nodes.size();
            std::shared_ptr<http_session> session;
            if (options_.enable_tls) {
                session = std::make_shared<http_session>(client_id_, ctx_, tls_, username, password, hostname, std::to_string(port));
            } else {
                session = std::make_shared<http_session>(client_id_, ctx_, username, password, hostname, std::to_string(port));
            }
            session->start();
            session->on_stop([type, id = session->id(), self = this->shared_from_this()]() {
                std::scoped_lock inner_lock(self->sessions_mutex_);
                self->busy_sessions_[type].remove_if([id](const auto& s) -> bool { return s->id() == id; });
                self->idle_sessions_[type].remove_if([id](const auto& s) -> bool { return s->id() == id; });
            });
            busy_sessions_[type].push_back(session);
            return session;
        }
        auto session = idle_sessions_[type].front();
        idle_sessions_[type].pop_front();
        busy_sessions_[type].push_back(session);
        return session;
    }

    void check_in(service_type type, std::shared_ptr<http_session> session)
    {
        if (!session->keep_alive()) {
            return session->stop();
        }
        if (!session->is_stopped()) {
            std::scoped_lock lock(sessions_mutex_);
            spdlog::debug("{} put HTTP session back to idle connections", session->log_prefix());
            idle_sessions_[type].push_back(session);
        }
    }

  private:
    std::pair<std::string, std::uint16_t> next_node(service_type type)
    {
        auto candidates = config_.nodes.size();
        while (candidates > 0) {
            --candidates;
            auto& node = config_.nodes[next_index_];
            next_index_ = (next_index_ + 1) % config_.nodes.size();
            std::uint16_t port = node.port_or(options_.network, type, options_.enable_tls, 0);
            if (port != 0) {
                return { node.hostname_for(options_.network), port };
            }
        }
        return { "", 0 };
    }

    std::string client_id_;
    asio::io_context& ctx_;
    asio::ssl::context& tls_;
    cluster_options options_;

    configuration config_{};
    std::map<service_type, std::list<std::shared_ptr<http_session>>> busy_sessions_{};
    std::map<service_type, std::list<std::shared_ptr<http_session>>> idle_sessions_{};
    std::size_t next_index_{ 0 };
    std::mutex sessions_mutex_{};
};
} // namespace couchbase::io
