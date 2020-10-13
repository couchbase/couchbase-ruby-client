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
#include <io/http_context.hxx>
#include <operations/http_noop.hxx>
#include <io/http_command.hxx>

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

    void export_diag_info(diag::diagnostics_result& res)
    {
        std::scoped_lock lock(sessions_mutex_);

        for (const auto& list : busy_sessions_) {
            for (const auto& session : list.second) {
                if (session) {
                    res.services[list.first].emplace_back(session->diag_info());
                }
            }
        }
        for (const auto& list : idle_sessions_) {
            for (const auto& session : list.second) {
                if (session) {
                    res.services[list.first].emplace_back(session->diag_info());
                }
            }
        }
    }

    template<typename Collector>
    void ping(std::set<service_type> services, std::shared_ptr<Collector> collector, const couchbase::cluster_credentials& credentials)
    {
        std::array<service_type, 4> known_types{ service_type::query, service_type::analytics, service_type::search, service_type::views };
        for (auto& node : config_.nodes) {
            for (auto type : known_types) {
                if (services.find(type) == services.end()) {
                    continue;
                }
                std::uint16_t port = 0;
                port = node.port_or(options_.network, type, options_.enable_tls, 0);
                if (port != 0) {
                    std::scoped_lock lock(sessions_mutex_);
                    std::shared_ptr<http_session> session;
                    session = options_.enable_tls ? std::make_shared<http_session>(type,
                                                                                   client_id_,
                                                                                   ctx_,
                                                                                   tls_,
                                                                                   credentials,
                                                                                   node.hostname_for(options_.network),
                                                                                   std::to_string(port),
                                                                                   http_context{ config_, options_, query_cache_ })
                                                  : std::make_shared<http_session>(type,
                                                                                   client_id_,
                                                                                   ctx_,
                                                                                   credentials,
                                                                                   node.hostname_for(options_.network),
                                                                                   std::to_string(port),
                                                                                   http_context{ config_, options_, query_cache_ });
                    session->start();
                    session->on_stop([type, id = session->id(), self = this->shared_from_this()]() {
                        for (auto& s : self->busy_sessions_[type]) {
                            if (s && s->id() == id) {
                                s.reset();
                            }
                        }
                        for (auto& s : self->idle_sessions_[type]) {
                            if (s && s->id() == id) {
                                s.reset();
                            }
                        }
                    });
                    busy_sessions_[type].push_back(session);
                    operations::http_noop_request request{};
                    request.type = type;
                    auto cmd = std::make_shared<operations::http_command<operations::http_noop_request>>(ctx_, request);
                    cmd->send_to(session,
                                 [start = std::chrono::steady_clock::now(),
                                  self = shared_from_this(),
                                  type,
                                  session,
                                  handler = collector->build_reporter()](operations::http_noop_response&& resp) mutable {
                                     diag::ping_state state = diag::ping_state::ok;
                                     std::optional<std::string> error{};
                                     if (resp.ec) {
                                         state = diag::ping_state::error;
                                         error.emplace(fmt::format(
                                           "code={}, message={}, http_code={}", resp.ec.value(), resp.ec.message(), resp.status_code));
                                     }
                                     handler(diag::endpoint_ping_info{
                                       type,
                                       session->id(),
                                       std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start),
                                       session->remote_address(),
                                       session->local_address(),
                                       state,
                                       {},
                                       error });
                                     self->check_in(type, session);
                                 });
                }
            }
        }
    }

    std::shared_ptr<http_session> check_out(service_type type, const couchbase::cluster_credentials& credentials)
    {
        std::scoped_lock lock(sessions_mutex_);
        idle_sessions_[type].remove_if([](const auto& s) -> bool { return !s; });
        busy_sessions_[type].remove_if([](const auto& s) -> bool { return !s; });
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
                session = std::make_shared<http_session>(type,
                                                         client_id_,
                                                         ctx_,
                                                         tls_,
                                                         credentials,
                                                         hostname,
                                                         std::to_string(port),
                                                         http_context{ config_, options_, query_cache_ });
            } else {
                session = std::make_shared<http_session>(
                  type, client_id_, ctx_, credentials, hostname, std::to_string(port), http_context{ config_, options_, query_cache_ });
            }
            session->start();

            session->on_stop([type, id = session->id(), self = this->shared_from_this()]() {
                for (auto& s : self->busy_sessions_[type]) {
                    if (s && s->id() == id) {
                        s.reset();
                    }
                }
                for (auto& s : self->idle_sessions_[type]) {
                    if (s && s->id() == id) {
                        s.reset();
                    }
                }
            });
            busy_sessions_[type].push_back(session);
            return session;
        }
        auto session = idle_sessions_[type].front();
        idle_sessions_[type].pop_front();
        session->reset_idle();
        busy_sessions_[type].push_back(session);
        return session;
    }

    void check_in(service_type type, std::shared_ptr<http_session> session)
    {
        if (!session->keep_alive()) {
            return session->stop();
        }
        if (!session->is_stopped()) {
            session->set_idle(options_.idle_http_connection_timeout);
            std::scoped_lock lock(sessions_mutex_);
            spdlog::debug("{} put HTTP session back to idle connections", session->log_prefix());
            idle_sessions_[type].push_back(session);
            busy_sessions_[type].remove_if([id = session->id()](const auto& s) -> bool { return !s || s->id() == id; });
        }
    }

    void close()
    {
        {
            for (auto& sessions : idle_sessions_) {
                for (auto& s : sessions.second) {
                    if (s) {
                        s->reset_idle();
                        s.reset();
                    }
                }
            }
        }
        {
            for (auto& sessions : busy_sessions_) {
                for (auto& s : sessions.second) {
                    s.reset();
                }
            }
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
    query_cache query_cache_{};
};
} // namespace couchbase::io
