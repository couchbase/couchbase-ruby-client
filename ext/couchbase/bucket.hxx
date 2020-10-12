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

#include <queue>
#include <utility>

#include <operations.hxx>
#include <origin.hxx>

namespace couchbase
{
class bucket : public std::enable_shared_from_this<bucket>
{
  public:
    explicit bucket(const std::string& client_id,
                    asio::io_context& ctx,
                    asio::ssl::context& tls,
                    std::string name,
                    couchbase::origin origin,
                    const std::vector<protocol::hello_feature>& known_features)

      : client_id_(client_id)
      , ctx_(ctx)
      , tls_(tls)
      , name_(std::move(name))
      , origin_(std::move(origin))
      , known_features_(known_features)
    {
        log_prefix_ = fmt::format("[{}/{}]", client_id_, name_);
    }

    ~bucket()
    {
        close();
    }

    [[nodiscard]] const std::string& name() const
    {
        return name_;
    }

    /**
     * copies nodes from rhs that are not in lhs to output vector
     */
    static void diff_nodes(const std::vector<configuration::node>& lhs,
                           const std::vector<configuration::node>& rhs,
                           std::vector<configuration::node>& output)
    {
        for (const auto& re : rhs) {
            bool known = false;
            for (const auto& le : lhs) {
                if (le.hostname == re.hostname && le.services_plain.management.value_or(0) == re.services_plain.management.value_or(0)) {
                    known = true;
                    break;
                }
            }
            if (!known) {
                output.push_back(re);
            }
        }
    }

    void update_config(const configuration& config)
    {
        if (!config_) {
            spdlog::debug("{} initialize configuration rev={}", log_prefix_, config.rev_str());
        } else if (config.rev && config_->rev && *config.rev > *config_->rev) {
            spdlog::debug("{} will update the configuration old={} -> new={}", log_prefix_, config_->rev_str(), config.rev_str());
        } else {
            return;
        }

        std::vector<configuration::node> added{};
        std::vector<configuration::node> removed{};
        if (config_) {
            diff_nodes(config_->nodes, config.nodes, added);
            diff_nodes(config.nodes, config_->nodes, removed);
        } else {
            added = config.nodes;
        }
        config_ = config;
        if (!added.empty() || removed.empty()) {
            std::map<size_t, std::shared_ptr<io::mcbp_session>> new_sessions{};

            for (auto entry : sessions_) {
                std::size_t new_index = config.nodes.size() + 1;
                for (const auto& node : config.nodes) {
                    if (entry.second->bootstrap_hostname() == node.hostname_for(origin_.options().network) &&
                        entry.second->bootstrap_port() ==
                          std::to_string(node.port_or(origin_.options().network, service_type::kv, origin_.options().enable_tls, 0))) {
                        new_index = node.index;
                        break;
                    }
                }
                if (new_index < config.nodes.size()) {
                    spdlog::debug(R"({} rev={}, preserve session="{}", address="{}:{}")",
                                  log_prefix_,
                                  config.rev_str(),
                                  entry.second->id(),
                                  entry.second->bootstrap_hostname(),
                                  entry.second->bootstrap_port());
                    new_sessions.emplace(new_index, std::move(entry.second));
                } else {
                    spdlog::debug(R"({} rev={}, drop session="{}", address="{}:{}")",
                                  log_prefix_,
                                  config.rev_str(),
                                  entry.second->id(),
                                  entry.second->bootstrap_hostname(),
                                  entry.second->bootstrap_port());
                    entry.second.reset();
                }
            }

            for (const auto& node : config.nodes) {
                if (new_sessions.find(node.index) != new_sessions.end()) {
                    continue;
                }

                auto hostname = node.hostname_for(origin_.options().network);
                auto port = node.port_or(origin_.options().network, service_type::kv, origin_.options().enable_tls, 0);
                if (port == 0) {
                    continue;
                }
                couchbase::origin origin(origin_.credentials(), hostname, port, origin_.options());
                std::shared_ptr<io::mcbp_session> session;
                if (origin_.options().enable_tls) {
                    session = std::make_shared<io::mcbp_session>(client_id_, ctx_, tls_, origin, name_, known_features_);
                } else {
                    session = std::make_shared<io::mcbp_session>(client_id_, ctx_, origin, name_, known_features_);
                }
                spdlog::debug(
                  R"({} rev={}, add session="{}", address="{}:{}")", log_prefix_, config.rev_str(), session->id(), hostname, port);
                session->bootstrap(
                  [self = shared_from_this(), session](std::error_code err, const configuration& cfg) {
                      if (!err) {
                          self->update_config(cfg);
                          session->on_configuration_update([self](const configuration& new_config) { self->update_config(new_config); });
                          session->on_stop([index = session->index(), self](io::retry_reason reason) {
                              if (reason == io::retry_reason::socket_closed_while_in_flight) {
                                  self->restart_node(index);
                              }
                          });
                      }
                  },
                  true);
                new_sessions.emplace(node.index, std::move(session));
            }
            sessions_ = new_sessions;
        }
    }

    void restart_node(std::size_t index)
    {
        auto ptr = sessions_.find(index);
        if (ptr == sessions_.end()) {
            spdlog::debug(R"({} requested to restart session idx={}, which does not exist, ignoring)", log_prefix_, index);
            return;
        }
        auto& old_session = ptr->second;
        auto hostname = old_session->bootstrap_hostname();
        auto port = old_session->bootstrap_port();
        auto old_id = old_session->id();
        couchbase::origin origin(origin_.credentials(), hostname, port, origin_.options());
        sessions_.erase(ptr);
        std::shared_ptr<io::mcbp_session> session;
        if (origin_.options().enable_tls) {
            session = std::make_shared<io::mcbp_session>(client_id_, ctx_, tls_, origin, name_, known_features_);
        } else {
            session = std::make_shared<io::mcbp_session>(client_id_, ctx_, origin, name_, known_features_);
        }
        spdlog::debug(
          R"({} restarting session idx={}, id=("{}" -> "{}"), address="{}")", log_prefix_, index, old_id, session->id(), hostname, port);
        session->bootstrap(
          [self = shared_from_this(), session](std::error_code err, const configuration& config) {
              if (!err) {
                  self->update_config(config);
                  session->on_configuration_update([self](const configuration& new_config) { self->update_config(new_config); });
              }
          },
          true);
        sessions_.emplace(index, std::move(session));
    }

    template<typename Handler>
    void bootstrap(Handler&& handler)
    {
        std::shared_ptr<io::mcbp_session> new_session;
        if (origin_.options().enable_tls) {
            new_session = std::make_shared<io::mcbp_session>(client_id_, ctx_, tls_, origin_, name_, known_features_);
        } else {
            new_session = std::make_shared<io::mcbp_session>(client_id_, ctx_, origin_, name_, known_features_);
        }
        new_session->bootstrap([self = shared_from_this(), new_session, h = std::forward<Handler>(handler)](
                                 std::error_code ec, const configuration& cfg) mutable {
            if (!ec) {
                size_t this_index = new_session->index();
                new_session->on_configuration_update([self](const configuration& config) { self->update_config(config); });
                new_session->on_stop([this_index, self](io::retry_reason reason) {
                    if (reason == io::retry_reason::socket_closed_while_in_flight) {
                        self->restart_node(this_index);
                    }
                });

                self->sessions_.emplace(this_index, std::move(new_session));
                self->update_config(cfg);
                self->drain_deferred_queue();
            }
            h(ec, cfg);
        });
    }

    void drain_deferred_queue()
    {
        while (!deferred_commands_.empty()) {
            deferred_commands_.front()();
            deferred_commands_.pop();
        }
    }

    template<typename Request, typename Handler>
    void execute(Request request, Handler&& handler)
    {
        if (closed_) {
            return;
        }
        auto cmd = std::make_shared<operations::mcbp_command<bucket, Request>>(ctx_, shared_from_this(), request);
        cmd->start([cmd, handler = std::forward<Handler>(handler)](std::error_code ec, std::optional<io::mcbp_message> msg) mutable {
            using encoded_response_type = typename Request::encoded_response_type;
            handler(make_response(ec, cmd->request, msg ? encoded_response_type(*msg) : encoded_response_type{}));
        });
        if (config_) {
            map_and_send(cmd);
        } else {
            deferred_commands_.emplace([self = shared_from_this(), cmd]() { self->map_and_send(cmd); });
        }
    }

    void close()
    {
        if (closed_) {
            return;
        }
        closed_ = true;

        drain_deferred_queue();
        for (auto& session : sessions_) {
            if (session.second) {
                spdlog::debug(R"({} shutdown session session="{}", idx={})", log_prefix_, session.second->id(), session.first);
                session.second->stop(io::retry_reason::do_not_retry);
            }
        }
    }

    template<typename Request>
    void map_and_send(std::shared_ptr<operations::mcbp_command<bucket, Request>> cmd)
    {
        if (closed_) {
            return cmd->cancel(io::retry_reason::do_not_retry);
        }
        std::int16_t index = 0;
        if (cmd->request.id.use_any_session) {
            index = round_robin_next_;
            ++round_robin_next_;
            if (static_cast<std::size_t>(round_robin_next_) >= sessions_.size()) {
                round_robin_next_ = 0;
            }
        } else {
            std::tie(cmd->request.partition, index) = config_->map_key(cmd->request.id.key);
            if (index < 0) {
                return io::retry_orchestrator::maybe_retry(
                  cmd->manager_, cmd, io::retry_reason::node_not_available, std::make_error_code(error::common_errc::request_canceled));
            }
        }
        cmd->send_to(sessions_.at(static_cast<std::size_t>(index)));
    }

    template<typename Request>
    void schedule_for_retry(std::shared_ptr<operations::mcbp_command<bucket, Request>> cmd, std::chrono::milliseconds duration)
    {
        if (closed_) {
            return cmd->cancel(io::retry_reason::do_not_retry);
        }
        cmd->retry_backoff.expires_after(duration);
        cmd->retry_backoff.async_wait([self = shared_from_this(), cmd](std::error_code ec) mutable {
            if (ec == asio::error::operation_aborted) {
                return;
            }
            self->map_and_send(cmd);
        });
    }

    [[nodiscard]] const std::string& log_prefix()
    {
        return log_prefix_;
    }

    void export_diag_info(diag::diagnostics_result& res) const
    {
        for (const auto& session : sessions_) {
            res.services[service_type::kv].emplace_back(session.second->diag_info());
        }
    }

    template<typename Collector>
    void ping(std::shared_ptr<Collector> collector)
    {
        for (const auto& session : sessions_) {
            session.second->ping(collector->build_reporter());
        }
    }

  private:
    std::string client_id_;
    asio::io_context& ctx_;
    asio::ssl::context& tls_;
    std::string name_;
    origin origin_;

    std::optional<configuration> config_{};
    std::vector<protocol::hello_feature> known_features_;

    std::queue<std::function<void()>> deferred_commands_{};

    bool closed_{ false };
    std::map<size_t, std::shared_ptr<io::mcbp_session>> sessions_{};
    std::int16_t round_robin_next_{ 0 };

    std::string log_prefix_{};
};
} // namespace couchbase
