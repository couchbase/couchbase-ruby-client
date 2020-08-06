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

#include <utility>
#include <queue>

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
    }

    ~bucket()
    {
        close();
    }

    [[nodiscard]] const std::string& name() const
    {
        return name_;
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
                self->config_ = cfg;
                size_t this_index = new_session->index();
                self->sessions_.emplace(this_index, std::move(new_session));
                if (cfg.nodes.size() > 1) {
                    for (const auto& n : cfg.nodes) {
                        if (n.index != this_index) {
                            couchbase::origin origin(
                              self->origin_.get_username(),
                              self->origin_.get_password(),
                              n.hostname,
                              n.port_or(self->origin_.options().network, service_type::kv, self->origin_.options().enable_tls, 0),
                              self->origin_.options());
                            std::shared_ptr<io::mcbp_session> s;
                            if (self->origin_.options().enable_tls) {
                                s = std::make_shared<io::mcbp_session>(
                                  self->client_id_, self->ctx_, self->tls_, origin, self->name_, self->known_features_);
                            } else {
                                s = std::make_shared<io::mcbp_session>(
                                  self->client_id_, self->ctx_, origin, self->name_, self->known_features_);
                            }
                            s->bootstrap([host = n.hostname, bucket = self->name_](std::error_code err, const configuration& /*config*/) {
                                // TODO: retry, we know that auth is correct
                                if (err) {
                                    spdlog::warn("unable to bootstrap node {} ({}): {}", host, bucket, err.message());
                                }
                            });
                            self->sessions_.emplace(n.index, std::move(s));
                        }
                    }
                }
                while (!self->deferred_commands_.empty()) {
                    self->deferred_commands_.front()();
                    self->deferred_commands_.pop();
                }
            }
            h(ec, cfg);
        });
    }

    template<typename Request, typename Handler>
    void execute(Request request, Handler&& handler)
    {
        if (closed_) {
            return;
        }
        auto cmd = std::make_shared<operations::mcbp_command<Request>>(ctx_, request);
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
        for (auto& session : sessions_) {
            session.second->stop();
        }
    }

    template<typename Request>
    void map_and_send(std::shared_ptr<operations::mcbp_command<Request>> cmd)
    {
        size_t index = 0;
        std::tie(cmd->request.partition, index) = config_->map_key(cmd->request.id.key);
        auto session = sessions_.at(index);
        cmd->send_to(session);
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
};
} // namespace couchbase
