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

#include <operations.hxx>
#include <origin.hxx>

namespace couchbase
{
class bucket : public std::enable_shared_from_this<bucket>
{
  public:
    explicit bucket(const std::string& client_id,
                    asio::io_context& ctx,
                    std::string name,
                    couchbase::origin origin,
                    const std::vector<protocol::hello_feature>& known_features)

      : client_id_(client_id)
      , ctx_(ctx)
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

    void set_node(size_t index, std::shared_ptr<io::mcbp_session> session)
    {
        if (closed_) {
            return;
        }

        sessions_.emplace(index, std::move(session));
    }

    void remove_node(size_t index)
    {
        if (closed_) {
            return;
        }
        sessions_.erase(index);
    }

    template<typename Handler>
    void bootstrap(Handler&& handler)
    {
        auto address = origin_.get_address();

        auto new_session = std::make_shared<io::mcbp_session>(client_id_, ctx_, name_, known_features_);
        new_session->bootstrap(
          address.first,
          address.second,
          origin_.get_username(),
          origin_.get_password(),
          [self = shared_from_this(), new_session, h = std::forward<Handler>(handler)](std::error_code ec, configuration cfg) mutable {
              if (!ec) {
                  // TODO: publish configuration for non-GCCCP HTTP services
                  self->config_ = cfg;
                  size_t this_index = new_session->index();
                  self->set_node(this_index, std::move(new_session));
                  if (cfg.nodes.size() > 1) {
                      for (const auto& n : cfg.nodes) {
                          if (n.index != this_index) {
                              auto s = std::make_shared<io::mcbp_session>(self->client_id_, self->ctx_, self->name_, self->known_features_);
                              s->bootstrap(n.hostname,
                                           std::to_string(*n.services_plain.key_value),
                                           self->origin_.get_username(),
                                           self->origin_.get_password(),
                                           [host = n.hostname, bucket = self->name_](std::error_code err, configuration /*config*/) {
                                               // TODO: retry, we know that auth is correct
                                               if (err) {
                                                   spdlog::warn("unable to bootstrap node {} ({}): {}", host, bucket, err.message());
                                               }
                                           });
                              self->set_node(n.index, std::move(s));
                          }
                      }
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
        size_t index = 0;
        std::tie(request.partition, index) = config_.map_key(request.id.key);
        auto session = sessions_.at(index);
        auto cmd = std::make_shared<operations::mcbp_command<Request>>(ctx_, request);
        cmd->start([cmd, handler = std::forward<Handler>(handler)](std::error_code ec, std::optional<io::mcbp_message> msg) mutable {
            using encoded_response_type = typename Request::encoded_response_type;
            handler(make_response(ec, cmd->request, msg ? encoded_response_type(*msg) : encoded_response_type{}));
        });
        cmd->send_to(session);
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

  private:
    std::string client_id_;
    asio::io_context& ctx_;
    std::string name_;
    origin origin_;

    configuration config_{};
    std::vector<protocol::hello_feature> known_features_;

    bool closed_{ false };
    std::map<size_t, std::shared_ptr<io::mcbp_session>> sessions_{};
};
} // namespace couchbase
