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
#include <thread>

#include <io/mcbp_session.hxx>
#include <io/http_session_manager.hxx>
#include <bucket.hxx>
#include <operations.hxx>
#include <operations/document_query.hxx>

namespace couchbase
{
struct origin {
    std::string username;
    std::string password;
    std::string hostname;

    [[nodiscard]] const std::string& get_username() const
    {
        return username;
    }

    [[nodiscard]] const std::string& get_password() const
    {
        return password;
    }

    [[nodiscard]] std::pair<std::string, std::string> get_address() const
    {
        return { hostname, "11210" };
    }
};

class cluster
{
  public:
    explicit cluster(asio::io_context& ctx)
      : id_(uuid::to_string(uuid::random()))
      , ctx_(ctx)
      , work_(asio::make_work_guard(ctx_))
      , session_(std::make_shared<io::mcbp_session>(id_, ctx_))
      , session_manager_(std::make_shared<io::http_session_manager>(id_, ctx_))
    {
    }

    ~cluster()
    {
        work_.reset();
    }

    template<typename Handler>
    void open(const couchbase::origin& origin, Handler&& handler)
    {
        auto address = origin.get_address();
        origin_ = origin;
        session_->bootstrap(address.first,
                            address.second,
                            origin.get_username(),
                            origin.get_password(),
                            [this, handler = std::forward<Handler>(handler)](std::error_code ec, configuration config) mutable {
                                session_manager_->set_configuration(config);
                                handler(ec);
                            });
    }

    template<typename Handler>
    void close(Handler&& handler)
    {
        asio::post(asio::bind_executor(ctx_, [this, handler = std::forward<Handler>(handler)]() {
            session_->stop();
            for (auto& bucket : buckets_) {
                bucket.second->close();
            }
            handler();
        }));
    }

    template<typename Handler>
    void open_bucket(const std::string& bucket_name, Handler&& handler)
    {
        if (!session_->has_config()) {
            return handler(std::make_error_code(error::common_errc::invalid_argument));
        }
        configuration config = session_->config();
        auto known_features = session_->supported_features();
        auto& node = config.nodes.front();
        auto new_session = std::make_shared<io::mcbp_session>(id_, ctx_, bucket_name, known_features);
        new_session->bootstrap(node.hostname,
                               std::to_string(*node.services_plain.key_value),
                               origin_.get_username(),
                               origin_.get_password(),
                               [this, name = bucket_name, new_session, known_features, h = std::forward<Handler>(handler)](
                                 std::error_code ec, configuration cfg) mutable {
                                   if (!ec) {
                                       if (!session_->supports_gcccp()) {
                                           session_manager_->set_configuration(cfg);
                                       }
                                       auto b = std::make_shared<bucket>(ctx_, name, cfg);
                                       size_t this_index = new_session->index();
                                       b->set_node(this_index, new_session);
                                       if (cfg.nodes.size() > 1) {
                                           for (const auto& n : cfg.nodes) {
                                               if (n.index != this_index) {
                                                   auto s = std::make_shared<io::mcbp_session>(id_, ctx_, name, known_features);
                                                   b->set_node(n.index, s);
                                                   s->bootstrap(n.hostname,
                                                                std::to_string(*n.services_plain.key_value),
                                                                origin_.get_username(),
                                                                origin_.get_password(),
                                                                [s, i = n.index, b](std::error_code err, configuration /*config*/) {
                                                                    if (err) {
                                                                        spdlog::warn("unable to bootstrap node: {}", err.message());
                                                                        b->remove_node(i);
                                                                    }
                                                                });
                                               }
                                           }
                                       }
                                       buckets_.emplace(name, std::move(b));
                                   }
                                   h(ec);
                               });
    }

    template<class Request, class Handler>
    void execute(Request request, Handler&& handler)
    {
        auto bucket = buckets_.find(request.id.bucket);
        if (bucket == buckets_.end()) {
            return handler(operations::make_response(std::make_error_code(error::common_errc::bucket_not_found), request, {}));
        }
        return bucket->second->execute(request, std::forward<Handler>(handler));
    }

    template<class Request, class Handler>
    void execute_http(Request request, Handler&& handler)
    {
        auto session = session_manager_->check_out(Request::type, origin_.username, origin_.password);
        if (!session) {
            return handler(operations::make_response(std::make_error_code(error::common_errc::service_not_available), request, {}));
        }
        auto cmd = std::make_shared<operations::command<Request>>(ctx_, std::move(request));
        cmd->send_to(session, [this, session, handler = std::forward<Handler>(handler)](typename Request::response_type resp) mutable {
            handler(std::move(resp));
            session_manager_->check_in(Request::type, session);
        });
    }

  private:
    std::string id_;
    asio::io_context& ctx_;
    asio::executor_work_guard<asio::io_context::executor_type> work_;
    std::shared_ptr<io::mcbp_session> session_;
    std::shared_ptr<io::http_session_manager> session_manager_;
    std::map<std::string, std::shared_ptr<bucket>> buckets_;
    origin origin_;
};
} // namespace couchbase
