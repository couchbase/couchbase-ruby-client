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
#include <fstream>

#include <asio/ssl.hpp>

#include <io/mcbp_session.hxx>
#include <io/http_session_manager.hxx>
#include <io/http_command.hxx>
#include <origin.hxx>
#include <bucket.hxx>
#include <operations.hxx>
#include <operations/document_query.hxx>

namespace couchbase
{
class cluster
{
  public:
    explicit cluster(asio::io_context& ctx)
      : id_(uuid::to_string(uuid::random()))
      , ctx_(ctx)
      , work_(asio::make_work_guard(ctx_))
      , tls_(asio::ssl::context::tls_client)
      , session_manager_(std::make_shared<io::http_session_manager>(id_, ctx_, tls_))
    {
    }

    template<typename Handler>
    void open(const couchbase::origin& origin, Handler&& handler)
    {
        origin_ = origin;
        if (origin_.options().enable_tls) {
            tls_.set_options(asio::ssl::context::default_workarounds | asio::ssl::context::no_sslv2 | asio::ssl::context::no_sslv3);
            if (!origin_.options().trust_certificate.empty()) {
                tls_.use_certificate_chain_file(origin_.options().trust_certificate);
            }
#ifdef TLS_KEY_LOG_FILE
            SSL_CTX_set_keylog_callback(tls_.native_handle(), [](const SSL* /* ssl */, const char* line) {
                std::ofstream keylog(TLS_KEY_LOG_FILE, std::ios::out | std::ios::app | std::ios::binary);
                keylog << std::string_view(line) << std::endl;
            });
            spdlog::critical("TLS_KEY_LOG_FILE was set to \"{}\" during build, all TLS keys will be logged for network analysis "
                             "(https://wiki.wireshark.org/TLS). DO NOT USE THIS BUILD IN PRODUCTION",
                             TLS_KEY_LOG_FILE);
#endif
            session_ = std::make_shared<io::mcbp_session>(id_, ctx_, tls_, origin_);
        } else {
            session_ = std::make_shared<io::mcbp_session>(id_, ctx_, origin_);
        }
        session_->bootstrap([this, handler = std::forward<Handler>(handler)](std::error_code ec, const configuration& config) mutable {
            if (!ec) {
                session_manager_->set_configuration(config, origin_.options());
            }
            handler(ec);
        });
    }

    template<typename Handler>
    void close(Handler&& handler)
    {
        asio::post(asio::bind_executor(ctx_, [this, handler = std::forward<Handler>(handler)]() {
            if (session_) {
                session_->stop();
            }
            for (auto& bucket : buckets_) {
                bucket.second->close();
            }
            handler();
            work_.reset();
        }));
    }

    template<typename Handler>
    void open_bucket(const std::string& bucket_name, Handler&& handler)
    {
        std::vector<protocol::hello_feature> known_features;
        if (session_ && session_->has_config()) {
            known_features = session_->supported_features();
        }
        auto b = std::make_shared<bucket>(id_, ctx_, tls_, bucket_name, origin_, known_features);
        b->bootstrap([this, handler = std::forward<Handler>(handler)](std::error_code ec, const configuration& config) mutable {
            if (!ec && !session_->supports_gcccp()) {
                session_manager_->set_configuration(config, origin_.options());
            }
            handler(ec);
        });
        buckets_.emplace(bucket_name, b);
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
        auto session = session_manager_->check_out(Request::type, origin_.get_username(), origin_.get_password());
        if (!session) {
            return handler(operations::make_response(std::make_error_code(error::common_errc::service_not_available), request, {}));
        }
        auto cmd = std::make_shared<operations::http_command<Request>>(ctx_, request);
        cmd->send_to(session, [this, session, handler = std::forward<Handler>(handler)](typename Request::response_type resp) mutable {
            handler(std::move(resp));
            session_manager_->check_in(Request::type, session);
        });
    }

  private:
    std::string id_;
    asio::io_context& ctx_;
    asio::executor_work_guard<asio::io_context::executor_type> work_;
    asio::ssl::context tls_;
    std::shared_ptr<io::http_session_manager> session_manager_;
    std::shared_ptr<io::mcbp_session> session_{};
    std::map<std::string, std::shared_ptr<bucket>> buckets_{};
    couchbase::origin origin_{};
};
} // namespace couchbase
