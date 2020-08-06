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
#include <io/dns_client.hxx>
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
      , dns_config_(io::dns::dns_config::get())
      , dns_client_(ctx_)
    {
    }

    template<typename Handler>
    void open(const couchbase::origin& origin, Handler&& handler)
    {
        origin_ = origin;
        if (origin_.options().enable_dns_srv) {
            return do_dns_srv(std::forward<Handler>(handler));
        }
        do_open(std::forward<Handler>(handler));
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
    template<typename Handler>
    void do_dns_srv(Handler&& handler)
    {
        std::string hostname;
        std::string service;
        std::tie(hostname, service) = origin_.next_address();
        service = origin_.options().enable_tls ? "_couchbases" : "_couchbase";
        dns_client_.query_srv(
          hostname,
          service,
          [hostname, this, handler = std::forward<Handler>(handler)](couchbase::io::dns::dns_client::dns_srv_response resp) mutable {
              if (resp.ec) {
                  spdlog::warn("failed to fetch DNS SRV records for \"{}\" ({}), assuming that cluster is listening this address",
                               hostname,
                               resp.ec.message());
              } else if (resp.targets.empty()) {
                  spdlog::warn("DNS SRV query returned 0 records for \"{}\", assuming that cluster is listening this address", hostname);
              } else {
                  origin::node_list nodes;
                  nodes.reserve(resp.targets.size());
                  for (const auto& address : resp.targets) {
                      origin::node_entry node;
                      node.first = address.hostname;
                      node.second = std::to_string(address.port);
                      nodes.emplace_back(node);
                  }
                  origin_.set_nodes(nodes);
                  spdlog::info("replace list of bootstrap nodes with addresses from DNS SRV of \"{}\": [{}]",
                               hostname,
                               fmt::join(origin_.get_nodes(), ", "));
              }
              return do_open(std::forward<Handler>(handler));
          });
    }

    template<typename Handler>
    void do_open(Handler&& handler)
    {
        if (origin_.options().enable_tls) {
            tls_.set_options(asio::ssl::context::default_workarounds | asio::ssl::context::no_sslv2 | asio::ssl::context::no_sslv3);
            if (!origin_.options().trust_certificate.empty()) {
                std::error_code ec{};
                tls_.use_certificate_chain_file(origin_.options().trust_certificate, ec);
                if (ec) {
                    spdlog::error("unable to load certificate chain \"{}\": {}", origin_.options().trust_certificate, ec.message());
                    return handler(ec);
                }
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
                if (origin_.options().network == "auto") {
                    origin_.options().network = config.select_network(session_->bootstrap_hostname());
                    spdlog::info(R"({} detected network is "{}")", session_->log_prefix(), origin_.options().network);
                }
                if (origin_.options().network != "default") {
                    origin::node_list nodes;
                    nodes.reserve(config.nodes.size());
                    for (const auto& address : config.nodes) {
                        origin::node_entry node;
                        node.first = address.hostname_for(origin_.options().network);
                        node.second =
                          std::to_string(address.port_or(origin_.options().network, service_type::kv, origin_.options().enable_tls, 0));
                        nodes.emplace_back(node);
                    }
                    origin_.set_nodes(nodes);
                    spdlog::info("replace list of bootstrap nodes with addresses of alternative network \"{}\": [{}]",
                                 origin_.options().network,
                                 fmt::join(origin_.get_nodes(), ","));
                }
                session_manager_->set_configuration(config, origin_.options());
            }
            handler(ec);
        });
    }

    std::string id_;
    asio::io_context& ctx_;
    asio::executor_work_guard<asio::io_context::executor_type> work_;
    asio::ssl::context tls_;
    std::shared_ptr<io::http_session_manager> session_manager_;
    io::dns::dns_config& dns_config_;
    couchbase::io::dns::dns_client dns_client_;
    std::shared_ptr<io::mcbp_session> session_{};
    std::map<std::string, std::shared_ptr<bucket>> buckets_{};
    couchbase::origin origin_{};
};
} // namespace couchbase
