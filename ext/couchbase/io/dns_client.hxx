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

#include <memory>
#include <sstream>

#include <io/dns_codec.hxx>
#include <io/dns_config.hxx>

#include <asio/read.hpp>

namespace couchbase::io::dns
{
class dns_client
{
  public:
    struct dns_srv_response {
        struct address {
            std::string hostname;
            std::uint16_t port;
        };
        std::error_code ec;
        std::vector<address> targets{};
    };

    class dns_srv_command : public std::enable_shared_from_this<dns_srv_command>
    {
      public:
        dns_srv_command(asio::io_context& ctx,
                        const std::string& name,
                        const std::string& service,
                        const asio::ip::address& address,
                        std::uint16_t port)
          : deadline_(ctx)
          , udp_(ctx)
          , tcp_(ctx)
          , address_(address)
          , port_(port)
        {
            static std::string protocol{ "_tcp" };
            dns_message request{};
            question_record qr;
            qr.klass = resource_class::in;
            qr.type = resource_type::srv;
            qr.name.labels.push_back(service);
            qr.name.labels.push_back(protocol);
            std::string label;
            std::istringstream name_stream(name);
            while (std::getline(name_stream, label, '.')) {
                qr.name.labels.push_back(label);
            }
            request.questions.emplace_back(qr);
            send_buf_ = dns_codec::encode(request);
        }

        template<class Handler>
        void execute(std::chrono::milliseconds timeout, Handler&& handler)
        {
            asio::ip::udp::endpoint endpoint(address_, port_);
            udp_.open(endpoint.protocol());
            udp_.async_send_to(
              asio::buffer(send_buf_),
              endpoint,
              [self = shared_from_this(), handler = std::forward<Handler>(handler)](std::error_code ec1,
                                                                                    std::size_t /* bytes_transferred */) mutable {
                  if (ec1 == asio::error::operation_aborted) {
                      self->deadline_.cancel();
                      return handler({ std::make_error_code(error::common_errc::ambiguous_timeout) });
                  }
                  if (ec1) {
                      self->deadline_.cancel();
                      return handler({ ec1 });
                  }

                  asio::ip::udp::endpoint sender_endpoint;
                  self->recv_buf_.resize(512);
                  self->udp_.async_receive_from(
                    asio::buffer(self->recv_buf_),
                    sender_endpoint,
                    [self, handler = std::forward<Handler>(handler)](std::error_code ec2, std::size_t bytes_transferred) mutable {
                        self->deadline_.cancel();
                        if (ec2) {
                            return handler({ ec2 });
                        }
                        self->recv_buf_.resize(bytes_transferred);
                        dns_message message = dns_codec::decode(self->recv_buf_);
                        if (message.header.flags.tc == truncation::yes) {
                            self->udp_.close();
                            return self->retry_with_tcp(std::forward<Handler>(handler));
                        }
                        dns_srv_response resp{ ec2 };
                        resp.targets.reserve(message.answers.size());
                        for (const auto& answer : message.answers) {
                            resp.targets.emplace_back(
                              dns_srv_response::address{ fmt::format("{}", fmt::join(answer.target.labels, ".")), answer.port });
                        }
                        return handler(resp);
                    });
              });
            deadline_.expires_after(timeout);
            deadline_.async_wait([self = shared_from_this()](std::error_code ec) {
                if (ec == asio::error::operation_aborted) {
                    return;
                }
                self->udp_.cancel();
                if (self->tcp_.is_open()) {
                    self->tcp_.cancel();
                }
            });
        }

      private:
        template<class Handler>
        void retry_with_tcp(Handler&& handler)
        {
            asio::ip::tcp::no_delay no_delay(true);
            std::error_code ignore_ec;
            tcp_.set_option(no_delay, ignore_ec);
            asio::ip::tcp::endpoint endpoint(address_, port_);
            tcp_.async_connect(
              endpoint, [self = shared_from_this(), handler = std::forward<Handler>(handler)](std::error_code ec1) mutable {
                  if (ec1) {
                      self->deadline_.cancel();
                      return handler({ ec1 });
                  }
                  auto send_size = static_cast<uint16_t>(self->send_buf_.size());
                  self->send_buf_.insert(self->send_buf_.begin(), std::uint8_t(send_size & 0xffU));
                  self->send_buf_.insert(self->send_buf_.begin(), std::uint8_t(send_size >> 8U));
                  asio::async_write(
                    self->tcp_,
                    asio::buffer(self->send_buf_),
                    [self, handler = std::forward<Handler>(handler)](std::error_code ec2, std::size_t /* bytes_transferred */) mutable {
                        if (ec2) {
                            self->deadline_.cancel();
                            if (ec2 == asio::error::operation_aborted) {
                                ec2 = std::make_error_code(error::common_errc::ambiguous_timeout);
                            }
                            return handler({ ec2 });
                        }
                        asio::async_read(self->tcp_,
                                         asio::buffer(&self->recv_buf_size_, sizeof(self->recv_buf_size_)),
                                         [self, handler = std::forward<Handler>(handler)](std::error_code ec3,
                                                                                          std::size_t /* bytes_transferred */) mutable {
                                             if (ec3) {
                                                 self->deadline_.cancel();
                                                 return handler({ ec3 });
                                             }
                                             self->recv_buf_size_ = ntohs(self->recv_buf_size_);
                                             self->recv_buf_.resize(self->recv_buf_size_);
                                             asio::async_read(
                                               self->tcp_,
                                               asio::buffer(self->recv_buf_),
                                               [self, handler = std::forward<Handler>(handler)](std::error_code ec4,
                                                                                                std::size_t bytes_transferred) mutable {
                                                   self->deadline_.cancel();
                                                   if (ec4) {
                                                       return handler({ ec4 });
                                                   }
                                                   self->recv_buf_.resize(bytes_transferred);
                                                   dns_message message = dns_codec::decode(self->recv_buf_);
                                                   dns_srv_response resp{ ec4 };
                                                   resp.targets.reserve(message.answers.size());
                                                   for (const auto& answer : message.answers) {
                                                       resp.targets.emplace_back(dns_srv_response::address{
                                                         fmt::format("{}", fmt::join(answer.target.labels, ".")), answer.port });
                                                   }
                                                   return handler(resp);
                                               });
                                         });
                    });
              });
        }

        asio::steady_timer deadline_;
        asio::ip::udp::socket udp_;
        asio::ip::tcp::socket tcp_;

        asio::ip::address address_;
        std::uint16_t port_;

        std::vector<uint8_t> send_buf_;
        std::uint16_t recv_buf_size_{ 0 };
        std::vector<uint8_t> recv_buf_;
    };

    explicit dns_client(asio::io_context& ctx)
      : ctx_(ctx)
    {
    }

    template<class Handler>
    void query_srv(const std::string& name, const std::string& service, Handler&& handler)
    {
        dns_config& config = dns_config::get();
        auto cmd = std::make_shared<dns_srv_command>(ctx_, name, service, config.address(), config.port());
        cmd->execute(config.timeout(), std::forward<Handler>(handler));
    }

    asio::io_context& ctx_;
};
} // namespace couchbase::io::dns
