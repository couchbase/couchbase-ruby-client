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
#include <memory>

#include <spdlog/spdlog.h>

#include <tao/json.hpp>

#include <asio.hpp>
#include <platform/uuid.h>

#include <errors.hxx>
#include <version.hxx>

#include <io/http_parser.hxx>
#include <io/http_message.hxx>
#include <platform/base64.h>
#include <timeout_defaults.hxx>

namespace couchbase::io
{

class http_session : public std::enable_shared_from_this<http_session>
{
  public:
    http_session(uuid::uuid_t client_id,
                 asio::io_context& ctx,
                 const std::string& username,
                 const std::string& password,
                 const std::string& hostname,
                 const std::string& service)
      : client_id_(client_id)
      , id_(uuid::random())
      , ctx_(ctx)
      , resolver_(ctx_)
      , strand_(asio::make_strand(ctx_))
      , socket_(strand_)
      , deadline_timer_(ctx_)
      , username_(username)
      , password_(password)
      , hostname_(hostname)
      , service_(service)
      , user_agent_(fmt::format("ruby_sdk/{}.{}.{}; client/{}; session/{}",
                                BACKEND_VERSION_MAJOR,
                                BACKEND_VERSION_MINOR,
                                BACKEND_VERSION_PATCH,
                                uuid::to_string(client_id_),
                                uuid::to_string(id_)))
    {
        resolver_.async_resolve(
          hostname, service, std::bind(&http_session::on_resolve, this, std::placeholders::_1, std::placeholders::_2));
    }

    ~http_session()
    {
        stop();
    }

    [[nodiscard]] uuid::uuid_t id()
    {
        return id_;
    }

    void on_stop(std::function<void()> handler)
    {
        on_stop_handler_ = std::move(handler);
    }

    void stop()
    {
        stopped_ = true;
        if (socket_.is_open()) {
            socket_.close();
        }
        deadline_timer_.cancel();

        for (auto& handler : command_handlers_) {
            handler(std::make_error_code(error::common_errc::ambiguous_timeout), {});
        }
        command_handlers_.clear();

        if (on_stop_handler_) {
            on_stop_handler_();
            on_stop_handler_ = nullptr;
        }
    }

    bool is_stopped()
    {
        return stopped_;
    }

    void write(const std::vector<uint8_t>& buf)
    {
        if (stopped_) {
            return;
        }
        output_buffer_.push_back(buf);
    }

    void write(const std::string& buf)
    {
        if (stopped_) {
            return;
        }
        output_buffer_.emplace_back(std::vector<uint8_t>{ buf.begin(), buf.end() });
    }

    void flush()
    {
        if (!connected_) {
            return;
        }
        if (stopped_) {
            return;
        }
        do_write();
    }

    void write_and_subscribe(io::http_request& request, std::function<void(std::error_code, io::http_response&&)> handler)
    {
        if (stopped_) {
            return;
        }
        request.headers["user-agent"] = user_agent_;
        request.headers["authorization"] = fmt::format("Basic {}", base64::encode(fmt::format("{}:{}", username_, password_)));
        write(fmt::format("{} {} HTTP/1.1\r\nhost: {}:{}\r\n", request.method, request.path, hostname_, service_));
        if (!request.body.empty()) {
            request.headers["content-length"] = std::to_string(request.body.size());
        }
        for (auto& header : request.headers) {
            write(fmt::format("{}: {}\r\n", header.first, header.second));
        }
        write("\r\n");
        write(request.body);
        command_handlers_.push_back(std::move(handler));
        flush();
    }

  private:
    void on_resolve(std::error_code ec, const asio::ip::tcp::resolver::results_type& endpoints)
    {
        if (ec) {
            spdlog::error("error on resolve: {}", ec.message());
            return;
        }
        endpoints_ = endpoints;
        do_connect(endpoints_.begin());
        deadline_timer_.async_wait(std::bind(&http_session::check_deadline, this, std::placeholders::_1));
    }

    void do_connect(asio::ip::tcp::resolver::results_type::iterator it)
    {
        if (it != endpoints_.end()) {
            spdlog::trace("connecting to {}:{}", it->endpoint().address().to_string(), it->endpoint().port());
            deadline_timer_.expires_after(timeout_defaults::connect_timeout);
            socket_.async_connect(it->endpoint(), std::bind(&http_session::on_connect, this, std::placeholders::_1, it));
        } else {
            spdlog::error("no more endpoints left to connect");
            stop();
        }
    }

    void on_connect(const std::error_code& ec, asio::ip::tcp::resolver::results_type::iterator it)
    {
        if (stopped_) {
            return;
        }
        if (!socket_.is_open() || ec) {
            do_connect(++it);
        } else {
            connected_ = true;
            endpoint_ = it->endpoint();
            spdlog::trace("connected to {}:{}", it->endpoint().address().to_string(), it->endpoint().port());
            deadline_timer_.expires_at(asio::steady_timer::time_point::max());
            deadline_timer_.cancel();
            flush();
        }
    }

    void check_deadline(std::error_code ec)
    {
        if (ec == asio::error::operation_aborted) {
            return;
        }
        if (stopped_) {
            return;
        }
        if (deadline_timer_.expiry() <= asio::steady_timer::clock_type::now()) {
            socket_.close();
            deadline_timer_.expires_at(asio::steady_timer::time_point::max());
        }
        deadline_timer_.async_wait(std::bind(&http_session::check_deadline, this, std::placeholders::_1));
    }

    void do_read()
    {
        if (stopped_) {
            return;
        }
        socket_.async_read_some(
          asio::buffer(input_buffer_), [self = shared_from_this()](std::error_code ec, std::size_t bytes_transferred) {
              if (self->stopped_) {
                  return;
              }
              if (ec && ec != asio::error::operation_aborted) {
                  spdlog::error("IO error while reading from the socket: {}", ec.message());
                  return self->stop();
              }

              switch (self->parser_.feed(reinterpret_cast<const char*>(self->input_buffer_.data()), bytes_transferred)) {
                  case http_parser::status::ok:
                      if (self->parser_.complete) {
                          if (!self->command_handlers_.empty()) {
                              auto handler = self->command_handlers_.front();
                              self->command_handlers_.pop_front();
                              handler({}, std::move(self->parser_.response));
                          }
                          self->parser_.reset();
                          return;
                      }
                      return self->do_read();
                  case http_parser::status::failure:
                      return self->stop();
              }
          });
    }

    void do_write()
    {
        if (stopped_) {
            return;
        }
        if (!writing_buffer_.empty()) {
            return;
        }
        std::swap(writing_buffer_, output_buffer_);
        std::vector<asio::const_buffer> buffers;
        buffers.reserve(writing_buffer_.size());
        for (auto& buf : writing_buffer_) {
            buffers.emplace_back(asio::buffer(buf));
        }
        asio::async_write(socket_, buffers, [self = shared_from_this()](std::error_code ec, std::size_t /* bytes_transferred */) {
            if (self->stopped_) {
                return;
            }
            if (ec) {
                spdlog::error("IO error while writing to the socket: {}", ec.message());
                return self->stop();
            }
            self->writing_buffer_.clear();
            if (!self->output_buffer_.empty()) {
                self->do_write();
            }
            self->do_read();
        });
    }

    uuid::uuid_t client_id_;
    uuid::uuid_t id_;
    asio::io_context& ctx_;
    asio::ip::tcp::resolver resolver_;
    asio::strand<asio::io_context::executor_type> strand_;
    asio::ip::tcp::socket socket_;
    asio::steady_timer deadline_timer_;

    std::string username_;
    std::string password_;
    std::string hostname_;
    std::string service_;
    std::string user_agent_;

    bool stopped_{ false };
    bool connected_{ false };

    std::function<void()> on_stop_handler_{ nullptr };

    std::list<std::function<void(std::error_code, io::http_response&&)>> command_handlers_{};
    http_parser parser_{};
    std::array<std::uint8_t, 16384> input_buffer_{};
    std::vector<std::vector<std::uint8_t>> output_buffer_{};
    std::vector<std::vector<std::uint8_t>> writing_buffer_{};
    asio::ip::tcp::endpoint endpoint_{}; // connected endpoint
    asio::ip::tcp::resolver::results_type endpoints_;
};
} // namespace couchbase::io