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
#include <io/http_context.hxx>
#include <platform/base64.h>
#include <timeout_defaults.hxx>

namespace couchbase::io
{

class http_session : public std::enable_shared_from_this<http_session>
{
  public:
    http_session(service_type type,
                 const std::string& client_id,
                 asio::io_context& ctx,
                 const cluster_credentials& credentials,
                 const std::string& hostname,
                 const std::string& service,
                 http_context http_ctx)
      : type_(type)
      , client_id_(client_id)
      , id_(uuid::to_string(uuid::random()))
      , ctx_(ctx)
      , resolver_(ctx_)
      , stream_(std::make_unique<plain_stream_impl>(ctx_))
      , deadline_timer_(ctx_)
      , idle_timer_(ctx_)
      , credentials_(credentials)
      , hostname_(hostname)
      , service_(service)
      , user_agent_(fmt::format("ruby/{}.{}.{}/{}; client/{}; session/{}; {}",
                                BACKEND_VERSION_MAJOR,
                                BACKEND_VERSION_MINOR,
                                BACKEND_VERSION_PATCH,
                                BACKEND_GIT_REVISION,
                                client_id_,
                                id_,
                                BACKEND_SYSTEM))
      , log_prefix_(fmt::format("[{}/{}]", client_id_, id_))
      , http_ctx_(std::move(http_ctx))
    {
    }

    http_session(service_type type,
                 const std::string& client_id,
                 asio::io_context& ctx,
                 asio::ssl::context& tls,
                 const cluster_credentials& credentials,
                 const std::string& hostname,
                 const std::string& service,
                 http_context http_ctx)
      : type_(type)
      , client_id_(client_id)
      , id_(uuid::to_string(uuid::random()))
      , ctx_(ctx)
      , resolver_(ctx_)
      , stream_(std::make_unique<tls_stream_impl>(ctx_, tls))
      , deadline_timer_(ctx_)
      , idle_timer_(ctx_)
      , credentials_(credentials)
      , hostname_(hostname)
      , service_(service)
      , user_agent_(fmt::format("ruby/{}.{}.{}/{}; client/{}; session/{}; {}",
                                BACKEND_VERSION_MAJOR,
                                BACKEND_VERSION_MINOR,
                                BACKEND_VERSION_PATCH,
                                BACKEND_GIT_REVISION,
                                client_id_,
                                id_,
                                BACKEND_SYSTEM))
      , log_prefix_(fmt::format("[{}/{}]", client_id_, id_))
      , http_ctx_(std::move(http_ctx))
    {
    }

    ~http_session()
    {
        stop();
    }

    [[nodiscard]] couchbase::http_context& http_context()
    {
        return http_ctx_;
    }

    [[nodiscard]] diag::endpoint_diag_info diag_info() const
    {
        return { type_,
                 id_,
                 last_active_.time_since_epoch().count() == 0 ? std::nullopt
                                                              : std::make_optional(std::chrono::duration_cast<std::chrono::microseconds>(
                                                                  std::chrono::steady_clock::now() - last_active_)),
                 fmt::format("{}:{}", endpoint_address_, endpoint_.port()),
                 fmt::format("{}:{}", local_endpoint_address_, local_endpoint_.port()),
                 state_ };
    }

    void start()
    {
        state_ = diag::endpoint_state::connecting;
        resolver_.async_resolve(
          hostname_, service_, std::bind(&http_session::on_resolve, shared_from_this(), std::placeholders::_1, std::placeholders::_2));
    }

    [[nodiscard]] const std::string& log_prefix() const
    {
        return log_prefix_;
    }

    [[nodiscard]] const std::string& id() const
    {
        return id_;
    }

    [[nodiscard]] const asio::ip::tcp::endpoint& endpoint() const
    {
        return endpoint_;
    }

    void on_stop(std::function<void()> handler)
    {
        on_stop_handler_ = std::move(handler);
    }

    void stop()
    {
        if (stopped_) {
            return;
        }
        stopped_ = true;
        state_ = diag::endpoint_state::disconnecting;
        if (stream_->is_open()) {
            stream_->close();
        }
        deadline_timer_.cancel();
        idle_timer_.cancel();

        {
            std::scoped_lock lock(command_handlers_mutex_);
            for (auto& handler : command_handlers_) {
                handler(std::make_error_code(error::common_errc::ambiguous_timeout), {});
            }
            command_handlers_.clear();
        }

        if (on_stop_handler_) {
            on_stop_handler_();
            on_stop_handler_ = nullptr;
        }
        state_ = diag::endpoint_state::disconnected;
    }

    bool keep_alive()
    {
        return keep_alive_;
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
        if (request.headers["connection"] == "keep-alive") {
            keep_alive_ = true;
        }
        request.headers["user-agent"] = user_agent_;
        request.headers["authorization"] =
          fmt::format("Basic {}", base64::encode(fmt::format("{}:{}", credentials_.username, credentials_.password)));
        write(fmt::format("{} {} HTTP/1.1\r\nhost: {}:{}\r\n", request.method, request.path, hostname_, service_));
        if (!request.body.empty()) {
            request.headers["content-length"] = std::to_string(request.body.size());
        }
        for (auto& header : request.headers) {
            write(fmt::format("{}: {}\r\n", header.first, header.second));
        }
        write("\r\n");
        write(request.body);
        {
            std::scoped_lock lock(command_handlers_mutex_);
            command_handlers_.push_back(std::move(handler));
        }
        flush();
    }

    void set_idle(std::chrono::milliseconds timeout)
    {
        idle_timer_.expires_after(timeout);
        return idle_timer_.async_wait([self = shared_from_this()](std::error_code ec) {
            if (ec == asio::error::operation_aborted) {
                return;
            }
            self->stop();
        });
    }

    void reset_idle()
    {
        idle_timer_.cancel();
    }

  private:
    void on_resolve(std::error_code ec, const asio::ip::tcp::resolver::results_type& endpoints)
    {
        if (ec) {
            spdlog::error("{} error on resolve: {}", log_prefix_, ec.message());
            return;
        }
        last_active_ = std::chrono::steady_clock::now();
        endpoints_ = endpoints;
        do_connect(endpoints_.begin());
        deadline_timer_.async_wait(std::bind(&http_session::check_deadline, shared_from_this(), std::placeholders::_1));
    }

    void do_connect(asio::ip::tcp::resolver::results_type::iterator it)
    {
        if (it != endpoints_.end()) {
            spdlog::debug("{} connecting to {}:{}", log_prefix_, it->endpoint().address().to_string(), it->endpoint().port());
            deadline_timer_.expires_after(timeout_defaults::connect_timeout);
            stream_->async_connect(it->endpoint(), std::bind(&http_session::on_connect, shared_from_this(), std::placeholders::_1, it));
        } else {
            spdlog::error("{} no more endpoints left to connect", log_prefix_);
            stop();
        }
    }

    void on_connect(const std::error_code& ec, asio::ip::tcp::resolver::results_type::iterator it)
    {
        if (stopped_) {
            return;
        }
        last_active_ = std::chrono::steady_clock::now();
        if (!stream_->is_open() || ec) {
            spdlog::warn(
              "{} unable to connect to {}:{}: {}", log_prefix_, it->endpoint().address().to_string(), it->endpoint().port(), ec.message());
            do_connect(++it);
        } else {
            state_ = diag::endpoint_state::connected;
            connected_ = true;
            local_endpoint_ = stream_->local_endpoint();
            local_endpoint_address_ = local_endpoint_.address().to_string();
            endpoint_ = it->endpoint();
            endpoint_address_ = endpoint_.address().to_string();
            spdlog::debug("{} connected to {}:{}", log_prefix_, it->endpoint().address().to_string(), it->endpoint().port());
            log_prefix_ = fmt::format("[{}/{}] <{}:{}>", client_id_, id_, endpoint_.address().to_string(), endpoint_.port());
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
            stream_->close();
            deadline_timer_.expires_at(asio::steady_timer::time_point::max());
        }
        deadline_timer_.async_wait(std::bind(&http_session::check_deadline, shared_from_this(), std::placeholders::_1));
    }

    void do_read()
    {
        if (stopped_) {
            return;
        }
        stream_->async_read_some(
          asio::buffer(input_buffer_), [self = shared_from_this()](std::error_code ec, std::size_t bytes_transferred) {
              if (ec == asio::error::operation_aborted || self->stopped_) {
                  return;
              }
              self->last_active_ = std::chrono::steady_clock::now();
              if (ec) {
                  spdlog::error("{} IO error while reading from the socket: {}", self->log_prefix_, ec.message());
                  return self->stop();
              }

              switch (self->parser_.feed(reinterpret_cast<const char*>(self->input_buffer_.data()), bytes_transferred)) {
                  case http_parser::status::ok:
                      if (self->parser_.complete) {
                          if (!self->command_handlers_.empty()) {
                              decltype(self->command_handlers_)::value_type handler{};
                              {
                                  std::scoped_lock lock(self->command_handlers_mutex_);
                                  handler = self->command_handlers_.front();
                                  self->command_handlers_.pop_front();
                              }
                              if (self->parser_.response.must_close_connection()) {
                                  self->keep_alive_ = false;
                              }
                              handler({}, std::move(self->parser_.response));
                          }
                          self->parser_.reset();
                          return;
                      }
                      return self->do_read();
                  case http_parser::status::failure:
                      spdlog::error("{} failed to parse HTTP response", self->log_prefix_);
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
        stream_->async_write(buffers, [self = shared_from_this()](std::error_code ec, std::size_t /* bytes_transferred */) {
            if (ec == asio::error::operation_aborted || self->stopped_) {
                return;
            }
            self->last_active_ = std::chrono::steady_clock::now();
            if (ec) {
                spdlog::error("{} IO error while writing to the socket: {}", self->log_prefix_, ec.message());
                return self->stop();
            }
            self->writing_buffer_.clear();
            if (!self->output_buffer_.empty()) {
                self->do_write();
            }
            self->do_read();
        });
    }

    service_type type_;
    std::string client_id_;
    std::string id_;
    asio::io_context& ctx_;
    asio::ip::tcp::resolver resolver_;
    std::unique_ptr<stream_impl> stream_;
    asio::steady_timer deadline_timer_;
    asio::steady_timer idle_timer_;

    cluster_credentials credentials_;
    std::string hostname_;
    std::string service_;
    std::string user_agent_;

    bool stopped_{ false };
    bool connected_{ false };
    bool keep_alive_{ false };

    std::function<void()> on_stop_handler_{ nullptr };

    std::list<std::function<void(std::error_code, io::http_response&&)>> command_handlers_{};
    std::mutex command_handlers_mutex_{};

    http_parser parser_{};
    std::array<std::uint8_t, 16384> input_buffer_{};
    std::vector<std::vector<std::uint8_t>> output_buffer_{};
    std::vector<std::vector<std::uint8_t>> writing_buffer_{};
    asio::ip::tcp::endpoint endpoint_{}; // connected endpoint
    std::string endpoint_address_{};     // cached string with endpoint address
    asio::ip::tcp::endpoint local_endpoint_{};
    std::string local_endpoint_address_{};
    asio::ip::tcp::resolver::results_type endpoints_{};

    std::string log_prefix_{};
    couchbase::http_context http_ctx_;

    std::chrono::time_point<std::chrono::steady_clock> last_active_{};
    diag::endpoint_state state_{ diag::endpoint_state::disconnected };
};
} // namespace couchbase::io
