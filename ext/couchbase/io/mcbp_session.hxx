/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

#include <tao/json.hpp>

#include <asio.hpp>

#include <platform/uuid.h>

#include <io/mcbp_message.hxx>
#include <io/mcbp_parser.hxx>
#include <io/streams.hxx>
#include <io/retry_orchestrator.hxx>
#include <io/mcbp_context.hxx>

#include <timeout_defaults.hxx>

#include <protocol/hello_feature.hxx>
#include <protocol/client_request.hxx>
#include <protocol/client_response.hxx>
#include <protocol/server_request.hxx>
#include <protocol/cmd_noop.hxx>
#include <protocol/cmd_hello.hxx>
#include <protocol/cmd_sasl_list_mechs.hxx>
#include <protocol/cmd_sasl_auth.hxx>
#include <protocol/cmd_sasl_step.hxx>
#include <protocol/cmd_select_bucket.hxx>
#include <protocol/cmd_get_cluster_config.hxx>
#include <protocol/cmd_get_error_map.hxx>
#include <protocol/cmd_get.hxx>
#include <protocol/cmd_cluster_map_change_notification.hxx>

#include <cbsasl/client.h>

#include <spdlog/fmt/bin_to_hex.h>

#include <origin.hxx>
#include <errors.hxx>
#include <version.hxx>
#include <diagnostics.hxx>

namespace couchbase::io
{

class mcbp_session : public std::enable_shared_from_this<mcbp_session>
{
    class collection_cache
    {
      private:
        std::map<std::string, std::uint32_t> cid_map_{ { "_default._default", 0 } };

      public:
        [[nodiscard]] std::optional<std::uint32_t> get(const std::string& path)
        {
            Expects(!path.empty());
            if (auto ptr = cid_map_.find(path); ptr != cid_map_.end()) {
                return ptr->second;
            }
            return {};
        }

        void update(const std::string& path, std::uint32_t id)
        {
            Expects(!path.empty());
            cid_map_[path] = id;
        }

        void reset()
        {
            cid_map_.clear();
            cid_map_["_default._default"] = 0;
        }
    };

    class message_handler
    {
      public:
        virtual void handle(mcbp_message&& msg) = 0;

        virtual ~message_handler() = default;

        virtual void stop()
        {
        }
    };

    class bootstrap_handler : public message_handler
    {
      private:
        std::shared_ptr<mcbp_session> session_;
        sasl::ClientContext sasl_;
        std::atomic_bool stopped_{ false };

      public:
        ~bootstrap_handler() override = default;

        void stop() override
        {
            if (stopped_) {
                return;
            }
            stopped_ = true;
            session_.reset();
        }

        explicit bootstrap_handler(std::shared_ptr<mcbp_session> session)
          : session_(session)
          , sasl_([origin = session_->origin_]() { return origin.username(); },
                  [origin = session_->origin_]() { return origin.password(); },
                  session_->origin_.credentials().allowed_sasl_mechanisms)
        {
            tao::json::value user_agent{
                { "a", couchbase::sdk_id() },
                { "i", fmt::format("{}/{}", session_->client_id_, session_->id_) },
            };
            protocol::client_request<protocol::hello_request_body> hello_req;
            if (session_->origin_.options().enable_unordered_execution) {
                hello_req.body().enable_unordered_execution();
            }
            if (session_->origin_.options().enable_clustermap_notification) {
                hello_req.body().enable_clustermap_change_notification();
            }
            if (session_->origin_.options().enable_compression) {
                hello_req.body().enable_compression();
            }
            hello_req.opaque(session_->next_opaque());
            hello_req.body().user_agent(tao::json::to_string(user_agent));
            spdlog::debug("{} user_agent={}, requested_features=[{}]",
                          session_->log_prefix_,
                          hello_req.body().user_agent(),
                          fmt::join(hello_req.body().features(), ", "));
            session_->write(hello_req.data());

            if (!session->origin_.credentials().uses_certificate()) {
                protocol::client_request<protocol::sasl_list_mechs_request_body> list_req;
                list_req.opaque(session_->next_opaque());
                session_->write(list_req.data());

                protocol::client_request<protocol::sasl_auth_request_body> auth_req;
                auto [sasl_code, sasl_payload] = sasl_.start();
                auth_req.opaque(session_->next_opaque());
                auth_req.body().mechanism(sasl_.get_name());
                auth_req.body().sasl_data(sasl_payload);
                session_->write(auth_req.data());
            }

            session_->flush();
        }

        void complete(std::error_code ec)
        {
            stopped_ = true;
            session_->invoke_bootstrap_handler(ec);
        }

        void auth_success()
        {
            session_->authenticated_ = true;
            if (session_->supports_feature(protocol::hello_feature::xerror)) {
                protocol::client_request<protocol::get_error_map_request_body> errmap_req;
                errmap_req.opaque(session_->next_opaque());
                session_->write(errmap_req.data());
            }
            if (session_->bucket_name_) {
                protocol::client_request<protocol::select_bucket_request_body> sb_req;
                sb_req.opaque(session_->next_opaque());
                sb_req.body().bucket_name(session_->bucket_name_.value());
                session_->write(sb_req.data());
            }
            protocol::client_request<protocol::get_cluster_config_request_body> cfg_req;
            cfg_req.opaque(session_->next_opaque());
            session_->write(cfg_req.data());
            session_->flush();
        }

        void handle(mcbp_message&& msg) override
        {
            if (stopped_ || !session_) {
                return;
            }
            Expects(protocol::is_valid_client_opcode(msg.header.opcode));
            switch (auto opcode = protocol::client_opcode(msg.header.opcode)) {
                case protocol::client_opcode::hello: {
                    protocol::client_response<protocol::hello_response_body> resp(std::move(msg));
                    if (resp.status() == protocol::status::success) {
                        session_->supported_features_ = resp.body().supported_features();
                        spdlog::debug("{} supported_features=[{}]", session_->log_prefix_, fmt::join(session_->supported_features_, ", "));
                        if (session_->origin_.credentials().uses_certificate()) {
                            spdlog::debug("{} skip SASL authentication, because TLS certificate was specified", session_->log_prefix_);
                            return auth_success();
                        }
                    } else {
                        spdlog::warn("{} unexpected message status during bootstrap: {} (opaque={})",
                                     session_->log_prefix_,
                                     resp.error_message(),
                                     resp.opaque());
                        return complete(error::network_errc::handshake_failure);
                    }
                } break;
                case protocol::client_opcode::sasl_list_mechs: {
                    protocol::client_response<protocol::sasl_list_mechs_response_body> resp(std::move(msg));
                    if (resp.status() != protocol::status::success) {
                        spdlog::warn("{} unexpected message status during bootstrap: {} (opaque={})",
                                     session_->log_prefix_,
                                     resp.error_message(),
                                     resp.opaque());
                        return complete(error::common_errc::authentication_failure);
                    }
                } break;
                case protocol::client_opcode::sasl_auth: {
                    protocol::client_response<protocol::sasl_auth_response_body> resp(std::move(msg));
                    if (resp.status() == protocol::status::success) {
                        return auth_success();
                    }
                    if (resp.status() == protocol::status::auth_continue) {
                        auto [sasl_code, sasl_payload] = sasl_.step(resp.body().value());
                        if (sasl_code == sasl::error::OK) {
                            return auth_success();
                        }
                        if (sasl_code == sasl::error::CONTINUE) {
                            protocol::client_request<protocol::sasl_step_request_body> req;
                            req.opaque(session_->next_opaque());
                            req.body().mechanism(sasl_.get_name());
                            req.body().sasl_data(sasl_payload);
                            session_->write_and_flush(req.data());
                        } else {
                            spdlog::error(
                              "{} unable to authenticate: (sasl_code={}, opaque={})", session_->log_prefix_, sasl_code, resp.opaque());
                            return complete(error::common_errc::authentication_failure);
                        }
                    } else {
                        spdlog::warn("{} unexpected message status during bootstrap: {} (opaque={})",
                                     session_->log_prefix_,
                                     resp.error_message(),
                                     resp.opaque());
                        return complete(error::common_errc::authentication_failure);
                    }
                } break;
                case protocol::client_opcode::sasl_step: {
                    protocol::client_response<protocol::sasl_step_response_body> resp(std::move(msg));
                    if (resp.status() == protocol::status::success) {
                        return auth_success();
                    }
                    return complete(error::common_errc::authentication_failure);
                }
                case protocol::client_opcode::get_error_map: {
                    protocol::client_response<protocol::get_error_map_response_body> resp(std::move(msg));
                    if (resp.status() == protocol::status::success) {
                        session_->error_map_.emplace(resp.body().errmap());
                    } else {
                        spdlog::warn("{} unexpected message status during bootstrap: {} (opaque={}, {:n})",
                                     session_->log_prefix_,
                                     resp.error_message(),
                                     resp.opaque(),
                                     spdlog::to_hex(msg.header_data()));
                        return complete(error::network_errc::protocol_error);
                    }
                } break;
                case protocol::client_opcode::select_bucket: {
                    protocol::client_response<protocol::select_bucket_response_body> resp(std::move(msg));
                    if (resp.status() == protocol::status::success) {
                        spdlog::debug("{} selected bucket: {}", session_->log_prefix_, session_->bucket_name_.value_or(""));
                        session_->bucket_selected_ = true;
                    } else if (resp.status() == protocol::status::not_found) {
                        spdlog::debug("{} kv_engine node does not have configuration propagated yet (opcode={}, status={}, opaque={})",
                                      session_->log_prefix_,
                                      opcode,
                                      resp.status(),
                                      resp.opaque());
                        return complete(error::network_errc::configuration_not_available);
                    } else if (resp.status() == protocol::status::no_access) {
                        spdlog::debug("{} unable to select bucket: {}, probably the bucket does not exist",
                                      session_->log_prefix_,
                                      session_->bucket_name_.value_or(""));
                        session_->bucket_selected_ = false;
                        return complete(error::common_errc::bucket_not_found);
                    } else {
                        spdlog::warn("{} unexpected message status during bootstrap: {} (opaque={}, {:n})",
                                     session_->log_prefix_,
                                     resp.error_message(),
                                     resp.opaque(),
                                     spdlog::to_hex(msg.header_data()));
                        return complete(error::common_errc::bucket_not_found);
                    }
                } break;
                case protocol::client_opcode::get_cluster_config: {
                    protocol::client_response<protocol::get_cluster_config_response_body> resp(std::move(msg));
                    if (resp.status() == protocol::status::success) {
                        session_->update_configuration(resp.body().config());
                        complete({});
                    } else if (resp.status() == protocol::status::not_found) {
                        spdlog::debug("{} kv_engine node does not have configuration propagated yet (opcode={}, status={}, opaque={})",
                                      session_->log_prefix_,
                                      opcode,
                                      resp.status(),
                                      resp.opaque());
                        return complete(error::network_errc::configuration_not_available);
                    } else if (resp.status() == protocol::status::no_bucket && !session_->bucket_name_) {
                        // bucket-less session, but the server wants bucket
                        session_->supports_gcccp_ = false;
                        spdlog::warn("{} this server does not support GCCCP, open bucket before making any cluster-level command",
                                     session_->log_prefix_);
                        session_->update_configuration(
                          make_blank_configuration(session_->endpoint_address_, session_->endpoint_.port(), 0));
                        complete({});
                    } else {
                        spdlog::warn("{} unexpected message status during bootstrap: {} (opaque={}, {:n})",
                                     session_->log_prefix_,
                                     resp.error_message(),
                                     resp.opaque(),
                                     spdlog::to_hex(msg.header_data()));
                        return complete(error::network_errc::protocol_error);
                    }
                } break;
                default:
                    spdlog::warn("{} unexpected message during bootstrap: {}", session_->log_prefix_, opcode);
                    return complete(error::network_errc::protocol_error);
            }
        }
    };

    class normal_handler : public message_handler
    {
      private:
        std::shared_ptr<mcbp_session> session_;
        asio::steady_timer heartbeat_timer_;
        std::atomic_bool stopped_{ false };

      public:
        ~normal_handler() override = default;

        explicit normal_handler(std::shared_ptr<mcbp_session> session)
          : session_(session)
          , heartbeat_timer_(session_->ctx_)
        {
            if (session_->supports_gcccp_) {
                fetch_config({});
            }
        }

        void stop() override
        {
            if (stopped_) {
                return;
            }
            stopped_ = true;
            heartbeat_timer_.cancel();
            session_.reset();
        }

        void handle(mcbp_message&& msg) override
        {
            if (stopped_ || !session_) {
                return;
            }
            Expects(protocol::is_valid_magic(msg.header.magic));
            switch (auto magic = protocol::magic(msg.header.magic)) {
                case protocol::magic::client_response:
                case protocol::magic::alt_client_response:
                    Expects(protocol::is_valid_client_opcode(msg.header.opcode));
                    switch (auto opcode = protocol::client_opcode(msg.header.opcode)) {
                        case protocol::client_opcode::get_cluster_config: {
                            protocol::client_response<protocol::get_cluster_config_response_body> resp(std::move(msg));
                            if (resp.status() == protocol::status::success) {
                                if (session_) {
                                    session_->update_configuration(resp.body().config());
                                }
                            } else {
                                spdlog::warn("{} unexpected message status: {} (opaque={})",
                                             session_->log_prefix_,
                                             resp.error_message(),
                                             resp.opaque());
                            }
                        } break;
                        case protocol::client_opcode::noop:
                        case protocol::client_opcode::get_collections_manifest:
                        case protocol::client_opcode::get_collection_id:
                        case protocol::client_opcode::get:
                        case protocol::client_opcode::get_and_lock:
                        case protocol::client_opcode::get_and_touch:
                        case protocol::client_opcode::touch:
                        case protocol::client_opcode::insert:
                        case protocol::client_opcode::replace:
                        case protocol::client_opcode::upsert:
                        case protocol::client_opcode::append:
                        case protocol::client_opcode::prepend:
                        case protocol::client_opcode::remove:
                        case protocol::client_opcode::observe:
                        case protocol::client_opcode::unlock:
                        case protocol::client_opcode::increment:
                        case protocol::client_opcode::decrement:
                        case protocol::client_opcode::subdoc_multi_lookup:
                        case protocol::client_opcode::subdoc_multi_mutation: {
                            std::uint32_t opaque = msg.header.opaque;
                            std::uint16_t status = ntohs(msg.header.specific);
                            session_->command_handlers_mutex_.lock();
                            auto handler = session_->command_handlers_.find(opaque);
                            if (handler != session_->command_handlers_.end() && handler->second) {
                                auto ec = session_->map_status_code(opcode, status);
                                spdlog::trace("{} MCBP invoke operation handler: opcode={}, opaque={}, status={}, ec={}",
                                              session_->log_prefix_,
                                              opcode,
                                              opaque,
                                              protocol::status_to_string(status),
                                              ec.message());
                                auto fun = std::move(handler->second);
                                session_->command_handlers_.erase(handler);
                                session_->command_handlers_mutex_.unlock();
                                fun(ec, retry_reason::do_not_retry, std::move(msg));
                            } else {
                                session_->command_handlers_mutex_.unlock();
                                spdlog::debug("{} unexpected orphan response: opcode={}, opaque={}, status={}",
                                              session_->log_prefix_,
                                              opcode,
                                              msg.header.opaque,
                                              protocol::status_to_string(status));
                            }
                        } break;
                        default:
                            spdlog::warn("{} unexpected client response: opcode={}, opaque={}{:a}{:a})",
                                         session_->log_prefix_,
                                         opcode,
                                         msg.header.opaque,
                                         spdlog::to_hex(msg.header_data()),
                                         spdlog::to_hex(msg.body));
                    }
                    break;
                case protocol::magic::server_request:
                    Expects(protocol::is_valid_server_request_opcode(msg.header.opcode));
                    switch (auto opcode = static_cast<protocol::server_opcode>(msg.header.opcode)) {
                        case protocol::server_opcode::cluster_map_change_notification: {
                            protocol::server_request<protocol::cluster_map_change_notification_request_body> req(msg);
                            std::optional<configuration> config = req.body().config();
                            if (session_ && config.has_value()) {
                                if ((!config->bucket.has_value() && req.body().bucket().empty()) ||
                                    (session_->bucket_name_.has_value() && !req.body().bucket().empty() &&
                                     session_->bucket_name_.value() == req.body().bucket())) {
                                    session_->update_configuration(std::move(config.value()));
                                }
                            }
                        } break;
                        default:
                            spdlog::warn("{} unexpected server request: opcode={:x}, opaque={}{:a}{:a}",
                                         session_->log_prefix_,
                                         opcode,
                                         msg.header.opaque,
                                         spdlog::to_hex(msg.header_data()),
                                         spdlog::to_hex(msg.body));
                    }
                    break;
                case protocol::magic::client_request:
                case protocol::magic::alt_client_request:
                case protocol::magic::server_response:
                    spdlog::warn("{} unexpected magic: {} (opcode={:x}, opaque={}){:a}{:a}",
                                 session_->log_prefix_,
                                 magic,
                                 msg.header.opcode,
                                 msg.header.opaque,
                                 spdlog::to_hex(msg.header_data()),
                                 spdlog::to_hex(msg.body));
                    break;
            }
        }

        void fetch_config(std::error_code ec)
        {
            if (ec == asio::error::operation_aborted || stopped_ || !session_) {
                return;
            }
            protocol::client_request<protocol::get_cluster_config_request_body> req;
            req.opaque(session_->next_opaque());
            session_->write_and_flush(req.data());
            heartbeat_timer_.expires_after(std::chrono::milliseconds(2500));
            heartbeat_timer_.async_wait(std::bind(&normal_handler::fetch_config, this, std::placeholders::_1));
        }
    };

  public:
    mcbp_session() = delete;
    mcbp_session(const std::string& client_id,
                 asio::io_context& ctx,
                 const couchbase::origin& origin,
                 std::optional<std::string> bucket_name = {},
                 std::vector<protocol::hello_feature> known_features = {})
      : client_id_(client_id)
      , id_(uuid::to_string(uuid::random()))
      , ctx_(ctx)
      , resolver_(ctx_)
      , stream_(std::make_unique<plain_stream_impl>(ctx_))
      , bootstrap_deadline_(ctx_)
      , connection_deadline_(ctx_)
      , retry_backoff_(ctx_)
      , origin_(origin)
      , bucket_name_(std::move(bucket_name))
      , supported_features_(known_features)
    {
        log_prefix_ = fmt::format("[{}/{}/{}/{}]", client_id_, id_, stream_->log_prefix(), bucket_name_.value_or("-"));
    }

    mcbp_session(const std::string& client_id,
                 asio::io_context& ctx,
                 asio::ssl::context& tls,
                 const couchbase::origin& origin,
                 std::optional<std::string> bucket_name = {},
                 std::vector<protocol::hello_feature> known_features = {})
      : client_id_(client_id)
      , id_(uuid::to_string(uuid::random()))
      , ctx_(ctx)
      , resolver_(ctx_)
      , stream_(std::make_unique<tls_stream_impl>(ctx_, tls))
      , bootstrap_deadline_(ctx_)
      , connection_deadline_(ctx_)
      , retry_backoff_(ctx_)
      , origin_(origin)
      , bucket_name_(std::move(bucket_name))
      , supported_features_(known_features)
    {
        log_prefix_ = fmt::format("[{}/{}/{}/{}]", client_id_, id_, stream_->log_prefix(), bucket_name_.value_or("-"));
    }

    ~mcbp_session()
    {
        stop(retry_reason::do_not_retry);
    }

    [[nodiscard]] const std::string& log_prefix() const
    {
        return log_prefix_;
    }

    std::string remote_address() const
    {
        if (endpoint_.protocol() == asio::ip::tcp::v6()) {
            return fmt::format("[{}]:{}", endpoint_address_, endpoint_.port());
        }
        return fmt::format("{}:{}", endpoint_address_, endpoint_.port());
    }

    std::string local_address() const
    {
        if (endpoint_.protocol() == asio::ip::tcp::v6()) {
            return fmt::format("[{}]:{}", local_endpoint_address_, local_endpoint_.port());
        }
        return fmt::format("{}:{}", local_endpoint_address_, local_endpoint_.port());
    }

    [[nodiscard]] diag::endpoint_diag_info diag_info() const
    {
        return { service_type::kv,
                 id_,
                 last_active_.time_since_epoch().count() == 0 ? std::nullopt
                                                              : std::make_optional(std::chrono::duration_cast<std::chrono::microseconds>(
                                                                  std::chrono::steady_clock::now() - last_active_)),
                 remote_address(),
                 local_address(),
                 state_,
                 bucket_name_ };
    }

    template<typename Handler>
    void ping(Handler&& handler)
    {
        protocol::client_request<protocol::mcbp_noop_request_body> req;
        req.opaque(next_opaque());
        write_and_subscribe(req.opaque(),
                            req.data(false),
                            [start = std::chrono::steady_clock::now(), self = shared_from_this(), handler](
                              std::error_code ec, retry_reason reason, io::mcbp_message&& /* msg */) {
                                diag::ping_state state = diag::ping_state::ok;
                                std::optional<std::string> error{};
                                if (ec) {
                                    state = diag::ping_state::error;
                                    error.emplace(fmt::format("code={}, message={}, reason={}", ec.value(), ec.message(), reason));
                                }
                                handler(diag::endpoint_ping_info{
                                  service_type::kv,
                                  self->id_,
                                  std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start),
                                  self->remote_address(),
                                  self->local_address(),
                                  state,
                                  self->bucket_name_,
                                  error });
                            });
    }

    [[nodiscard]] mcbp_context context() const
    {
        return { config_, supported_features_ };
    }

    void bootstrap(std::function<void(std::error_code, configuration)>&& handler, bool retry_on_bucket_not_found = false)
    {
        retry_bootstrap_on_bucket_not_found_ = retry_on_bucket_not_found;
        bootstrap_handler_ = std::move(handler);
        bootstrap_deadline_.expires_after(timeout_defaults::bootstrap_timeout);
        bootstrap_deadline_.async_wait([self = shared_from_this()](std::error_code ec) {
            if (ec == asio::error::operation_aborted || self->stopped_) {
                return;
            }
            spdlog::warn("{} unable to bootstrap in time", self->log_prefix_);
            self->bootstrap_handler_(error::common_errc::unambiguous_timeout, {});
            self->bootstrap_handler_ = nullptr;
            self->stop(retry_reason::socket_closed_while_in_flight);
        });
        initiate_bootstrap();
    }

    void initiate_bootstrap()
    {
        if (stopped_) {
            return;
        }
        state_ = diag::endpoint_state::connecting;
        if (stream_->is_open()) {
            std::string old_id = stream_->id();
            stream_->reopen();
            spdlog::trace(R"({} reopen socket connection "{}" -> "{}", host="{}", port={})",
                          log_prefix_,
                          old_id,
                          stream_->id(),
                          bootstrap_hostname_,
                          bootstrap_port_);
        }
        if (origin_.exhausted()) {
            auto backoff = std::chrono::milliseconds(500);
            spdlog::debug("{} reached the end of list of bootstrap nodes, waiting for {}ms before restart", log_prefix_, backoff.count());
            retry_backoff_.expires_after(backoff);
            retry_backoff_.async_wait([self = shared_from_this()](std::error_code ec) mutable {
                if (ec == asio::error::operation_aborted || self->stopped_) {
                    return;
                }
                self->origin_.restart();
                self->initiate_bootstrap();
            });
            return;
        }
        std::tie(bootstrap_hostname_, bootstrap_port_) = origin_.next_address();
        log_prefix_ = fmt::format("[{}/{}/{}/{}] <{}:{}>",
                                  client_id_,
                                  id_,
                                  stream_->log_prefix(),
                                  bucket_name_.value_or("-"),
                                  bootstrap_hostname_,
                                  bootstrap_port_);
        spdlog::debug("{} attempt to establish MCBP connection", log_prefix_);
        resolver_.async_resolve(bootstrap_hostname_,
                                bootstrap_port_,
                                std::bind(&mcbp_session::on_resolve, shared_from_this(), std::placeholders::_1, std::placeholders::_2));
    }

    [[nodiscard]] const std::string& id() const
    {
        return id_;
    }

    [[nodiscard]] bool is_stopped() const
    {
        return stopped_;
    }

    void on_stop(std::function<void(io::retry_reason)> handler)
    {
        on_stop_handler_ = std::move(handler);
    }

    void stop(retry_reason reason)
    {
        if (stopped_) {
            return;
        }
        state_ = diag::endpoint_state::disconnecting;
        spdlog::debug("{} stop MCBP connection, reason={}", log_prefix_, reason);
        stopped_ = true;
        bootstrap_deadline_.cancel();
        connection_deadline_.cancel();
        retry_backoff_.cancel();
        resolver_.cancel();
        if (stream_->is_open()) {
            stream_->close();
        }
        std::error_code ec = error::common_errc::request_canceled;
        if (!bootstrapped_ && bootstrap_handler_) {
            bootstrap_handler_(ec, {});
            bootstrap_handler_ = nullptr;
        }
        if (handler_) {
            handler_->stop();
        }
        {
            std::scoped_lock lock(command_handlers_mutex_);
            for (auto& [opaque, handler] : command_handlers_) {
                if (handler) {
                    spdlog::debug("{} MCBP cancel operation during session close, opaque={}, ec={}", log_prefix_, opaque, ec.message());
                    auto fun = std::move(handler);
                    fun(ec, reason, {});
                }
            }
            command_handlers_.clear();
        }
        config_listeners_.clear();
        if (on_stop_handler_) {
            on_stop_handler_(reason);
        }
        on_stop_handler_ = nullptr;
        state_ = diag::endpoint_state::disconnected;
    }

    void write(const std::vector<uint8_t>& buf)
    {
        if (stopped_) {
            return;
        }
        std::uint32_t opaque{ 0 };
        std::memcpy(&opaque, buf.data() + 12, sizeof(opaque));
        spdlog::trace("{} MCBP send, opaque={}, {:n}", log_prefix_, opaque, spdlog::to_hex(buf.begin(), buf.begin() + 24));
        SPDLOG_TRACE("{} MCBP send, opaque={}{:a}", log_prefix_, opaque, spdlog::to_hex(data));
        std::scoped_lock lock(output_buffer_mutex_);
        output_buffer_.push_back(buf);
    }

    void flush()
    {
        if (stopped_) {
            return;
        }
        do_write();
    }

    void write_and_flush(const std::vector<uint8_t>& buf)
    {
        if (stopped_) {
            return;
        }
        write(buf);
        flush();
    }

    void write_and_subscribe(uint32_t opaque,
                             std::vector<std::uint8_t>& data,
                             std::function<void(std::error_code, retry_reason, io::mcbp_message&&)> handler)
    {
        if (stopped_) {
            spdlog::warn("{} MCBP cancel operation, while trying to write to closed session, opaque={}", log_prefix_, opaque);
            handler(error::common_errc::request_canceled, retry_reason::socket_closed_while_in_flight, {});
            return;
        }
        {
            std::scoped_lock lock(command_handlers_mutex_);
            command_handlers_.try_emplace(opaque, std::move(handler));
        }
        if (bootstrapped_ && stream_->is_open()) {
            write_and_flush(data);
        } else {
            spdlog::debug("{} the stream is not ready yet, put the message into pending buffer, opaque={}", log_prefix_, opaque);
            std::scoped_lock lock(pending_buffer_mutex_);
            pending_buffer_.push_back(data);
        }
    }

    [[nodiscard]] bool cancel(uint32_t opaque, std::error_code ec, retry_reason reason)
    {
        if (stopped_) {
            return false;
        }
        command_handlers_mutex_.lock();
        if (auto handler = command_handlers_.find(opaque); handler != command_handlers_.end()) {
            spdlog::debug("{} MCBP cancel operation, opaque={}, ec={} ({})", log_prefix_, opaque, ec.value(), ec.message());
            if (handler->second) {
                auto fun = std::move(handler->second);
                command_handlers_.erase(handler);
                command_handlers_mutex_.unlock();
                fun(ec, reason, {});
                return true;
            }
        }
        command_handlers_mutex_.unlock();
        return false;
    }

    [[nodiscard]] bool supports_feature(protocol::hello_feature feature)
    {
        return std::find(supported_features_.begin(), supported_features_.end(), feature) != supported_features_.end();
    }

    [[nodiscard]] std::vector<protocol::hello_feature> supported_features() const
    {
        return supported_features_;
    }

    [[nodiscard]] bool supports_gcccp() const
    {
        return supports_gcccp_;
    }

    [[nodiscard]] bool has_config() const
    {
        return config_.has_value();
    }

    [[nodiscard]] configuration config() const
    {
        return config_.value();
    }

    [[nodiscard]] size_t index() const
    {
        Expects(config_.has_value());
        return config_->index_for_this_node();
    }

    [[nodiscard]] const std::string& bootstrap_hostname() const
    {
        return bootstrap_hostname_;
    }

    [[nodiscard]] const std::string& bootstrap_port() const
    {
        return bootstrap_port_;
    }

    [[nodiscard]] uint32_t next_opaque()
    {
        return ++opaque_;
    }

    [[nodiscard]] std::error_code map_status_code(protocol::client_opcode opcode, uint16_t status) const
    {
        switch (protocol::status(status)) {
            case protocol::status::success:
            case protocol::status::subdoc_multi_path_failure:
            case protocol::status::subdoc_success_deleted:
            case protocol::status::subdoc_multi_path_failure_deleted:
                return {};

            case protocol::status::not_found:
            case protocol::status::not_stored:
                return error::key_value_errc::document_not_found;

            case protocol::status::exists:
                if (opcode == protocol::client_opcode::insert) {
                    return error::key_value_errc::document_exists;
                }
                return error::common_errc::cas_mismatch;

            case protocol::status::too_big:
                return error::key_value_errc::value_too_large;

            case protocol::status::invalid:
            case protocol::status::xattr_invalid:
            case protocol::status::subdoc_invalid_combo:
                return error::common_errc::invalid_argument;

            case protocol::status::delta_bad_value:
                return error::key_value_errc::delta_invalid;

            case protocol::status::no_bucket:
                return error::common_errc::bucket_not_found;

            case protocol::status::locked:
                return error::key_value_errc::document_locked;

            case protocol::status::auth_stale:
            case protocol::status::auth_error:
            case protocol::status::no_access:
                return error::common_errc::authentication_failure;

            case protocol::status::not_supported:
            case protocol::status::unknown_command:
                return error::common_errc::unsupported_operation;

            case protocol::status::internal:
                return error::common_errc::internal_server_failure;

            case protocol::status::busy:
            case protocol::status::temporary_failure:
            case protocol::status::no_memory:
            case protocol::status::not_initialized:
                return error::common_errc::temporary_failure;

            case protocol::status::unknown_collection:
                return error::common_errc::collection_not_found;

            case protocol::status::unknown_scope:
                return error::common_errc::scope_not_found;

            case protocol::status::durability_invalid_level:
                return error::key_value_errc::durability_level_not_available;

            case protocol::status::durability_impossible:
                return error::key_value_errc::durability_impossible;

            case protocol::status::sync_write_in_progress:
                return error::key_value_errc::durable_write_in_progress;

            case protocol::status::sync_write_ambiguous:
                return error::key_value_errc::durability_ambiguous;

            case protocol::status::sync_write_re_commit_in_progress:
                return error::key_value_errc::durable_write_re_commit_in_progress;

            case protocol::status::subdoc_path_not_found:
                return error::key_value_errc::path_not_found;

            case protocol::status::subdoc_path_mismatch:
                return error::key_value_errc::path_mismatch;

            case protocol::status::subdoc_path_invalid:
                return error::key_value_errc::path_invalid;

            case protocol::status::subdoc_path_too_big:
                return error::key_value_errc::path_too_big;

            case protocol::status::subdoc_doc_too_deep:
                return error::key_value_errc::value_too_deep;

            case protocol::status::subdoc_value_cannot_insert:
                return error::key_value_errc::value_invalid;

            case protocol::status::subdoc_doc_not_json:
                return error::key_value_errc::document_not_json;

            case protocol::status::subdoc_num_range_error:
                return error::key_value_errc::number_too_big;

            case protocol::status::subdoc_delta_invalid:
                return error::key_value_errc::delta_invalid;

            case protocol::status::subdoc_path_exists:
                return error::key_value_errc::path_exists;

            case protocol::status::subdoc_value_too_deep:
                return error::key_value_errc::value_too_deep;

            case protocol::status::subdoc_xattr_invalid_flag_combo:
            case protocol::status::subdoc_xattr_invalid_key_combo:
                return error::key_value_errc::xattr_invalid_key_combo;

            case protocol::status::subdoc_xattr_unknown_macro:
                return error::key_value_errc::xattr_unknown_macro;

            case protocol::status::subdoc_xattr_unknown_vattr:
                return error::key_value_errc::xattr_unknown_virtual_attribute;

            case protocol::status::subdoc_xattr_cannot_modify_vattr:
                return error::key_value_errc::xattr_cannot_modify_virtual_attribute;

            case protocol::status::subdoc_invalid_xattr_order:
            case protocol::status::not_my_vbucket:
            case protocol::status::auth_continue:
            case protocol::status::range_error:
            case protocol::status::rollback:
            case protocol::status::unknown_frame_info:
            case protocol::status::no_collections_manifest:
            case protocol::status::cannot_apply_collections_manifest:
            case protocol::status::collections_manifest_is_ahead:
            case protocol::status::dcp_stream_id_invalid:
                break;
        }
        // FIXME: use error map here
        spdlog::warn("{} unknown status code: {} (opcode={})", log_prefix_, protocol::status_to_string(status), opcode);
        return error::network_errc::protocol_error;
    }

    std::optional<error_map::error_info> decode_error_code(std::uint16_t code)
    {
        if (error_map_) {
            auto info = error_map_->errors.find(code);
            if (info != error_map_->errors.end()) {
                return info->second;
            }
        }
        return {};
    }

    void on_configuration_update(std::function<void(const configuration&)> handler)
    {
        config_listeners_.emplace_back(std::move(handler));
    }

    void update_configuration(configuration&& config)
    {
        if (stopped_) {
            return;
        }
        if (config_) {
            if (config_->vbmap && config.vbmap && config_->vbmap->size() != config.vbmap->size()) {
                spdlog::debug("{} received a configuration with a different number of vbuckets, ignoring", log_prefix_);
                return;
            }
            if (config_->rev && config.rev) {
                if (*config_->rev == *config.rev) {
                    spdlog::trace("{} received a configuration with identical revision (rev={}), ignoring", log_prefix_, *config.rev);
                    return;
                }
                if (*config_->rev > *config.rev) {
                    spdlog::debug("{} received a configuration with older revision, ignoring", log_prefix_);
                    return;
                }
            }
        }
        bool this_node_found = false;
        for (auto& node : config.nodes) {
            if (node.hostname.empty()) {
                node.hostname = bootstrap_hostname_;
            }
            if (node.this_node) {
                this_node_found = true;
            }
        }
        if (!this_node_found) {
            for (auto& node : config.nodes) {
                if (node.hostname == bootstrap_hostname_) {
                    if ((node.services_plain.key_value && std::to_string(node.services_plain.key_value.value()) == bootstrap_port_) ||
                        (node.services_tls.key_value && std::to_string(node.services_tls.key_value.value()) == bootstrap_port_)) {
                        node.this_node = true;
                    }
                }
            }
        }
        config_.emplace(config);
        spdlog::debug("{} received new configuration: {}", log_prefix_, config_.value());
        for (const auto& listener : config_listeners_) {
            listener(*config_);
        }
    }

    void handle_not_my_vbucket(io::mcbp_message&& msg)
    {
        if (stopped_) {
            return;
        }
        Expects(msg.header.magic == static_cast<std::uint8_t>(protocol::magic::alt_client_response) ||
                msg.header.magic == static_cast<std::uint8_t>(protocol::magic::client_response));
        if (protocol::has_json_datatype(msg.header.datatype)) {
            auto magic = protocol::magic(msg.header.magic);
            uint8_t extras_size = msg.header.extlen;
            uint8_t framing_extras_size = 0;
            uint16_t key_size = htons(msg.header.keylen);
            if (magic == protocol::magic::alt_client_response) {
                framing_extras_size = static_cast<std::uint8_t>(msg.header.keylen >> 8U);
                key_size = msg.header.keylen & 0xffU;
            }

            std::vector<uint8_t>::difference_type offset = framing_extras_size + key_size + extras_size;
            if (ntohl(msg.header.bodylen) - offset > 0) {
                auto config = protocol::parse_config(msg.body.begin() + offset, msg.body.end());
                spdlog::debug("{} received not_my_vbucket status for {}, opaque={} with config rev={} in the payload",
                              log_prefix_,
                              protocol::client_opcode(msg.header.opcode),
                              msg.header.opaque,
                              config.rev_str());
                update_configuration(std::move(config));
            }
        }
    }

    std::optional<std::uint32_t> get_collection_uid(const std::string& collection_path)
    {
        return collection_cache_.get(collection_path);
    }

    void update_collection_uid(const std::string& path, std::uint32_t uid)
    {
        if (stopped_) {
            return;
        }
        collection_cache_.update(path, uid);
    }

  private:
    void invoke_bootstrap_handler(std::error_code ec)
    {
        if (ec == error::network_errc::configuration_not_available) {
            return initiate_bootstrap();
        }
        if (retry_bootstrap_on_bucket_not_found_ && ec == error::common_errc::bucket_not_found) {
            spdlog::debug(R"({} server returned {} ({}), it must be transient condition, retrying)", log_prefix_, ec.value(), ec.message());
            return initiate_bootstrap();
        }

        if (!bootstrapped_ && bootstrap_handler_) {
            bootstrap_deadline_.cancel();
            bootstrap_handler_(ec, config_.value_or(configuration{}));
            bootstrap_handler_ = nullptr;
        }
        if (ec) {
            return stop(retry_reason::node_not_available);
        }
        state_ = diag::endpoint_state::connected;
        bootstrapped_ = true;
        handler_ = std::make_unique<normal_handler>(shared_from_this());
        std::scoped_lock lock(pending_buffer_mutex_);
        if (!pending_buffer_.empty()) {
            for (const auto& buf : pending_buffer_) {
                write(buf);
            }
            pending_buffer_.clear();
            flush();
        }
    }

    void on_resolve(std::error_code ec, const asio::ip::tcp::resolver::results_type& endpoints)
    {
        if (stopped_) {
            return;
        }
        last_active_ = std::chrono::steady_clock::now();
        if (ec) {
            spdlog::error("{} error on resolve: {} ({})", log_prefix_, ec.value(), ec.message());
            return initiate_bootstrap();
        }
        endpoints_ = endpoints;
        do_connect(endpoints_.begin());
        connection_deadline_.expires_after(timeout_defaults::connect_timeout);
        connection_deadline_.async_wait(std::bind(&mcbp_session::check_deadline, shared_from_this(), std::placeholders::_1));
    }

    void do_connect(asio::ip::tcp::resolver::results_type::iterator it)
    {
        if (stopped_) {
            return;
        }
        last_active_ = std::chrono::steady_clock::now();
        if (it != endpoints_.end()) {
            spdlog::debug("{} connecting to {}:{}", log_prefix_, it->endpoint().address().to_string(), it->endpoint().port());
            connection_deadline_.expires_after(timeout_defaults::connect_timeout);
            stream_->async_connect(it->endpoint(), std::bind(&mcbp_session::on_connect, shared_from_this(), std::placeholders::_1, it));
        } else {
            spdlog::error("{} no more endpoints left to connect, will try another address", log_prefix_);
            return initiate_bootstrap();
        }
    }

    void on_connect(const std::error_code& ec, asio::ip::tcp::resolver::results_type::iterator it)
    {
        if (stopped_) {
            return;
        }
        last_active_ = std::chrono::steady_clock::now();
        if (!stream_->is_open() || ec) {
            spdlog::warn("{} unable to connect to {}:{}: {} ({}), is_open={}",
                         log_prefix_,
                         it->endpoint().address().to_string(),
                         it->endpoint().port(),
                         ec.value(),
                         ec.message(),
                         stream_->is_open());
            do_connect(++it);
        } else {
            stream_->set_options();
            local_endpoint_ = stream_->local_endpoint();
            local_endpoint_address_ = local_endpoint_.address().to_string();
            endpoint_ = it->endpoint();
            endpoint_address_ = endpoint_.address().to_string();
            spdlog::debug("{} connected to {}:{}", log_prefix_, endpoint_address_, it->endpoint().port());
            log_prefix_ = fmt::format("[{}/{}/{}/{}] <{}/{}:{}>",
                                      client_id_,
                                      id_,
                                      stream_->log_prefix(),
                                      bucket_name_.value_or("-"),
                                      bootstrap_hostname_,
                                      endpoint_address_,
                                      endpoint_.port());
            handler_ = std::make_unique<bootstrap_handler>(shared_from_this());
            connection_deadline_.expires_at(asio::steady_timer::time_point::max());
            connection_deadline_.cancel();
        }
    }

    void check_deadline(std::error_code ec)
    {
        if (ec == asio::error::operation_aborted || stopped_) {
            return;
        }
        if (connection_deadline_.expiry() <= asio::steady_timer::clock_type::now()) {
            stream_->close();
            connection_deadline_.expires_at(asio::steady_timer::time_point::max());
        }
        connection_deadline_.async_wait(std::bind(&mcbp_session::check_deadline, shared_from_this(), std::placeholders::_1));
    }

    void do_read()
    {
        if (stopped_ || reading_ || !stream_->is_open()) {
            return;
        }
        reading_ = true;
        stream_->async_read_some(
          asio::buffer(input_buffer_),
          [self = shared_from_this(), stream_id = stream_->id()](std::error_code ec, std::size_t bytes_transferred) {
              if (ec == asio::error::operation_aborted || self->stopped_) {
                  return;
              }
              self->last_active_ = std::chrono::steady_clock::now();
              if (ec) {
                  if (stream_id != self->stream_->id()) {
                      spdlog::error(R"({} ignore IO error while reading from the socket: {} ({}), old_id="{}", new_id="{}")",
                                    self->log_prefix_,
                                    ec.value(),
                                    ec.message(),
                                    stream_id,
                                    self->stream_->id());
                      return;
                  }
                  spdlog::error(R"({} IO error while reading from the socket("{}"): {} ({}))",
                                self->log_prefix_,
                                self->stream_->id(),
                                ec.value(),
                                ec.message());
                  return self->stop(retry_reason::socket_closed_while_in_flight);
              }
              self->parser_.feed(self->input_buffer_.data(), self->input_buffer_.data() + ssize_t(bytes_transferred));

              for (;;) {
                  mcbp_message msg{};
                  switch (self->parser_.next(msg)) {
                      case mcbp_parser::result::ok:
                          spdlog::trace(
                            "{} MCBP recv, opaque={}, {:n}", self->log_prefix_, msg.header.opaque, spdlog::to_hex(msg.header_data()));
                          SPDLOG_TRACE("{} MCBP recv, opaque={}{:a}{:a}",
                                       self->log_prefix_,
                                       msg.header.opaque,
                                       spdlog::to_hex(msg.header_data()),
                                       spdlog::to_hex(msg.body));
                          self->handler_->handle(std::move(msg));
                          if (self->stopped_) {
                              return;
                          }
                          break;
                      case mcbp_parser::result::need_data:
                          self->reading_ = false;
                          if (!self->stopped_ && self->stream_->is_open()) {
                              self->do_read();
                          }
                          return;
                      case mcbp_parser::result::failure:
                          return self->stop(retry_reason::kv_temporary_failure);
                  }
              }
          });
    }

    void do_write()
    {
        if (stopped_ || !stream_->is_open()) {
            return;
        }
        std::scoped_lock lock(writing_buffer_mutex_, output_buffer_mutex_);
        if (!writing_buffer_.empty() || output_buffer_.empty()) {
            return;
        }
        std::swap(writing_buffer_, output_buffer_);
        std::vector<asio::const_buffer> buffers;
        buffers.reserve(writing_buffer_.size());
        for (auto& buf : writing_buffer_) {
            buffers.emplace_back(asio::buffer(buf));
        }
        stream_->async_write(buffers, [self = shared_from_this()](std::error_code ec, std::size_t /*unused*/) {
            if (ec == asio::error::operation_aborted || self->stopped_) {
                return;
            }
            self->last_active_ = std::chrono::steady_clock::now();
            if (ec) {
                spdlog::error(R"({} IO error while writing to the socket("{}"): {} ({}))",
                              self->log_prefix_,
                              self->stream_->id(),
                              ec.value(),
                              ec.message());
                return self->stop(retry_reason::socket_closed_while_in_flight);
            }
            {
                std::scoped_lock inner_lock(self->writing_buffer_mutex_);
                self->writing_buffer_.clear();
            }
            self->do_write();
            self->do_read();
        });
    }

    std::string client_id_;
    const std::string id_;
    asio::io_context& ctx_;
    asio::ip::tcp::resolver resolver_;
    std::unique_ptr<stream_impl> stream_;
    asio::steady_timer bootstrap_deadline_;
    asio::steady_timer connection_deadline_;
    asio::steady_timer retry_backoff_;
    couchbase::origin origin_;
    std::optional<std::string> bucket_name_;
    mcbp_parser parser_;
    std::unique_ptr<message_handler> handler_;
    std::function<void(std::error_code, const configuration&)> bootstrap_handler_{};
    std::mutex command_handlers_mutex_{};
    std::map<uint32_t, std::function<void(std::error_code, retry_reason, io::mcbp_message&&)>> command_handlers_{};
    std::vector<std::function<void(const configuration&)>> config_listeners_{};
    std::function<void(io::retry_reason)> on_stop_handler_{};

    bool bootstrapped_{ false };
    std::atomic_bool stopped_{ false };
    bool authenticated_{ false };
    bool bucket_selected_{ false };
    bool supports_gcccp_{ true };
    bool retry_bootstrap_on_bucket_not_found_{ false };

    std::atomic<std::uint32_t> opaque_{ 0 };

    std::array<std::uint8_t, 16384> input_buffer_{};
    std::vector<std::vector<std::uint8_t>> output_buffer_{};
    std::vector<std::vector<std::uint8_t>> pending_buffer_{};
    std::vector<std::vector<std::uint8_t>> writing_buffer_{};
    std::mutex output_buffer_mutex_{};
    std::mutex pending_buffer_mutex_{};
    std::mutex writing_buffer_mutex_{};
    std::string bootstrap_hostname_{};
    std::string bootstrap_port_{};
    asio::ip::tcp::endpoint endpoint_{}; // connected endpoint
    std::string endpoint_address_{};     // cached string with endpoint address
    asio::ip::tcp::endpoint local_endpoint_{};
    std::string local_endpoint_address_{};
    asio::ip::tcp::resolver::results_type endpoints_;
    std::vector<protocol::hello_feature> supported_features_;
    std::optional<configuration> config_;
    std::optional<error_map> error_map_;
    collection_cache collection_cache_;

    std::atomic_bool reading_{ false };

    std::string log_prefix_{};
    std::chrono::time_point<std::chrono::steady_clock> last_active_{};
    diag::endpoint_state state_{ diag::endpoint_state::disconnected };
};
} // namespace couchbase::io
