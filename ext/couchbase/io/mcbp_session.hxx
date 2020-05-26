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

#include <tao/json.hpp>

#include <asio.hpp>

#include <platform/uuid.h>

#include <io/mcbp_message.hxx>
#include <io/mcbp_parser.hxx>

#include <protocol/hello_feature.hxx>
#include <protocol/client_request.hxx>
#include <protocol/client_response.hxx>
#include <protocol/server_request.hxx>
#include <protocol/cmd_hello.hxx>
#include <protocol/cmd_sasl_list_mechs.hxx>
#include <protocol/cmd_sasl_auth.hxx>
#include <protocol/cmd_sasl_step.hxx>
#include <protocol/cmd_select_bucket.hxx>
#include <protocol/cmd_get_cluster_config.hxx>
#include <protocol/cmd_get_error_map.hxx>
#include <protocol/cmd_get_collections_manifest.hxx>
#include <protocol/cmd_get.hxx>
#include <protocol/cmd_cluster_map_change_notification.hxx>

#include <cbsasl/client.h>

#include <spdlog/fmt/bin_to_hex.h>

#include <errors.hxx>
#include <version.hxx>

namespace couchbase::io
{

class mcbp_session : public std::enable_shared_from_this<mcbp_session>
{
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
          , sasl_([this]() -> std::string { return session_->username_; },
                  [this]() -> std::string { return session_->password_; },
                  { "SCRAM-SHA512", "SCRAM-SHA256", "SCRAM-SHA1", "PLAIN" })
        {
            tao::json::value user_agent{
                { "a", fmt::format("ruby_sdk/{}.{}.{}", BACKEND_VERSION_MAJOR, BACKEND_VERSION_MINOR, BACKEND_VERSION_PATCH) },
                { "i", fmt::format("{}/{}", uuid::to_string(session_->client_id_), uuid::to_string(session_->id_)) }
            };
            protocol::client_request<protocol::hello_request_body> hello_req;
            hello_req.opaque(session_->next_opaque());
            hello_req.body().user_agent(tao::json::to_string(user_agent));
            session_->write(hello_req.data());

            protocol::client_request<protocol::sasl_list_mechs_request_body> list_req;
            list_req.opaque(session_->next_opaque());
            session_->write(list_req.data());

            protocol::client_request<protocol::sasl_auth_request_body> auth_req;
            sasl::error sasl_code;
            std::string_view sasl_payload;
            std::tie(sasl_code, sasl_payload) = sasl_.start();
            auth_req.opaque(session_->next_opaque());
            auth_req.body().mechanism(sasl_.get_name());
            auth_req.body().sasl_data(sasl_payload);
            session_->write(auth_req.data());

            session_->flush();
        }

        void complete(std::error_code ec)
        {
            session_->invoke_bootstrap_handler(ec);
            if (!ec) {
                session_->handler_ = std::make_unique<normal_handler>(session_);
            }
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

                protocol::client_request<protocol::get_collections_manifest_request_body> gcm_req;
                gcm_req.opaque(session_->next_opaque());
                session_->write(gcm_req.data());
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
            auto opcode = static_cast<protocol::client_opcode>(msg.header.opcode);
            switch (opcode) {
                case protocol::client_opcode::hello: {
                    protocol::client_response<protocol::hello_response_body> resp(msg);
                    if (resp.status() == protocol::status::success) {
                        session_->supported_features_ = resp.body().supported_features();
                    } else {
                        spdlog::warn("unexpected message status during bootstrap: {}", resp.error_message());
                        return complete(std::make_error_code(error::network_errc::handshake_failure));
                    }
                } break;
                case protocol::client_opcode::sasl_list_mechs: {
                    protocol::client_response<protocol::sasl_list_mechs_response_body> resp(msg);
                    if (resp.status() != protocol::status::success) {
                        spdlog::warn("unexpected message status during bootstrap: {}", resp.error_message());
                        return complete(std::make_error_code(error::common_errc::authentication_failure));
                    }
                } break;
                case protocol::client_opcode::sasl_auth: {
                    protocol::client_response<protocol::sasl_auth_response_body> resp(msg);
                    if (resp.status() == protocol::status::success) {
                        return auth_success();
                    }
                    if (resp.status() == protocol::status::auth_continue) {
                        sasl::error sasl_code;
                        std::string_view sasl_payload;
                        std::tie(sasl_code, sasl_payload) = sasl_.step(resp.body().value());
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
                            spdlog::error("unable to authenticate: sasl_code={}", sasl_code);
                            return complete(std::make_error_code(error::common_errc::authentication_failure));
                        }
                    } else {
                        spdlog::warn("unexpected message status during bootstrap: {} (opcode={})", resp.error_message(), opcode);
                        return complete(std::make_error_code(error::common_errc::authentication_failure));
                    }
                } break;
                case protocol::client_opcode::sasl_step: {
                    protocol::client_response<protocol::sasl_step_response_body> resp(msg);
                    if (resp.status() == protocol::status::success) {
                        return auth_success();
                    }
                    return complete(std::make_error_code(error::common_errc::authentication_failure));
                }
                case protocol::client_opcode::get_error_map: {
                    protocol::client_response<protocol::get_error_map_response_body> resp(msg);
                    if (resp.status() == protocol::status::success) {
                        session_->errmap_.emplace(resp.body().errmap());
                    } else {
                        spdlog::warn("unexpected message status during bootstrap: {} (opcode={})", resp.error_message(), opcode);
                        return complete(std::make_error_code(error::network_errc::protocol_error));
                    }
                } break;
                case protocol::client_opcode::get_collections_manifest: {
                    protocol::client_response<protocol::get_collections_manifest_response_body> resp(msg);
                    if (resp.status() == protocol::status::success) {
                        session_->manifest_.emplace(resp.body().manifest());
                        spdlog::trace(
                          "collections manifest for bucket \"{}\": {}", session_->bucket_name_.value_or(""), *session_->manifest_);
                    } else if (resp.status() == protocol::status::no_collections_manifest) {
                        spdlog::trace("collection manifest is not available for bucket \"{}\": {}",
                                      session_->bucket_name_.value_or(""),
                                      resp.error_message());
                    } else {
                        spdlog::warn("unexpected message status during bootstrap: {} (opcode={})", resp.error_message(), opcode);
                        return complete(std::make_error_code(error::network_errc::protocol_error));
                    }
                } break;
                case protocol::client_opcode::select_bucket: {
                    protocol::client_response<protocol::select_bucket_response_body> resp(msg);
                    if (resp.status() == protocol::status::success) {
                        spdlog::trace("selected bucket: {}", session_->bucket_name_.value_or(""));
                        session_->bucket_selected_ = true;
                    } else if (resp.status() == protocol::status::no_access) {
                        spdlog::trace("unable to select bucket: {}, probably the bucket does not exist",
                                      session_->bucket_name_.value_or(""));
                        session_->bucket_selected_ = false;
                        return complete(std::make_error_code(error::common_errc::bucket_not_found));
                    } else {
                        spdlog::warn("unexpected message status during bootstrap: {}", resp.error_message());
                        return complete(std::make_error_code(error::common_errc::bucket_not_found));
                    }
                } break;
                case protocol::client_opcode::get_cluster_config: {
                    protocol::client_response<protocol::get_cluster_config_response_body> resp(msg);
                    if (resp.status() == protocol::status::success) {
                        session_->update_configuration(resp.body().config());
                        complete({});
                    } else {
                        spdlog::warn("unexpected message status during bootstrap: {} (opcode={})", resp.error_message(), opcode);
                        return complete(std::make_error_code(error::network_errc::protocol_error));
                    }
                } break;
                default:
                    spdlog::warn("unexpected message during bootstrap: {}", opcode);
                    return complete(std::make_error_code(error::network_errc::protocol_error));
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
            fetch_config({});
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
            switch (auto magic = static_cast<protocol::magic>(msg.header.magic)) {
                case protocol::magic::client_response:
                    Expects(protocol::is_valid_client_opcode(msg.header.opcode));
                    switch (auto opcode = static_cast<protocol::client_opcode>(msg.header.opcode)) {
                        case protocol::client_opcode::get_cluster_config: {
                            protocol::client_response<protocol::get_cluster_config_response_body> resp(msg);
                            if (resp.status() == protocol::status::success) {
                                if (session_) {
                                    session_->update_configuration(resp.body().config());
                                }
                            } else {
                                spdlog::warn("unexpected message status: {}", resp.error_message());
                            }
                        } break;
                        case protocol::client_opcode::get:
                        case protocol::client_opcode::upsert:
                        case protocol::client_opcode::remove:
                        case protocol::client_opcode::subdoc_multi_lookup:
                        case protocol::client_opcode::subdoc_multi_mutation: {
                            std::uint32_t opaque = msg.header.opaque;
                            std::uint16_t status = ntohs(msg.header.specific);
                            auto handler = session_->command_handlers_.find(opaque);
                            if (handler != session_->command_handlers_.end()) {
                                handler->second(session_->map_status_code(opcode, status), std::move(msg));
                                session_->command_handlers_.erase(handler);
                            } else {
                                spdlog::trace("unexpected orphan response opcode={}, opaque={}", msg.header.opcode, msg.header.opaque);
                            }
                        } break;
                        default:
                            spdlog::trace("unexpected client response: {}", opcode);
                    }
                    break;
                case protocol::magic::server_request:
                    Expects(protocol::is_valid_server_request_opcode(msg.header.opcode));
                    switch (auto opcode = static_cast<protocol::server_opcode>(msg.header.opcode)) {
                        case protocol::server_opcode::cluster_map_change_notification: {
                            protocol::server_request<protocol::cluster_map_change_notification_request_body> req(msg);
                            if (session_) {
                                if ((!req.body().config().bucket.has_value() && req.body().bucket().empty()) ||
                                    (session_->bucket_name_.has_value() && !req.body().bucket().empty() &&
                                     session_->bucket_name_.value() == req.body().bucket())) {
                                    session_->update_configuration(req.body().config());
                                }
                            }
                        } break;
                        default:
                            spdlog::trace("unexpected server request: {}", opcode);
                    }
                    break;
                case protocol::magic::client_request:
                case protocol::magic::alt_client_request:
                case protocol::magic::alt_client_response:
                case protocol::magic::server_response:
                    spdlog::trace("unexpected magic: {}, opcode={}, opaque={}{}", magic, msg.header.opcode, msg.header.opaque);
                    break;
            }
        }

        void fetch_config(std::error_code ec)
        {
            if (ec == asio::error::operation_aborted) {
                return;
            }
            if (stopped_ || !session_) {
                return;
            }
            if (session_->bootstrapped_) {
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
    mcbp_session(uuid::uuid_t client_id, asio::io_context& ctx, std::optional<std::string> bucket_name = {})
      : client_id_(client_id)
      , id_(uuid::random())
      , ctx_(ctx)
      , resolver_(ctx_)
      , strand_(asio::make_strand(ctx_))
      , socket_(strand_)
      , deadline_timer_(ctx_)
      , bucket_name_(std::move(bucket_name))
    {
    }

    ~mcbp_session()
    {
        stop();
    }

    void bootstrap(const std::string& hostname,
                   const std::string& service,
                   const std::string& username,
                   const std::string& password,
                   std::function<void(std::error_code, configuration)>&& handler)
    {
        username_ = username;
        password_ = password;
        bootstrap_handler_ = std::move(handler);
        resolver_.async_resolve(
          hostname, service, std::bind(&mcbp_session::on_resolve, this, std::placeholders::_1, std::placeholders::_2));
    }

    [[nodiscard]] std::string id()
    {
        return uuid::to_string(id_);
    }

    void stop()
    {
        if (stopped_) {
            return;
        }
        stopped_ = true;
        deadline_timer_.cancel();
        resolver_.cancel();
        if (socket_.is_open()) {
            socket_.close();
        }
        if (handler_) {
            handler_->stop();
        }
    }

    void write(const std::vector<uint8_t>& buf)
    {
        if (stopped_) {
            return;
        }
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
                             std::function<void(std::error_code, io::mcbp_message&&)> handler)
    {
        if (stopped_) {
            return;
        }
        command_handlers_.emplace(opaque, std::move(handler));
        if (bootstrapped_ && socket_.is_open()) {
            write_and_flush(data);
        } else {
            pending_buffer_.push_back(data);
        }
    }

    bool supports_feature(protocol::hello_feature feature)
    {
        return std::find(supported_features_.begin(), supported_features_.end(), feature) != supported_features_.end();
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

    [[nodiscard]] uint32_t next_opaque()
    {
        return ++opaque_;
    }

    std::error_code map_status_code(protocol::client_opcode opcode, uint16_t status)
    {
        switch (static_cast<protocol::status>(status)) {
            case protocol::status::success:
            case protocol::status::subdoc_multi_path_failure:
            case protocol::status::subdoc_success_deleted:
            case protocol::status::subdoc_multi_path_failure_deleted:
                return {};

            case protocol::status::not_found:
            case protocol::status::not_stored:
                return std::make_error_code(error::key_value_errc::document_not_found);

            case protocol::status::exists:
                if (opcode == protocol::client_opcode::replace || opcode == protocol::client_opcode::remove) {
                    return std::make_error_code(error::common_errc::cas_mismatch);
                }
                return std::make_error_code(error::key_value_errc::document_exists);

            case protocol::status::too_big:
                return std::make_error_code(error::key_value_errc::value_too_large);

            case protocol::status::invalid:
            case protocol::status::xattr_invalid:
                return std::make_error_code(error::common_errc::invalid_argument);

            case protocol::status::delta_bad_value:
                return std::make_error_code(error::key_value_errc::delta_invalid);

            case protocol::status::no_bucket:
                return std::make_error_code(error::common_errc::bucket_not_found);

            case protocol::status::locked:
                return std::make_error_code(error::key_value_errc::document_locked);

            case protocol::status::auth_stale:
            case protocol::status::auth_error:
            case protocol::status::no_access:
                return std::make_error_code(error::common_errc::authentication_failure);

            case protocol::status::not_supported:
            case protocol::status::unknown_command:
                return std::make_error_code(error::common_errc::unsupported_operation);

            case protocol::status::internal:
                return std::make_error_code(error::common_errc::internal_server_failure);

            case protocol::status::busy:
            case protocol::status::temp_failure:
            case protocol::status::no_memory:
            case protocol::status::not_initialized:
                return std::make_error_code(error::common_errc::temporary_failure);

            case protocol::status::unknown_collection:
                return std::make_error_code(error::common_errc::collection_not_found);

            case protocol::status::unknown_scope:
                return std::make_error_code(error::common_errc::scope_not_found);

            case protocol::status::durability_invalid_level:
                return std::make_error_code(error::key_value_errc::durability_level_not_available);

            case protocol::status::durability_impossible:
                return std::make_error_code(error::key_value_errc::durability_impossible);

            case protocol::status::sync_write_in_progress:
                return std::make_error_code(error::key_value_errc::durable_write_in_progress);

            case protocol::status::sync_write_ambiguous:
                return std::make_error_code(error::key_value_errc::durability_ambiguous);

            case protocol::status::sync_write_re_commit_in_progress:
                return std::make_error_code(error::key_value_errc::durable_write_re_commit_in_progress);

            case protocol::status::subdoc_path_not_found:
                return std::make_error_code(error::key_value_errc::path_not_found);

            case protocol::status::subdoc_path_mismatch:
                return std::make_error_code(error::key_value_errc::path_mismatch);

            case protocol::status::subdoc_path_invalid:
                return std::make_error_code(error::key_value_errc::path_invalid);

            case protocol::status::subdoc_path_too_big:
                return std::make_error_code(error::key_value_errc::path_too_big);

            case protocol::status::subdoc_doc_too_deep:
                return std::make_error_code(error::key_value_errc::value_too_deep);

            case protocol::status::subdoc_value_cannot_insert:
                return std::make_error_code(error::key_value_errc::value_invalid);

            case protocol::status::subdoc_doc_not_json:
                return std::make_error_code(error::key_value_errc::document_not_json);

            case protocol::status::subdoc_num_range_error:
                return std::make_error_code(error::key_value_errc::number_too_big);

            case protocol::status::subdoc_delta_invalid:
                return std::make_error_code(error::key_value_errc::delta_invalid);

            case protocol::status::subdoc_path_exists:
                return std::make_error_code(error::key_value_errc::path_exists);

            case protocol::status::subdoc_value_too_deep:
                return std::make_error_code(error::key_value_errc::value_too_deep);

            case protocol::status::subdoc_invalid_combo:
            case protocol::status::subdoc_xattr_invalid_flag_combo:
            case protocol::status::subdoc_xattr_invalid_key_combo:
                return std::make_error_code(error::key_value_errc::xattr_invalid_key_combo);

            case protocol::status::subdoc_xattr_unknown_macro:
                return std::make_error_code(error::key_value_errc::xattr_unknown_macro);

            case protocol::status::subdoc_xattr_unknown_vattr:
                return std::make_error_code(error::key_value_errc::xattr_unknown_virtual_attribute);

            case protocol::status::subdoc_xattr_cannot_modify_vattr:
                return std::make_error_code(error::key_value_errc::xattr_cannot_modify_virtual_attribute);

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
        spdlog::warn("unknown status code: {} (opcode={})", status, opcode);
        return std::make_error_code(error::network_errc::protocol_error);
    }

    void update_configuration(configuration&& config)
    {
        if (!config_ || config.rev > config_->rev) {
            config_.emplace(config);
            spdlog::trace("received new configuration: {}", config_.value());
        }
    }

  private:
    void invoke_bootstrap_handler(std::error_code ec)
    {
        if (!bootstrapped_ && bootstrap_handler_) {
            bootstrap_handler_(ec, config_.value_or(configuration{}));
            bootstrap_handler_ = nullptr;
        }
        bootstrapped_ = true;
        if (ec) {
            stop();
        }
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
        if (ec) {
            spdlog::error("error on resolve: {}", ec.message());
            return invoke_bootstrap_handler(std::make_error_code(error::network_errc::resolve_failure));
        }
        endpoints_ = endpoints;
        do_connect(endpoints_.begin());
        deadline_timer_.async_wait(std::bind(&mcbp_session::check_deadline, this, std::placeholders::_1));
    }

    void do_connect(asio::ip::tcp::resolver::results_type::iterator it)
    {
        if (stopped_) {
            return;
        }
        if (it != endpoints_.end()) {
            spdlog::debug("connecting to {}:{}", it->endpoint().address().to_string(), it->endpoint().port());
            deadline_timer_.expires_after(std::chrono::seconds(10));
            socket_.async_connect(it->endpoint(), std::bind(&mcbp_session::on_connect, this, std::placeholders::_1, it));
        } else {
            spdlog::error("no more endpoints left to connect");
            invoke_bootstrap_handler(std::make_error_code(error::network_errc::no_endpoints_left));
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
            socket_.set_option(asio::ip::tcp::no_delay{ true });
            socket_.set_option(asio::socket_base::keep_alive{ true });
            endpoint_ = it->endpoint();
            spdlog::debug("connected to {}:{}", endpoint_.address().to_string(), it->endpoint().port());
            handler_ = std::make_unique<bootstrap_handler>(shared_from_this());
            deadline_timer_.expires_at(asio::steady_timer::time_point::max());
            deadline_timer_.cancel();
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
        deadline_timer_.async_wait(std::bind(&mcbp_session::check_deadline, this, std::placeholders::_1));
    }

    void do_read()
    {
        if (stopped_) {
            return;
        }
        if (reading_) {
            return;
        }
        reading_ = true;
        socket_.async_read_some(asio::buffer(input_buffer_),
                                [self = shared_from_this()](std::error_code ec, std::size_t bytes_transferred) {
                                    if (ec == asio::error::operation_aborted || self->stopped_) {
                                        return;
                                    }
                                    if (ec) {
                                        spdlog::error("[{}] [{}] IO error while reading from the socket: {}",
                                                      uuid::to_string(self->id_),
                                                      self->endpoint_.address().to_string(),
                                                      ec.message());
                                        return self->stop();
                                    }
                                    self->parser_.feed(self->input_buffer_.data(), self->input_buffer_.data() + ssize_t(bytes_transferred));

                                    for (;;) {
                                        mcbp_message msg{};
                                        switch (self->parser_.next(msg)) {
                                            case mcbp_parser::ok:
                                                self->handler_->handle(std::move(msg));
                                                break;
                                            case mcbp_parser::need_data:
                                                self->reading_ = false;
                                                return self->do_read();
                                            case mcbp_parser::failure:
                                                ec = std::make_error_code(error::common_errc::parsing_failure);
                                                return self->stop();
                                        }
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
        asio::async_write(socket_, buffers, [self = shared_from_this()](std::error_code ec, std::size_t /*unused*/) {
            if (self->stopped_) {
                return;
            }
            if (ec) {
                spdlog::error("[{}] [{}] IO error while writing to the socket: {}",
                              uuid::to_string(self->id_),
                              self->endpoint_.address().to_string(),
                              ec.message());
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
    std::optional<std::string> bucket_name_;
    mcbp_parser parser_;
    std::unique_ptr<message_handler> handler_;
    std::function<void(std::error_code, configuration)> bootstrap_handler_;
    std::map<uint32_t, std::function<void(std::error_code, io::mcbp_message&&)>> command_handlers_{};

    bool bootstrapped_{ false };
    std::atomic_bool stopped_{ false };
    bool authenticated_{ false };
    bool bucket_selected_{ false };

    std::atomic<std::uint32_t> opaque_{ 0 };

    std::string username_;
    std::string password_;

    std::array<std::uint8_t, 16384> input_buffer_{};
    std::vector<std::vector<std::uint8_t>> output_buffer_{};
    std::vector<std::vector<std::uint8_t>> pending_buffer_{};
    std::vector<std::vector<std::uint8_t>> writing_buffer_{};
    asio::ip::tcp::endpoint endpoint_{}; // connected endpoint
    asio::ip::tcp::resolver::results_type endpoints_;
    std::vector<protocol::hello_feature> supported_features_;
    std::optional<configuration> config_;
    std::optional<error_map> errmap_;
    std::optional<collections_manifest> manifest_;

    std::atomic_bool reading_{ false };
};
} // namespace couchbase::io
