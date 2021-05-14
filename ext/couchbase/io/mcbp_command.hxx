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

#include <platform/uuid.h>

#include <io/mcbp_session.hxx>
#include <io/retry_orchestrator.hxx>

#include <protocol/cmd_get_collection_id.hxx>
#include <functional>
#include <utility>

namespace couchbase::operations
{

using mcbp_command_handler = std::function<void(std::error_code, std::optional<io::mcbp_message>)>;

template<typename Manager, typename Request>
struct mcbp_command : public std::enable_shared_from_this<mcbp_command<Manager, Request>> {
    using encoded_request_type = typename Request::encoded_request_type;
    using encoded_response_type = typename Request::encoded_response_type;
    asio::steady_timer deadline;
    asio::steady_timer retry_backoff;
    Request request;
    encoded_request_type encoded;
    std::optional<std::uint32_t> opaque_{};
    std::shared_ptr<io::mcbp_session> session_{};
    mcbp_command_handler handler_{};
    std::shared_ptr<Manager> manager_{};
    std::string id_;

    mcbp_command(asio::io_context& ctx, std::shared_ptr<Manager> manager, Request req)
      : deadline(ctx)
      , retry_backoff(ctx)
      , request(req)
      , manager_(manager)
      , id_(uuid::to_string(uuid::random()))
    {
    }

    void start(mcbp_command_handler&& handler)
    {
        handler_ = std::move(handler);
        deadline.expires_after(request.timeout);
        deadline.async_wait([self = this->shared_from_this()](std::error_code ec) {
            if (ec == asio::error::operation_aborted) {
                return;
            }
            self->cancel(io::retry_reason::do_not_retry);
        });
    }

    void cancel(io::retry_reason reason)
    {
        if (opaque_ && session_) {
            if (session_->cancel(opaque_.value(), asio::error::operation_aborted, reason)) {
                handler_ = nullptr;
            }
        }
        invoke_handler(request.retries.idempotent ? error::common_errc::unambiguous_timeout : error::common_errc::ambiguous_timeout);
        retry_backoff.cancel();
        deadline.cancel();
    }

    void invoke_handler(std::error_code ec, std::optional<io::mcbp_message> msg = {})
    {
        if (handler_) {
            handler_(ec, std::move(msg));
        }
        handler_ = nullptr;
    }

    void request_collection_id()
    {
        if (session_->is_stopped()) {
            return manager_->map_and_send(this->shared_from_this());
        }
        protocol::client_request<protocol::get_collection_id_request_body> req;
        req.opaque(session_->next_opaque());
        req.body().collection_path(request.id.collection);
        session_->write_and_subscribe(
          req.opaque(),
          req.data(session_->supports_feature(protocol::hello_feature::snappy)),
          [self = this->shared_from_this()](std::error_code ec, io::retry_reason /* reason */, io::mcbp_message&& msg) mutable {
              if (ec == asio::error::operation_aborted) {
                  return self->invoke_handler(error::common_errc::ambiguous_timeout);
              }
              if (ec == error::common_errc::collection_not_found) {
                  if (self->request.id.collection_uid) {
                      return self->invoke_handler(ec);
                  }
                  return self->handle_unknown_collection();
              }
              if (ec) {
                  return self->invoke_handler(ec);
              }
              protocol::client_response<protocol::get_collection_id_response_body> resp(std::move(msg));
              self->session_->update_collection_uid(self->request.id.collection, resp.body().collection_uid());
              self->request.id.collection_uid = resp.body().collection_uid();
              return self->send();
          });
    }

    void handle_unknown_collection()
    {
        auto backoff = std::chrono::milliseconds(500);
        auto time_left = deadline.expiry() - std::chrono::steady_clock::now();
        spdlog::debug(R"({} unknown collection response for "{}/{}/{}", time_left={}ms, id="{}")",
                      session_->log_prefix(),
                      request.id.bucket,
                      request.id.collection,
                      request.id.key,
                      std::chrono::duration_cast<std::chrono::milliseconds>(time_left).count(),
                      id_);
        if (time_left < backoff) {
            return invoke_handler(make_error_code(request.retries.idempotent ? error::common_errc::unambiguous_timeout
                                                                             : error::common_errc::ambiguous_timeout));
        }
        retry_backoff.expires_after(backoff);
        retry_backoff.async_wait([self = this->shared_from_this()](std::error_code ec) mutable {
            if (ec == asio::error::operation_aborted) {
                return;
            }
            self->request_collection_id();
        });
    }

    void send()
    {
        opaque_ = session_->next_opaque();
        request.opaque = *opaque_;
        if (request.id.use_collections && !request.id.collection_uid) {
            if (session_->supports_feature(protocol::hello_feature::collections)) {
                auto collection_id = session_->get_collection_uid(request.id.collection);
                if (collection_id) {
                    request.id.collection_uid = *collection_id;
                } else {
                    spdlog::debug(R"({} no cache entry for collection, resolve collection id for "{}/{}/{}", timeout={}ms, id="{}")",
                                  session_->log_prefix(),
                                  request.id.bucket,
                                  request.id.collection,
                                  request.id.key,
                                  request.timeout.count(),
                                  id_);
                    return request_collection_id();
                }
            } else {
                if (!request.id.collection.empty() && request.id.collection != "_default._default") {
                    return invoke_handler(error::common_errc::unsupported_operation);
                }
            }
        }

        if (auto ec = request.encode_to(encoded, session_->context()); ec) {
            return invoke_handler(ec);
        }

        session_->write_and_subscribe(
          request.opaque,
          encoded.data(session_->supports_feature(protocol::hello_feature::snappy)),
          [self = this->shared_from_this()](std::error_code ec, io::retry_reason reason, io::mcbp_message&& msg) mutable {
              self->retry_backoff.cancel();
              if (ec == asio::error::operation_aborted) {
                  return self->invoke_handler(make_error_code(self->request.retries.idempotent ? error::common_errc::unambiguous_timeout
                                                                                               : error::common_errc::ambiguous_timeout));
              }
              if (ec == error::common_errc::request_canceled) {
                  if (reason == io::retry_reason::do_not_retry) {
                      return self->invoke_handler(ec);
                  }
                  return io::retry_orchestrator::maybe_retry(self->manager_, self, reason, ec);
              }
              protocol::status status = protocol::status::invalid;
              std::optional<error_map::error_info> error_code{};
              if (protocol::is_valid_status(msg.header.status())) {
                  status = protocol::status(msg.header.status());
              } else {
                  error_code = self->session_->decode_error_code(msg.header.status());
              }
              if (status == protocol::status::not_my_vbucket) {
                  self->session_->handle_not_my_vbucket(std::move(msg));
                  return io::retry_orchestrator::maybe_retry(self->manager_, self, io::retry_reason::kv_not_my_vbucket, ec);
              }
              if (status == protocol::status::unknown_collection) {
                  return self->handle_unknown_collection();
              }
              if (error_code && error_code.value().has_retry_attribute()) {
                  reason = io::retry_reason::kv_error_map_retry_indicated;
              } else {
                  switch (status) {
                      case protocol::status::locked:
                          if (encoded_request_type::body_type::opcode != protocol::client_opcode::unlock) {
                              /**
                               * special case for unlock command, when it should not be retried, because it does not make sense
                               * (someone else unlocked the document)
                               */
                              reason = io::retry_reason::kv_locked;
                          }
                          break;
                      case protocol::status::temporary_failure:
                          reason = io::retry_reason::kv_temporary_failure;
                          break;
                      case protocol::status::sync_write_in_progress:
                          reason = io::retry_reason::kv_sync_write_in_progress;
                          break;
                      case protocol::status::sync_write_re_commit_in_progress:
                          reason = io::retry_reason::kv_sync_write_re_commit_in_progress;
                          break;
                      default:
                          break;
                  }
              }
              if (reason == io::retry_reason::do_not_retry) {
                  self->deadline.cancel();
                  self->invoke_handler(ec, msg);
              } else {
                  io::retry_orchestrator::maybe_retry(self->manager_, self, reason, ec);
              }
          });
    }

    void send_to(std::shared_ptr<io::mcbp_session> session)
    {
        if (!handler_) {
            return;
        }
        session_ = std::move(session);
        send();
    }
};

} // namespace couchbase::operations
