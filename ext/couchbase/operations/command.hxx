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

#include <io/mcbp_session.hxx>
#include <io/http_session.hxx>
#include <protocol/cmd_get_collection_id.hxx>

namespace couchbase::operations
{

template<typename Request>
struct command : public std::enable_shared_from_this<command<Request>> {
    using encoded_request_type = typename Request::encoded_request_type;
    using encoded_response_type = typename Request::encoded_response_type;
    asio::steady_timer deadline;
    asio::steady_timer retry_backoff;
    Request request;
    encoded_request_type encoded;

    command(asio::io_context& ctx, Request req)
      : deadline(ctx)
      , retry_backoff(ctx)
      , request(req)
    {
    }

    template<typename Handler>
    void request_collection_id(std::shared_ptr<io::mcbp_session> session, Handler&& handler)
    {
        protocol::client_request<protocol::get_collection_id_request_body> req;
        req.opaque(session->next_opaque());
        req.body().collection_path(request.id.collection);
        session->write_and_subscribe(
          req.opaque(),
          req.data(session->supports_feature(protocol::hello_feature::snappy)),
          [self = this->shared_from_this(), session, handler = std::forward<Handler>(handler)](std::error_code ec,
                                                                                               io::mcbp_message&& msg) mutable {
              if (ec == asio::error::operation_aborted) {
                  return handler(make_response(std::make_error_code(error::common_errc::ambiguous_timeout), self->request, {}));
              }
              if (ec == std::make_error_code(error::common_errc::collection_not_found)) {
                  if (self->request.id.collection_uid) {
                      return self->handle_unknown_collection(std::move(session), std::forward<Handler>(handler));
                  }
                  return handler(make_response(ec, self->request, {}));
              }
              if (ec) {
                  return handler(make_response(ec, self->request, {}));
              }
              protocol::client_response<protocol::get_collection_id_response_body> resp(msg);
              session->update_collection_uid(self->request.id.collection, resp.body().collection_uid());
              self->request.id.collection_uid = resp.body().collection_uid();
              return self->send_to(std::move(session), std::forward<Handler>(handler));
          });
    }

    template<typename Handler>
    void handle_unknown_collection(std::shared_ptr<io::mcbp_session> session, Handler&& handler)
    {
        auto backoff = std::chrono::milliseconds(500);
        auto time_left = deadline.expiry() - std::chrono::steady_clock::now();
        spdlog::debug("{} unknown collection response for \"{}/{}/{}\", time_left={}ms",
                      session->log_prefix(),
                      request.id.bucket,
                      request.id.collection,
                      request.id.key,
                      std::chrono::duration_cast<std::chrono::milliseconds>(time_left).count());
        if (time_left < backoff) {
            return handler(make_response(std::make_error_code(error::common_errc::ambiguous_timeout), request, {}));
        }
        retry_backoff.expires_after(backoff);
        retry_backoff.async_wait(
          [self = this->shared_from_this(), session, handler = std::forward<Handler>(handler)](std::error_code ec) mutable {
              if (ec == asio::error::operation_aborted) {
                  return;
              }
              self->request_collection_id(std::move(session), std::forward<Handler>(handler));
          });
    }

    template<typename Handler>
    void send_to(std::shared_ptr<io::mcbp_session> session, Handler&& handler)
    {
        auto opaque = session->next_opaque();
        request.opaque = opaque;
        if (!request.id.collection_uid) {
            if (session->supports_feature(protocol::hello_feature::collections)) {
                auto collection_id = session->get_collection_uid(request.id.collection);
                if (collection_id) {
                    request.id.collection_uid = *collection_id;
                } else {
                    spdlog::debug("{} no cache entry for collection, resolve collection id for \"{}/{}/{}\", timeout={}ms",
                                  session->log_prefix(),
                                  request.id.bucket,
                                  request.id.collection,
                                  request.id.key,
                                  request.timeout.count());
                    return request_collection_id(std::move(session), std::forward<Handler>(handler));
                }
            } else {
                if (!request.id.collection.empty() && request.id.collection != "_default._default") {
                    return handler(make_response(std::make_error_code(error::common_errc::unsupported_operation), request, {}));
                }
            }
        }
        request.encode_to(encoded);

        session->write_and_subscribe(
          request.opaque,
          encoded.data(session->supports_feature(protocol::hello_feature::snappy)),
          [self = this->shared_from_this(), session, handler = std::forward<Handler>(handler)](std::error_code ec,
                                                                                               io::mcbp_message&& msg) mutable {
              self->retry_backoff.cancel();
              if (ec == asio::error::operation_aborted) {
                  return handler(make_response(std::make_error_code(error::common_errc::ambiguous_timeout), self->request, {}));
              }
              if (ec == std::make_error_code(error::common_errc::request_canceled)) {
                  return handler(make_response(ec, self->request, {}));
              }
              encoded_response_type resp(msg);
              if (resp.status() == protocol::status::unknown_collection) {
                  return self->handle_unknown_collection(session, std::forward<Handler>(handler));
              }
              self->deadline.cancel();
              handler(make_response(ec, self->request, resp));
          });

        if (deadline.expiry().time_since_epoch() == std::chrono::milliseconds::zero()) {
            deadline.expires_after(request.timeout);
            deadline.async_wait([session, opaque](std::error_code ec) {
                if (ec == asio::error::operation_aborted) {
                    return;
                }
                session->cancel(opaque, asio::error::operation_aborted);
            });
        }
    }

    template<typename Handler>
    void send_to(std::shared_ptr<io::http_session> session, Handler&& handler)
    {
        encoded.type = Request::type;
        request.encode_to(encoded);
        encoded.headers["client-context-id"] = request.client_context_id;
        auto log_prefix = session->log_prefix();
        spdlog::debug("{} HTTP request: {}, method={}, path={}, client_context_id={}, timeout={}ms",
                      log_prefix,
                      encoded.type,
                      encoded.method,
                      encoded.path,
                      request.client_context_id,
                      request.timeout.count());
        SPDLOG_TRACE("{} HTTP request: {}, method={}, path={}, client_context_id={}, timeout={}ms{:a}",
                     log_prefix,
                     encoded.type,
                     encoded.method,
                     encoded.path,
                     request.client_context_id,
                     request.timeout.count(),
                     spdlog::to_hex(encoded.body));
        session->write_and_subscribe(
          encoded,
          [self = this->shared_from_this(), log_prefix, handler = std::forward<Handler>(handler)](std::error_code ec,
                                                                                                  io::http_response&& msg) mutable {
              self->deadline.cancel();
              encoded_response_type resp(msg);
              spdlog::debug("{} HTTP response: {}, client_context_id={}, status={}",
                            log_prefix,
                            self->request.type,
                            self->request.client_context_id,
                            resp.status_code);
              SPDLOG_TRACE("{} HTTP response: {}, client_context_id={}, status={}{:a}",
                           log_prefix,
                           self->request.type,
                           self->request.client_context_id,
                           resp.status_code,
                           spdlog::to_hex(resp.body));
              handler(make_response(ec, self->request, resp));
          });
        deadline.expires_after(request.timeout);
        deadline.async_wait([session](std::error_code ec) {
            if (ec == asio::error::operation_aborted) {
                return;
            }
            session->stop();
        });
    }
};

} // namespace couchbase::operations
