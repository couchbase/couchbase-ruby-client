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

#include <io/http_session.hxx>

namespace couchbase::operations
{

template<typename Request>
struct http_command : public std::enable_shared_from_this<http_command<Request>> {
    using encoded_request_type = typename Request::encoded_request_type;
    using encoded_response_type = typename Request::encoded_response_type;
    asio::steady_timer deadline;
    asio::steady_timer retry_backoff;
    Request request;
    encoded_request_type encoded;

    http_command(asio::io_context& ctx, Request req)
      : deadline(ctx)
      , retry_backoff(ctx)
      , request(req)
    {
    }

    template<typename Handler>
    void send_to(std::shared_ptr<io::http_session> session, Handler&& handler)
    {
        encoded.type = Request::type;
        auto encoding_ec = request.encode_to(encoded, session->http_context());
        if (encoding_ec) {
            return handler(make_response(encoding_ec, request, {}));
        }
        encoded.headers["client-context-id"] = request.client_context_id;
        auto log_prefix = session->log_prefix();
        spdlog::trace(R"({} HTTP request: {}, method={}, path="{}", client_context_id="{}", timeout={}ms)",
                      log_prefix,
                      encoded.type,
                      encoded.method,
                      encoded.path,
                      request.client_context_id,
                      request.timeout.count());
        SPDLOG_TRACE(R"({} HTTP request: {}, method={}, path="{}", client_context_id="{}", timeout={}ms{:a})",
                     log_prefix,
                     encoded.type,
                     encoded.method,
                     encoded.path,
                     request.client_context_id,
                     request.timeout.count(),
                     spdlog::to_hex(encoded.body));
        session->write_and_subscribe(encoded,
                                     [self = this->shared_from_this(), log_prefix, session, handler = std::forward<Handler>(handler)](
                                       std::error_code ec, io::http_response&& msg) mutable {
                                         self->deadline.cancel();
                                         encoded_response_type resp(msg);
                                         spdlog::trace(R"({} HTTP response: {}, client_context_id="{}", status={})",
                                                       log_prefix,
                                                       self->request.type,
                                                       self->request.client_context_id,
                                                       resp.status_code);
                                         SPDLOG_TRACE(R"({} HTTP response: {}, client_context_id="{}", status={}{:a})",
                                                      log_prefix,
                                                      self->request.type,
                                                      self->request.client_context_id,
                                                      resp.status_code,
                                                      spdlog::to_hex(resp.body));
                                         try {
                                             handler(make_response(ec, self->request, resp));
                                         } catch (priv::retry_http_request) {
                                             self->send_to(session, std::forward<Handler>(handler));
                                         }
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
