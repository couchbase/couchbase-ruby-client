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

#include <io/key_value_session.hxx>
#include <io/http_session.hxx>

namespace couchbase::operations
{

template<typename Request>
struct command : public std::enable_shared_from_this<command<Request>> {
    using encoded_request_type = typename Request::encoded_request_type;
    using encoded_response_type = typename Request::encoded_response_type;
    asio::steady_timer deadline;
    Request request;
    encoded_request_type encoded;

    command(asio::io_context& ctx, Request&& req)
      : deadline(ctx)
      , request(req)
    {
    }

    template<typename Handler>
    void send_to(std::shared_ptr<io::key_value_session> session, Handler&& handler)
    {
        request.opaque = session->next_opaque();
        request.encode_to(encoded);
        session->write_and_subscribe(request.opaque,
                                     encoded.data(),
                                     [self = this->shared_from_this(),
                                      handler = std::forward<Handler>(handler)](std::error_code ec, io::binary_message&& msg) mutable {
                                         encoded_response_type resp(msg);
                                         self->deadline.cancel();
                                         handler(make_response(ec, self->request, resp));
                                     });
        deadline.expires_after(std::chrono::milliseconds(2500));
        deadline.async_wait(std::bind(&command<Request>::deadline_handler, this));
    }

    template<typename Handler>
    void send_to(std::shared_ptr<io::http_session> session, Handler&& handler)
    {
        request.encode_to(encoded);
        session->write_and_subscribe(
          encoded,
          [self = this->shared_from_this(), handler = std::forward<Handler>(handler)](std::error_code ec, io::http_response&& msg) mutable {
              encoded_response_type resp(msg);
              self->deadline.cancel();
              handler(make_response(ec, self->request, resp));
          });
        deadline.expires_after(std::chrono::milliseconds(2500));
        deadline.async_wait(std::bind(&command<Request>::deadline_handler, this));
    }

    void deadline_handler()
    {
    }
};

} // namespace couchbase::operations
