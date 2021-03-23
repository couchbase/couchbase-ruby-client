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

#include <document_id.hxx>
#include <protocol/cmd_touch.hxx>
#include <io/retry_context.hxx>

namespace couchbase::operations
{

struct touch_response {
    error_context::key_value ctx;
    std::uint64_t cas{};
};

struct touch_request {
    using encoded_request_type = protocol::client_request<protocol::touch_request_body>;
    using encoded_response_type = protocol::client_response<protocol::touch_response_body>;

    document_id id;
    std::uint16_t partition{};
    std::uint32_t opaque{};
    std::uint32_t expiry{};
    std::chrono::milliseconds timeout{ timeout_defaults::key_value_timeout };
    io::retry_context<io::retry_strategy::best_effort> retries{ false };

    [[nodiscard]] std::error_code encode_to(encoded_request_type& encoded, mcbp_context&&)
    {
        encoded.opaque(opaque);
        encoded.partition(partition);
        encoded.body().id(id);
        encoded.body().expiry(expiry);
        return {};
    }
};

touch_response
make_response(error_context::key_value&& ctx, touch_request&, touch_request::encoded_response_type&& encoded)
{
    touch_response response{ ctx };
    if (!response.ctx.ec) {
        response.cas = encoded.cas();
    }
    return response;
}

} // namespace couchbase::operations
