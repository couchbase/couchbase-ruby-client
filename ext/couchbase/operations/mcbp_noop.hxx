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
#include <protocol/cmd_noop.hxx>
#include <io/retry_context.hxx>

namespace couchbase::operations
{

struct mcbp_noop_response {
    error_context::key_value ctx;
};

struct mcbp_noop_request {
    using encoded_request_type = protocol::client_request<protocol::mcbp_noop_request_body>;
    using encoded_response_type = protocol::client_response<protocol::mcbp_noop_response_body>;

    uint16_t partition{};
    uint32_t opaque{};
    std::chrono::milliseconds timeout{ timeout_defaults::key_value_timeout };
    io::retry_context<io::retry_strategy::best_effort> retries{ true };

    [[nodiscard]] std::error_code encode_to(encoded_request_type& encoded, mcbp_context&&)
    {
        encoded.opaque(opaque);
        encoded.partition(partition);
        return {};
    }
};

mcbp_noop_response
make_response(error_context::key_value&& ctx, mcbp_noop_request& /* request */, mcbp_noop_request::encoded_response_type&&)
{
    mcbp_noop_response response{ ctx };
    return response;
}

} // namespace couchbase::operations
