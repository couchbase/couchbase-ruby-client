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

#include <document_id.hxx>
#include <protocol/cmd_get_and_lock.hxx>
#include <io/retry_context.hxx>

namespace couchbase::operations
{

struct get_and_lock_response {
    document_id id;
    std::uint32_t opaque;
    std::error_code ec{};
    std::string value{};
    std::uint64_t cas{};
    std::uint32_t flags{};
};

struct get_and_lock_request {
    using encoded_request_type = protocol::client_request<protocol::get_and_lock_request_body>;
    using encoded_response_type = protocol::client_response<protocol::get_and_lock_response_body>;

    document_id id;
    uint16_t partition{};
    uint32_t opaque{};
    uint32_t lock_time{};
    std::chrono::milliseconds timeout{ timeout_defaults::key_value_timeout };
    io::retry_context<io::retry_strategy::best_effort> retries{ false };

    [[nodiscard]] std::error_code encode_to(encoded_request_type& encoded, mcbp_context&&)
    {
        encoded.opaque(opaque);
        encoded.partition(partition);
        encoded.body().id(id);
        encoded.body().lock_time(lock_time);
        return {};
    }
};

get_and_lock_response
make_response(std::error_code ec, get_and_lock_request& request, get_and_lock_request::encoded_response_type&& encoded)
{
    get_and_lock_response response{ request.id, encoded.opaque(), ec };
    if (ec && response.opaque == 0) {
        response.opaque = request.opaque;
    }
    if (!ec) {
        response.value = std::move(encoded.body().value());
        response.cas = encoded.cas();
        response.flags = encoded.body().flags();
    }
    return response;
}

} // namespace couchbase::operations
