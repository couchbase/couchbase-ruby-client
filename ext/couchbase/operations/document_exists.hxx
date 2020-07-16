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
#include <protocol/cmd_exists.hxx>
#include <io/retry_context.hxx>

namespace couchbase::operations
{

struct exists_response {
    enum class observe_status { invalid, found, not_found, persisted, logically_deleted };

    document_id id;
    std::uint32_t opaque;
    std::error_code ec{};
    std::uint16_t partition_id{};
    std::uint64_t cas{};
    observe_status status{ observe_status::invalid };
};

struct exists_request {
    using encoded_request_type = protocol::client_request<protocol::exists_request_body>;
    using encoded_response_type = protocol::client_response<protocol::exists_response_body>;

    document_id id;
    std::uint16_t partition{};
    std::uint32_t opaque{};
    std::chrono::milliseconds timeout{ timeout_defaults::key_value_timeout };
    io::retry_context<io::retry_strategy::best_effort> retries{ false };

    void encode_to(encoded_request_type& encoded)
    {
        encoded.opaque(opaque);
        encoded.body().id(partition, id);
    }
};

exists_response
make_response(std::error_code ec, exists_request& request, exists_request::encoded_response_type encoded)
{
    exists_response response{ request.id, encoded.opaque(), ec, request.partition };
    if (!ec) {
        response.cas = encoded.body().cas();
        response.partition_id = encoded.body().partition_id();
        switch (encoded.body().status()) {
            case 0x00:
                response.status = exists_response::observe_status::found;
                break;
            case 0x01:
                response.status = exists_response::observe_status::persisted;
                break;
            case 0x80:
                response.status = exists_response::observe_status::not_found;
                break;
            case 0x81:
                response.status = exists_response::observe_status::logically_deleted;
                break;
            default:
                spdlog::warn("invalid observe status for \"{}\": {:x}", request.id, encoded.body().status());
                response.status = exists_response::observe_status::invalid;
                break;
        }
    }
    return response;
}

} // namespace couchbase::operations
