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
#include <protocol/cmd_decrement.hxx>

namespace couchbase::operations
{

struct decrement_response {
    document_id id;
    std::uint32_t opaque;
    std::error_code ec{};
    std::uint64_t content{};
    std::uint64_t cas{};
    mutation_token token{};
};

struct decrement_request {
    using encoded_request_type = protocol::client_request<protocol::decrement_request_body>;
    using encoded_response_type = protocol::client_response<protocol::decrement_response_body>;

    document_id id;
    std::uint16_t partition{};
    std::uint32_t opaque{};
    std::uint32_t expiry{ 0 };
    std::uint64_t delta{ 1 };
    std::optional<std::uint64_t> initial_value{};
    protocol::durability_level durability_level{ protocol::durability_level::none };
    std::optional<std::uint16_t> durability_timeout{};
    std::chrono::milliseconds timeout{ timeout_defaults::key_value_timeout };

    void encode_to(encoded_request_type& encoded)
    {
        encoded.opaque(opaque);
        encoded.partition(partition);
        encoded.body().id(id);
        encoded.body().delta(delta);
        if (initial_value) {
            encoded.body().initial_value(initial_value.value());
            encoded.body().expiry(expiry);
        } else {
            encoded.body().initial_value(0);
            encoded.body().expiry(0xffff'ffff);
        }
        if (durability_level != protocol::durability_level::none) {
            encoded.body().durability(durability_level, durability_timeout);
        }
    }
};

decrement_response
make_response(std::error_code ec, decrement_request& request, decrement_request::encoded_response_type encoded)
{
    decrement_response response{ request.id, encoded.opaque(), ec };
    if (ec && response.opaque == 0) {
        response.opaque = request.opaque;
    }
    if (!ec) {
        response.cas = encoded.cas();
        response.content = encoded.body().content();
        response.token = encoded.body().token();
        response.token.partition_id = request.partition;
        response.token.bucket_name = response.id.bucket;
    }
    return response;
}

} // namespace couchbase::operations
