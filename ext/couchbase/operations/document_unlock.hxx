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
#include <protocol/cmd_unlock.hxx>

namespace couchbase::operations
{

struct unlock_response {
    document_id id;
    std::error_code ec{};
    std::uint64_t cas{};
    mutation_token token{};
};

struct unlock_request {
    using encoded_request_type = protocol::client_request<protocol::unlock_request_body>;
    using encoded_response_type = protocol::client_response<protocol::unlock_response_body>;

    document_id id;
    uint16_t partition{};
    uint32_t opaque{};
    uint64_t cas{};

    void encode_to(encoded_request_type& encoded)
    {
        encoded.opaque(opaque);
        encoded.partition(partition);
        encoded.body().id(id);
        encoded.cas(cas);
    }
};

unlock_response
make_response(std::error_code ec, unlock_request& request, unlock_request::encoded_response_type encoded)
{
    unlock_response response{ request.id, ec };
    if (!ec) {
        response.cas = encoded.cas();
    }
    return response;
}

} // namespace couchbase::operations
