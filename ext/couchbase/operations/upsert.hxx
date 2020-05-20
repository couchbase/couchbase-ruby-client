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

#include <operations/document_id.hxx>
#include <protocol/cmd_upsert.hxx>

namespace couchbase::operations
{

struct upsert_response {
    document_id id;
    std::error_code ec{};
    std::uint64_t cas{};
};

struct upsert_request {
    using encoded_request_type = protocol::client_request<protocol::upsert_request_body>;
    using encoded_response_type = protocol::client_response<protocol::upsert_response_body>;

    document_id id;
    std::string value;
    uint16_t partition{};
    uint32_t opaque{};

    void encode_to(encoded_request_type& encoded)
    {
        encoded.opaque(opaque);
        encoded.partition(partition);
        encoded.body().id(id);
        encoded.body().content(value);
    }
};

upsert_response
make_response(std::error_code ec, upsert_request& request, upsert_request::encoded_response_type encoded)
{
    upsert_response response{ request.id, ec };
    if (!ec) {
        response.cas = encoded.cas();
    }
    return response;
}

} // namespace couchbase::operations