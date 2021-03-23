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

#include <tao/json.hpp>

#include <document_id.hxx>
#include <operations/bucket_settings.hxx>
#include <protocol/cmd_get_collections_manifest.hxx>
#include <collections_manifest.hxx>
#include <io/retry_context.hxx>
#include <protocol/client_request.hxx>
#include <timeout_defaults.hxx>

namespace couchbase::operations
{

struct collections_manifest_get_response {
    error_context::key_value ctx;
    collections_manifest manifest{};
};

struct collections_manifest_get_request {
    using encoded_request_type = protocol::client_request<protocol::get_collections_manifest_request_body>;
    using encoded_response_type = protocol::client_response<protocol::get_collections_manifest_response_body>;

    document_id id{ "", "", "", std::nullopt, false, true };
    uint16_t partition{};
    uint32_t opaque{};
    std::chrono::milliseconds timeout{ timeout_defaults::key_value_timeout };
    io::retry_context<io::retry_strategy::best_effort> retries{ true };

    [[nodiscard]] std::error_code encode_to(encoded_request_type& encoded, mcbp_context&&)
    {
        encoded.opaque(opaque);
        return {};
    }
};

collections_manifest_get_response
make_response(error_context::key_value&& ctx,
              collections_manifest_get_request&,
              collections_manifest_get_request::encoded_response_type&& encoded)
{
    collections_manifest_get_response response{ ctx };
    if (!response.ctx.ec) {
        response.manifest = encoded.body().manifest();
    }
    return response;
}
} // namespace couchbase::operations
