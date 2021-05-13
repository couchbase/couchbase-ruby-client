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

#include <gsl/gsl_assert>
#include <document_id.hxx>
#include <protocol/cmd_lookup_in.hxx>
#include <io/retry_context.hxx>

namespace couchbase::operations
{

struct lookup_in_response {
    struct field {
        protocol::subdoc_opcode opcode;
        bool exists;
        protocol::status status;
        std::string path;
        std::string value;
        std::size_t original_index;
    };
    error_context::key_value ctx;
    std::uint64_t cas{};
    std::vector<field> fields{};
    bool deleted{ false };
};

struct lookup_in_request {
    using encoded_request_type = protocol::client_request<protocol::lookup_in_request_body>;
    using encoded_response_type = protocol::client_response<protocol::lookup_in_response_body>;

    document_id id;
    uint16_t partition{};
    uint32_t opaque{};
    bool access_deleted{ false };
    protocol::lookup_in_request_body::lookup_in_specs specs{};
    std::chrono::milliseconds timeout{ timeout_defaults::key_value_timeout };
    io::retry_context<io::retry_strategy::best_effort> retries{ false };

    [[nodiscard]] std::error_code encode_to(encoded_request_type& encoded, mcbp_context&&)
    {
        for (std::size_t i = 0; i < specs.entries.size(); ++i) {
            auto& entry = specs.entries[i];
            entry.original_index = i;
        }
        std::stable_sort(specs.entries.begin(),
                         specs.entries.end(),
                         [](const protocol::lookup_in_request_body::lookup_in_specs::entry& lhs,
                            const protocol::lookup_in_request_body::lookup_in_specs::entry& rhs) -> bool {
                             return (lhs.flags & protocol::lookup_in_request_body::lookup_in_specs::path_flag_xattr) >
                                    (rhs.flags & protocol::lookup_in_request_body::lookup_in_specs::path_flag_xattr);
                         });

        encoded.opaque(opaque);
        encoded.partition(partition);
        encoded.body().id(id);
        encoded.body().access_deleted(access_deleted);
        encoded.body().specs(specs);
        return {};
    }
};

lookup_in_response
make_response(error_context::key_value&& ctx, lookup_in_request& request, lookup_in_request::encoded_response_type&& encoded)
{
    lookup_in_response response{ ctx };
    if (encoded.status() == protocol::status::subdoc_success_deleted ||
        encoded.status() == protocol::status::subdoc_multi_path_failure_deleted) {
        response.deleted = true;
    }
    if (!ctx.ec) {
        response.cas = encoded.cas();
        response.fields.resize(request.specs.entries.size());
        for (size_t i = 0; i < request.specs.entries.size(); ++i) {
            auto& req_entry = request.specs.entries[i];
            response.fields[i].original_index = req_entry.original_index;
            response.fields[i].opcode = protocol::subdoc_opcode(req_entry.opcode);
            response.fields[i].path = req_entry.path;
            response.fields[i].status = protocol::status::success;
        }
        for (size_t i = 0; i < encoded.body().fields().size(); ++i) {
            auto& res_entry = encoded.body().fields()[i];
            response.fields[i].status = res_entry.status;
            response.fields[i].exists =
              res_entry.status == protocol::status::success || res_entry.status == protocol::status::subdoc_success_deleted;
            response.fields[i].value = res_entry.value;
        }
        std::sort(response.fields.begin(),
                  response.fields.end(),
                  [](const lookup_in_response::field& lhs, const lookup_in_response::field& rhs) -> bool {
                      return lhs.original_index < rhs.original_index;
                  });
    }
    return response;
}

} // namespace couchbase::operations
