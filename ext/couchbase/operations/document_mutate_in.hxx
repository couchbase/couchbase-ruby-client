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

#include <gsl/gsl_assert>
#include <algorithm>

#include <protocol/cmd_mutate_in.hxx>
#include <protocol/status.hxx>
#include <protocol/durability_level.hxx>
#include <protocol/client_request.hxx>

#include <mutation_token.hxx>
#include <document_id.hxx>
#include <timeout_defaults.hxx>
#include <errors.hxx>

namespace couchbase::operations
{

struct mutate_in_response {
    struct field {
        protocol::subdoc_opcode opcode;
        protocol::status status;
        std::string path;
        std::string value;
        std::size_t original_index;
    };
    document_id id;
    std::uint32_t opaque;
    std::error_code ec{};
    std::uint64_t cas{};
    mutation_token token{};
    std::vector<field> fields{};
    std::optional<std::size_t> first_error_index{};
};

struct mutate_in_request {
    using encoded_request_type = protocol::client_request<protocol::mutate_in_request_body>;
    using encoded_response_type = protocol::client_response<protocol::mutate_in_response_body>;

    document_id id;
    uint16_t partition{};
    uint32_t opaque{};
    uint64_t cas{ 0 };
    bool access_deleted{ false };
    std::optional<std::uint32_t> expiration{};
    protocol::mutate_in_request_body::store_semantics_type store_semantics{
        protocol::mutate_in_request_body::store_semantics_type::replace
    };
    protocol::mutate_in_request_body::mutate_in_specs specs{};
    protocol::durability_level durability_level{ protocol::durability_level::none };
    std::optional<std::uint16_t> durability_timeout{};
    std::chrono::milliseconds timeout{ timeout_defaults::key_value_timeout };

    void encode_to(encoded_request_type& encoded)
    {
        for (std::size_t i = 0; i < specs.entries.size(); ++i) {
            auto& entry = specs.entries[i];
            entry.original_index = i;
        }
        std::stable_sort(specs.entries.begin(),
                         specs.entries.end(),
                         [](const protocol::mutate_in_request_body::mutate_in_specs::entry& lhs,
                            const protocol::mutate_in_request_body::mutate_in_specs::entry& rhs) -> bool {
                             return (lhs.flags & protocol::mutate_in_request_body::mutate_in_specs::path_flag_xattr) >
                                    (rhs.flags & protocol::mutate_in_request_body::mutate_in_specs::path_flag_xattr);
                         });

        encoded.opaque(opaque);
        encoded.partition(partition);
        encoded.body().id(id);
        encoded.cas(cas);
        if (expiration) {
            encoded.body().expiration(*expiration);
        }
        encoded.body().access_deleted(access_deleted);
        encoded.body().store_semantics(store_semantics);
        encoded.body().specs(specs);
        if (durability_level != protocol::durability_level::none) {
            encoded.body().durability(durability_level, durability_timeout);
        }
    }
};

mutate_in_response
make_response(std::error_code ec, mutate_in_request& request, mutate_in_request::encoded_response_type encoded)
{
    mutate_in_response response{ request.id, encoded.opaque(), ec };
    if (!ec) {
        response.cas = encoded.cas();
        response.token = encoded.body().token();
        response.token.partition_id = request.partition;
        response.token.bucket_name = response.id.bucket;
        response.fields.resize(request.specs.entries.size());
        for (size_t i = 0; i < request.specs.entries.size(); ++i) {
            auto& req_entry = request.specs.entries[i];
            response.fields[i].original_index = req_entry.original_index;
            response.fields[i].opcode = static_cast<protocol::subdoc_opcode>(req_entry.opcode);
            response.fields[i].path = req_entry.path;
            response.fields[i].status = protocol::status::success;
        }
        for (auto& entry : encoded.body().fields()) {
            if (entry.status == protocol::status::success) {
                response.fields[entry.index].value = entry.value;
            } else {
                response.fields[entry.index].status = entry.status;
                response.first_error_index = entry.index;
                break;
            }
        }
        std::sort(response.fields.begin(),
                  response.fields.end(),
                  [](const mutate_in_response::field& lhs, const mutate_in_response::field& rhs) -> bool {
                      return lhs.original_index < rhs.original_index;
                  });
    } else if (request.store_semantics == protocol::mutate_in_request_body::store_semantics_type::insert &&
               ec == std::make_error_code(error::common_errc::cas_mismatch)) {
        response.ec = std::make_error_code(error::key_value_errc::document_exists);
    }
    return response;
}

} // namespace couchbase::operations
