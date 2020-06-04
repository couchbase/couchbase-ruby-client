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

namespace couchbase::protocol
{
static const size_t header_size = 24;
using header_buffer = std::array<std::uint8_t, header_size>;

enum class client_opcode : uint8_t {
    get = 0x00,
    upsert = 0x01,
    insert = 0x02,
    replace = 0x03,
    remove = 0x04,
    increment = 0x05,
    decrement = 0x06,
    touch = 0x1c,
    get_and_touch = 0x1d,
    hello = 0x1f,
    sasl_list_mechs = 0x20,
    sasl_auth = 0x21,
    sasl_step = 0x22,
    select_bucket = 0x89,
    observe = 0x92,
    get_and_lock = 0x94,
    unlock = 0x95,
    get_collections_manifest = 0xba,
    get_collection_id = 0xbb,
    subdoc_multi_lookup = 0xd0,
    subdoc_multi_mutation = 0xd1,
    get_cluster_config = 0xb5,
    get_error_map = 0xfe,
    invalid = 0xff,
};

/**
 * subdocument opcodes are listed separately, because we are not going to implement/support single-op messages
 */
enum class subdoc_opcode : uint8_t {
    get_doc = 0x00,
    get = 0xc5,
    exists = 0xc6,
    dict_add = 0xc7,
    dict_upsert = 0xc8,
    remove = 0xc9,
    replace = 0xca,
    array_push_last = 0xcb,
    array_push_first = 0xcc,
    array_insert = 0xcd,
    array_add_unique = 0xce,
    counter = 0xcf,
    get_count = 0xd2,
};

constexpr inline bool
is_valid_client_opcode(uint8_t code)
{
    switch (static_cast<client_opcode>(code)) {
        case client_opcode::get:
        case client_opcode::upsert:
        case client_opcode::insert:
        case client_opcode::replace:
        case client_opcode::remove:
        case client_opcode::hello:
        case client_opcode::sasl_list_mechs:
        case client_opcode::sasl_auth:
        case client_opcode::sasl_step:
        case client_opcode::select_bucket:
        case client_opcode::subdoc_multi_lookup:
        case client_opcode::subdoc_multi_mutation:
        case client_opcode::get_cluster_config:
        case client_opcode::get_error_map:
        case client_opcode::invalid:
        case client_opcode::get_collections_manifest:
        case client_opcode::touch:
        case client_opcode::observe:
        case client_opcode::get_and_lock:
        case client_opcode::unlock:
        case client_opcode::get_and_touch:
        case client_opcode::increment:
        case client_opcode::decrement:
        case client_opcode::get_collection_id:
            return true;
    }
    return false;
}

constexpr inline bool
is_valid_subdoc_opcode(uint8_t code)
{
    switch (static_cast<subdoc_opcode>(code)) {
        case subdoc_opcode::get:
        case subdoc_opcode::exists:
        case subdoc_opcode::dict_add:
        case subdoc_opcode::dict_upsert:
        case subdoc_opcode::remove:
        case subdoc_opcode::replace:
        case subdoc_opcode::array_push_last:
        case subdoc_opcode::array_push_first:
        case subdoc_opcode::array_insert:
        case subdoc_opcode::array_add_unique:
        case subdoc_opcode::counter:
        case subdoc_opcode::get_count:
        case subdoc_opcode::get_doc:
            return true;
    }
    return false;
}
} // namespace couchbase::protocol

template<>
struct fmt::formatter<couchbase::protocol::client_opcode> : formatter<string_view> {
    template<typename FormatContext>
    auto format(couchbase::protocol::client_opcode opcode, FormatContext& ctx)
    {
        string_view name = "unknown";
        switch (opcode) {
            case couchbase::protocol::client_opcode::get:
                name = "get";
                break;
            case couchbase::protocol::client_opcode::upsert:
                name = "upsert";
                break;
            case couchbase::protocol::client_opcode::insert:
                name = "insert";
                break;
            case couchbase::protocol::client_opcode::replace:
                name = "replace";
                break;
            case couchbase::protocol::client_opcode::remove:
                name = "remove";
                break;
            case couchbase::protocol::client_opcode::hello:
                name = "hello";
                break;
            case couchbase::protocol::client_opcode::sasl_list_mechs:
                name = "sasl_list_mechs";
                break;
            case couchbase::protocol::client_opcode::sasl_auth:
                name = "sasl_auth";
                break;
            case couchbase::protocol::client_opcode::sasl_step:
                name = "sasl_step";
                break;
            case couchbase::protocol::client_opcode::select_bucket:
                name = "select_bucket";
                break;
            case couchbase::protocol::client_opcode::subdoc_multi_lookup:
                name = "subdoc_multi_lookup";
                break;
            case couchbase::protocol::client_opcode::subdoc_multi_mutation:
                name = "subdoc_multi_mutation";
                break;
            case couchbase::protocol::client_opcode::get_cluster_config:
                name = "get_cluster_config";
                break;
            case couchbase::protocol::client_opcode::get_error_map:
                name = "get_error_map";
                break;
            case couchbase::protocol::client_opcode::invalid:
                name = "invalid";
                break;
            case couchbase::protocol::client_opcode::get_collections_manifest:
                name = "get_collections_manifest";
                break;
            case couchbase::protocol::client_opcode::touch:
                name = "touch";
                break;
            case couchbase::protocol::client_opcode::observe:
                name = "observe";
                break;
            case couchbase::protocol::client_opcode::get_and_lock:
                name = "get_and_lock";
                break;
            case couchbase::protocol::client_opcode::unlock:
                name = "unlock";
                break;
            case couchbase::protocol::client_opcode::get_and_touch:
                name = "get_and_touch";
                break;
            case couchbase::protocol::client_opcode::increment:
                name = "increment";
                break;
            case couchbase::protocol::client_opcode::decrement:
                name = "decrement";
                break;
            case couchbase::protocol::client_opcode::get_collection_id:
                name = "get_collection_uid";
                break;
        }
        return formatter<string_view>::format(name, ctx);
    }
};

template<>
struct fmt::formatter<couchbase::protocol::subdoc_opcode> : formatter<string_view> {
    template<typename FormatContext>
    auto format(couchbase::protocol::subdoc_opcode opcode, FormatContext& ctx)
    {
        string_view name = "unknown";
        switch (opcode) {
            case couchbase::protocol::subdoc_opcode::get:
                name = "get";
                break;
            case couchbase::protocol::subdoc_opcode::exists:
                name = "exists";
                break;
            case couchbase::protocol::subdoc_opcode::dict_add:
                name = "dict_add";
                break;
            case couchbase::protocol::subdoc_opcode::dict_upsert:
                name = "dict_upsert";
                break;
            case couchbase::protocol::subdoc_opcode::remove:
                name = "remove";
                break;
            case couchbase::protocol::subdoc_opcode::replace:
                name = "replace";
                break;
            case couchbase::protocol::subdoc_opcode::array_push_last:
                name = "array_push_last";
                break;
            case couchbase::protocol::subdoc_opcode::array_push_first:
                name = "array_push_first";
                break;
            case couchbase::protocol::subdoc_opcode::array_insert:
                name = "array_insert";
                break;
            case couchbase::protocol::subdoc_opcode::array_add_unique:
                name = "array_add_unique";
                break;
            case couchbase::protocol::subdoc_opcode::counter:
                name = "counter";
                break;
            case couchbase::protocol::subdoc_opcode::get_count:
                name = "get_count";
                break;
            case couchbase::protocol::subdoc_opcode::get_doc:
                name = "get_doc";
                break;
        }
        return formatter<string_view>::format(name, ctx);
    }
};
