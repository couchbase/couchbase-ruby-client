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

#include <spdlog/spdlog.h>

namespace couchbase::protocol
{
enum class magic : uint8_t {
    /// Request packet from client to server
    client_request = 0x80,
    /// The alternative request packet containing frame extras
    alt_client_request = 0x08,
    /// Response packet from server to client
    client_response = 0x81,
    /// The alternative response packet containing frame extras
    alt_client_response = 0x18,
    /// Request packet from server to client
    server_request = 0x82,
    /// Response packet from client to server
    server_response = 0x83
};

constexpr inline bool
is_valid_magic(uint8_t code)
{
    switch (static_cast<magic>(code)) {
        case magic::client_request:
        case magic::alt_client_request:
        case magic::client_response:
        case magic::alt_client_response:
        case magic::server_request:
        case magic::server_response:
            return true;
    }
    return false;
}
} // namespace couchbase::protocol

template<>
struct fmt::formatter<couchbase::protocol::magic> : formatter<string_view> {
    template<typename FormatContext>
    auto format(couchbase::protocol::magic code, FormatContext& ctx)
    {
        string_view name = "unknown";
        switch (code) {
            case couchbase::protocol::magic::client_request:
                name = "client_request";
                break;
            case couchbase::protocol::magic::alt_client_request:
                name = "alt_client_request";
                break;
            case couchbase::protocol::magic::client_response:
                name = "client_response";
                break;
            case couchbase::protocol::magic::alt_client_response:
                name = "alt_client_response";
                break;
            case couchbase::protocol::magic::server_request:
                name = "server_request";
                break;
            case couchbase::protocol::magic::server_response:
                name = "server_response";
                break;
        }
        return formatter<string_view>::format(name, ctx);
    }
};
