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

namespace couchbase::protocol
{
enum class server_opcode : uint8_t {
    cluster_map_change_notification = 0x01,
    invalid = 0xff,
};

constexpr bool
is_valid_server_request_opcode(uint8_t code)
{
    switch (server_opcode(code)) {
        case server_opcode::cluster_map_change_notification:
            return true;
        case server_opcode::invalid:
            break;
    }
    return false;
}

} // namespace couchbase::protocol

template<>
struct fmt::formatter<couchbase::protocol::server_opcode> : formatter<string_view> {
    template<typename FormatContext>
    auto format(couchbase::protocol::server_opcode opcode, FormatContext& ctx)
    {
        string_view name = "unknown";
        switch (opcode) {
            case couchbase::protocol::server_opcode::cluster_map_change_notification:
                name = "cluster_map_change_notification (0x01)";
                break;
            case couchbase::protocol::server_opcode::invalid:
                name = "invalid (0xff)";
                break;
        }
        return formatter<string_view>::format(name, ctx);
    }
};
