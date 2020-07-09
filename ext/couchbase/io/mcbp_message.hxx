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

#include <cstdint>
#include <vector>
#include <array>

namespace couchbase
{
namespace protocol
{
static const size_t header_size = 24;
using header_buffer = std::array<std::uint8_t, header_size>;
} // namespace protocol

namespace io
{
struct binary_header {
    std::uint8_t magic;
    std::uint8_t opcode;
    std::uint16_t keylen;
    std::uint8_t extlen;
    std::uint8_t datatype;
    std::uint16_t specific;
    std::uint32_t bodylen;
    std::uint32_t opaque;
    std::uint64_t cas;

    std::uint16_t status()
    {
        return htons(specific);
    }
};

struct mcbp_message {
    binary_header header;
    std::vector<std::uint8_t> body;

    protocol::header_buffer header_data()
    {
        protocol::header_buffer buf;
        std::memcpy(buf.data(), &header, sizeof(header));
        return buf;
    }
};
} // namespace io
} // namespace couchbase
