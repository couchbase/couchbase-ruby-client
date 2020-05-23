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
#include <protocol/magic.hxx>

#include <spdlog/fmt/bin_to_hex.h>

namespace couchbase::io
{
struct binary_parser {
    enum result { ok, need_data, failure };

    template<typename Iterator>
    void feed(Iterator begin, Iterator end)
    {
        buf.reserve(buf.size() + static_cast<size_t>(std::distance(begin, end)));
        std::copy(begin, end, std::back_inserter(buf));
    }

    void reset()
    {
        buf.clear();
    }

    result next(binary_message& msg)
    {
        static const size_t header_size = 24;
        if (buf.size() < header_size) {
            return need_data;
        }
        std::memcpy(&msg.header, buf.data(), header_size);
        uint32_t body_size = ntohl(msg.header.bodylen);
        if (body_size > 0 && buf.size() - header_size < body_size) {
            return need_data;
        }
        msg.body.clear();
        msg.body.reserve(body_size);
        std::copy(buf.begin() + header_size, buf.begin() + header_size + body_size, std::back_inserter(msg.body));
        buf.erase(buf.begin(), buf.begin() + header_size + body_size);
        if (!protocol::is_valid_magic(buf[0])) {
            spdlog::warn("parsed frame for magic={:x}, opcode={:x}, opaque={}, body_len={}. Invalid magic of the next frame: {:x}, {} "
                         "bytes to parse{}",
                         msg.header.magic,
                         msg.header.opcode,
                         msg.header.opaque,
                         body_size,
                         buf[0],
                         buf.size(),
                         spdlog::to_hex(buf));
            reset();
        }
        return ok;
    }

    std::vector<std::uint8_t> buf;
};
} // namespace couchbase::io
