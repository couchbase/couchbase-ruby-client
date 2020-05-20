/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include <platform/string_hex.h>

#include <cinttypes>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string>

static inline uint8_t
from_hex_digit(char c)
{
    if ('0' <= c && c <= '9') {
        return static_cast<uint8_t>(c - '0');
    } else if ('A' <= c && c <= 'F') {
        return static_cast<uint8_t>(c + 10 - 'A');
    } else if ('a' <= c && c <= 'f') {
        return static_cast<uint8_t>(c + 10 - 'a');
    }
    throw std::invalid_argument("couchbase::from_hex_digit: character was not in hexadecimal range");
}

uint64_t
couchbase::from_hex(std::string_view buffer)
{
    uint64_t ret = 0;
    if (buffer.size() > 16) {
        throw std::overflow_error("couchbase::from_hex: input string too long: " + std::to_string(buffer.size()));
    }

    for (char digit : buffer) {
        ret = (ret << 4) | from_hex_digit(digit);
    }

    return ret;
}

std::string
couchbase::to_hex(uint8_t val)
{
    char buf[32];
    snprintf(buf, sizeof(buf), "0x%02" PRIx8, val);
    return std::string{ buf };
}

std::string
couchbase::to_hex(uint16_t val)
{
    char buf[32];
    snprintf(buf, sizeof(buf), "0x%04" PRIx16, val);
    return std::string{ buf };
}

std::string
couchbase::to_hex(uint32_t val)
{
    char buf[32];
    snprintf(buf, sizeof(buf), "0x%08" PRIx32, val);
    return std::string{ buf };
}

std::string
couchbase::to_hex(uint64_t val)
{
    char buf[32];
    snprintf(buf, sizeof(buf), "0x%016" PRIx64, val);
    return std::string{ buf };
}

std::string
couchbase::to_hex(std::string_view buffer)
{
    if (buffer.empty()) {
        return "";
    }
    std::stringstream ss;
    for (const auto& c : buffer) {
        ss << "0x" << std::hex << std::setfill('0') << std::setw(2) << uint32_t(c) << " ";
    }
    auto ret = ss.str();
    ret.resize(ret.size() - 1);
    return ret;
}
