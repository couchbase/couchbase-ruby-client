/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

/*
 * Function to base64 encode and decode text as described in RFC 4648
 *
 * @author Trond Norbye
 */

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#include <platform/base64.h>

/**
 * An array of the legal characters used for direct lookup
 */
static const std::uint8_t codemap[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/**
 * A method to map the code back to the value
 *
 * @param code the code to map
 * @return the byte value for the code character
 */
static std::uint32_t
code2val(const std::uint8_t code)
{
    if (code >= 'A' && code <= 'Z') {
        return std::uint32_t(code) - 'A';
    }
    if (code >= 'a' && code <= 'z') {
        return std::uint32_t(code) - 'a' + std::uint8_t(26);
    }
    if (code >= '0' && code <= '9') {
        return std::uint32_t(code) - '0' + std::uint8_t(52);
    }
    if (code == '+') {
        return std::uint32_t(62);
    }
    if (code == '/') {
        return std::uint32_t(63);
    }
    throw std::invalid_argument("couchbase::base64::code2val Invalid input character");
}

/**
 * Encode up to 3 characters to 4 output character.
 *
 * @param s pointer to the input stream
 * @param d pointer to the output stream
 * @param num the number of characters from s to encode
 */
static void
encode_rest(const std::uint8_t* s, std::string& result, size_t num)
{
    uint32_t val = 0;

    switch (num) {
        case 2:
            val = static_cast<uint32_t>((*s << 16U) | (*(s + 1) << 8U));
            break;
        case 1:
            val = static_cast<uint32_t>((*s << 16U));
            break;
        default:
            throw std::invalid_argument("base64::encode_rest num may be 1 or 2");
    }

    result.push_back(static_cast<char>(codemap[(val >> 18U) & 63]));
    result.push_back(static_cast<char>(codemap[(val >> 12U) & 63]));
    if (num == 2) {
        result.push_back(static_cast<char>(codemap[(val >> 6U) & 63]));
    } else {
        result.push_back('=');
    }
    result.push_back('=');
}

/**
 * Encode 3 bytes to 4 output character.
 *
 * @param s pointer to the input stream
 * @param d pointer to the output stream
 */
static void
encode_triplet(const std::uint8_t* s, std::string& str)
{
    auto val = static_cast<uint32_t>((*s << 16U) | (*(s + 1) << 8U) | (*(s + 2)));
    str.push_back(static_cast<char>(codemap[(val >> 18U) & 63]));
    str.push_back(static_cast<char>(codemap[(val >> 12U) & 63]));
    str.push_back(static_cast<char>(codemap[(val >> 6U) & 63]));
    str.push_back(static_cast<char>(codemap[val & 63]));
}

/**
 * decode 4 input characters to up to two output bytes
 *
 * @param s source string
 * @param d destination
 * @return the number of characters inserted
 */
static int
decode_quad(const std::uint8_t* s, std::string& d)
{
    uint32_t value = code2val(s[0]) << 18U;
    value |= code2val(s[1]) << 12U;

    int ret = 3;

    if (s[2] == '=') {
        ret = 1;
    } else {
        value |= code2val(s[2]) << 6U;
        if (s[3] == '=') {
            ret = 2;
        } else {
            value |= code2val(s[3]);
        }
    }

    d.push_back(static_cast<char>(value >> 16U));
    if (ret > 1) {
        d.push_back(static_cast<char>(value >> 8U));
        if (ret > 2) {
            d.push_back(static_cast<char>(value));
        }
    }

    return ret;
}

namespace couchbase::base64
{
std::string
encode(const std::string_view blob, bool prettyprint)
{
    // base64 encoding encodes up to 3 input characters to 4 output
    // characters in the alphabet above.
    auto triplets = blob.size() / 3;
    auto rest = blob.size() % 3;
    auto chunks = triplets;
    if (rest != 0) {
        ++chunks;
    }

    std::string result;
    if (prettyprint) {
        // In pretty-print mode we insert a newline after adding
        // 16 chunks (four characters).
        result.reserve(chunks * 4 + chunks / 16);
    } else {
        result.reserve(chunks * 4);
    }

    const auto* in = reinterpret_cast<const std::uint8_t*>(blob.data());

    chunks = 0;
    for (size_t ii = 0; ii < triplets; ++ii) {
        encode_triplet(in, result);
        in += 3;

        if (prettyprint && (++chunks % 16) == 0) {
            result.push_back('\n');
        }
    }

    if (rest > 0) {
        encode_rest(in, result, rest);
    }

    if (prettyprint && result.back() != '\n') {
        result.push_back('\n');
    }

    return result;
}

std::string
decode(std::string_view blob)
{
    std::string destination;

    if (blob.empty()) {
        return destination;
    }

    // To reduce the number of reallocations, start by reserving an
    // output buffer of 75% of the input size (and add 3 to avoid dealing
    // with zero)
    size_t estimate = blob.size() / 100 * 75;
    destination.reserve(estimate + 3);

    const auto* in = reinterpret_cast<const std::uint8_t*>(blob.data());
    size_t offset = 0;
    while (offset < blob.size()) {
        if (std::isspace(static_cast<int>(*in)) != 0) {
            ++offset;
            ++in;
            continue;
        }

        // We need at least 4 bytes
        if ((offset + 4) > blob.size()) {
            throw std::invalid_argument("couchbase::base64::decode invalid input");
        }

        decode_quad(in, destination);
        in += 4;
        offset += 4;
    }

    return destination;
}

} // namespace couchbase::base64
