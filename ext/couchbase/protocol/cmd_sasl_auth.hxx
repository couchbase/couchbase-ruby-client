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

#include <algorithm>

#include <protocol/hello_feature.hxx>

namespace couchbase::protocol
{
class sasl_auth_response_body
{
  public:
    static const inline client_opcode opcode = client_opcode::sasl_auth;

  private:
    std::string value_;

  public:
    bool parse(protocol::status status,
               const header_buffer& header,
               std::uint8_t framing_extras_size,
               std::uint16_t key_size,
               std::uint8_t extras_size,
               const std::vector<uint8_t>& body,
               const cmd_info&)
    {
        Expects(header[1] == static_cast<uint8_t>(opcode));
        if (status == protocol::status::success || status == protocol::status::auth_continue) {
            value_.assign(body.begin() + framing_extras_size + extras_size + key_size, body.end());
            return true;
        }
        return false;
    }

    [[nodiscard]] std::string_view value() const
    {
        return value_;
    }
};

class sasl_auth_request_body
{
  public:
    using response_body_type = sasl_auth_response_body;
    static const inline client_opcode opcode = client_opcode::sasl_auth;

  private:
    std::string key_;
    std::vector<std::uint8_t> value_;

  public:
    void mechanism(std::string_view mech)
    {
        key_ = mech;
    }

    void sasl_data(std::string_view data)
    {
        value_.assign(data.begin(), data.end());
    }

    const std::string& key()
    {
        return key_;
    }

    const std::vector<std::uint8_t>& framing_extras()
    {
        static std::vector<std::uint8_t> empty;
        return empty;
    }

    const std::vector<std::uint8_t>& extras()
    {
        static std::vector<std::uint8_t> empty;
        return empty;
    }

    const std::vector<std::uint8_t>& value()
    {
        return value_;
    }

    std::size_t size()
    {
        return key_.size() + value_.size();
    }
};

} // namespace couchbase::protocol
