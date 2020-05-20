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
#include <string>

namespace couchbase::protocol
{
class sasl_list_mechs_response_body
{
  public:
    static const inline client_opcode opcode = client_opcode::sasl_list_mechs;

  private:
    std::vector<std::string> supported_mechs_;

  public:
    [[nodiscard]] const std::vector<std::string>& supported_mechs() const
    {
        return supported_mechs_;
    }

    bool parse(protocol::status status, const header_buffer& header, const std::vector<uint8_t>& body, const cmd_info&)
    {
        Expects(header[1] == static_cast<uint8_t>(opcode));
        if (status == protocol::status::success) {
            auto previous = body.begin();
            auto current = std::find(body.begin(), body.end(), ' ');
            while (current != body.end()) {
                supported_mechs_.emplace_back(previous, current);
                previous = current + 1;
                current = std::find(previous, body.end(), ' ');
            }
            supported_mechs_.emplace_back(previous, current);
            return true;
        }
        return false;
    }
};

class sasl_list_mechs_request_body
{
  public:
    using response_body_type = sasl_list_mechs_response_body;
    static const inline client_opcode opcode = client_opcode::sasl_list_mechs;

  public:
    const std::string& key()
    {
        static std::string empty;
        return empty;
    }

    const std::vector<std::uint8_t>& extension()
    {
        static std::vector<std::uint8_t> empty;
        return empty;
    }

    const std::vector<std::uint8_t>& value()
    {
        static std::vector<std::uint8_t> empty;
        return empty;
    }

    std::size_t size()
    {
        return 0;
    }
};

} // namespace couchbase::protocol
