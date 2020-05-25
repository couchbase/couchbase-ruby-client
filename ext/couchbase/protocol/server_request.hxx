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

#include <tao/json.hpp>
#include <tao/json/contrib/traits.hpp>

#include <protocol/server_opcode.hxx>
#include <protocol/magic.hxx>
#include <protocol/status.hxx>
#include <protocol/datatype.hxx>
#include <protocol/cmd_info.hxx>

namespace couchbase::protocol
{

template<typename Body>
class server_request
{
  private:
    static const inline magic magic_ = magic::server_request;

    Body body_;
    server_opcode opcode_{ server_opcode::invalid };
    header_buffer header_;
    uint8_t data_type_;
    std::vector<std::uint8_t> data_;
    std::size_t body_size_;
    std::uint32_t opaque_;
    std::uint64_t cas_;
    cmd_info info_;

  public:
    server_request() = default;
    explicit server_request(io::binary_message& msg)
    {
        std::memcpy(header_.data(), &msg.header, sizeof(msg.header));
        verify_header();
        data_ = std::move(msg.body);
        parse_body();
    }

    [[nodiscard]] server_opcode opcode() const
    {
        return opcode_;
    }

    [[nodiscard]] std::size_t body_size() const
    {
        return body_size_;
    }

    [[nodiscard]] std::uint64_t cas() const
    {
        return cas_;
    }

    [[nodiscard]] std::uint32_t opaque() const
    {
        return opaque_;
    }

    Body& body()
    {
        return body_;
    }

    cmd_info& info()
    {
        return info_;
    }

    [[nodiscard]] header_buffer& header()
    {
        return header_;
    }

    void verify_header()
    {
        Expects(header_[0] == static_cast<std::uint8_t>(magic_));
        Expects(header_[1] == static_cast<std::uint8_t>(Body::opcode));
        opcode_ = static_cast<server_opcode>(header_[1]);
        data_type_ = header_[5];

        uint32_t field = 0;
        memcpy(&field, header_.data() + 8, sizeof(field));
        body_size_ = ntohl(field);
        data_.resize(body_size_);

        memcpy(&opaque_, header_.data() + 12, sizeof(opaque_));

        memcpy(&cas_, header_.data() + 16, sizeof(cas_));
    }

    void parse_body()
    {
        body_.parse(header_, data_, info_);
    }

    [[nodiscard]] std::vector<std::uint8_t>& data()
    {
        return data_;
    }
};
} // namespace couchbase::protocol
