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

#include <protocol/magic.hxx>
#include <protocol/status.hxx>
#include <protocol/datatype.hxx>
#include <protocol/cmd_info.hxx>

namespace couchbase::protocol
{

struct enhanced_error {
    std::string context;
    std::string ref;
};

template<typename Body>
class client_response
{
  private:
    static const inline magic magic_ = magic::client_response;

    Body body_;
    client_opcode opcode_{ client_opcode::invalid };
    header_buffer header_;
    uint8_t data_type_;
    std::vector<std::uint8_t> data_;
    std::size_t body_size_;
    protocol::status status_;
    std::optional<enhanced_error> error_;
    std::uint32_t opaque_;
    std::uint64_t cas_;
    cmd_info info_;

  public:
    client_response() = default;
    explicit client_response(io::mcbp_message& msg)
    {
        std::memcpy(header_.data(), &msg.header, sizeof(msg.header));
        verify_header();
        data_ = std::move(msg.body);
        parse_body();
    }

    [[nodiscard]] client_opcode opcode() const
    {
        return opcode_;
    }

    [[nodiscard]] protocol::status status() const
    {
        return status_;
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
        opcode_ = static_cast<client_opcode>(header_[1]);
        data_type_ = header_[5];

        uint16_t status = 0;
        memcpy(&status, header_.data() + 6, sizeof(status));
        status = ntohs(status);
        Expects(protocol::is_valid_status(status));
        status_ = static_cast<protocol::status>(status);

        uint32_t field = 0;
        memcpy(&field, header_.data() + 8, sizeof(field));
        body_size_ = ntohl(field);
        data_.resize(body_size_);

        memcpy(&opaque_, header_.data() + 12, sizeof(opaque_));

        memcpy(&cas_, header_.data() + 16, sizeof(cas_));
    }

    std::string error_message()
    {
        if (error_) {
            return fmt::format(R"({}:{} {} {})", magic_, opcode_, status_, *error_);
        }
        return fmt::format("{}:{} {}", magic_, opcode_, status_);
    }

    void parse_body()
    {
        bool parsed = body_.parse(status_, header_, data_, info_);
        if (status_ != protocol::status::success && !parsed && has_json_datatype(data_type_)) {
            auto error = tao::json::from_string(std::string(data_.begin(), data_.end()));
            if (error.is_object()) {
                auto& err_obj = error["error"];
                if (err_obj.is_object()) {
                    enhanced_error err{};
                    auto& ref = err_obj["ref"];
                    if (ref.is_string()) {
                        err.ref = ref.get_string();
                    }
                    auto& ctx = err_obj["context"];
                    if (ctx.is_string()) {
                        err.context = ctx.get_string();
                    }
                    error_.emplace(err);
                }
            }
        }
    }

    [[nodiscard]] std::vector<std::uint8_t>& data()
    {
        return data_;
    }
};
} // namespace couchbase::protocol

template<>
struct fmt::formatter<couchbase::protocol::enhanced_error> : formatter<std::string> {
    template<typename FormatContext>
    auto format(const couchbase::protocol::enhanced_error& error, FormatContext& ctx)
    {
        if (!error.ref.empty() && !error.context.empty()) {
            format_to(ctx.out(), R"((ref: "{}", ctx: "{}"))", error.ref, error.context);
        } else if (!error.ref.empty()) {
            format_to(ctx.out(), R"((ref: "{}"))", error.ref);
        } else if (!error.context.empty()) {
            format_to(ctx.out(), R"((ctx: "{}"))", error.context);
        }
        return formatter<std::string>::format("", ctx);
    }
};
