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
#include <protocol/frame_info_id.hxx>
#include <protocol/enhanced_error_info.hxx>
#include <utils/byteswap.hxx>

namespace couchbase::protocol
{

template<typename Body>
class client_response
{
  private:
    Body body_;
    magic magic_{ magic::client_response };
    client_opcode opcode_{ client_opcode::invalid };
    header_buffer header_{};
    uint8_t data_type_{ 0 };
    std::vector<std::uint8_t> data_{};
    std::uint16_t key_size_{ 0 };
    std::uint8_t framing_extras_size_{ 0 };
    std::uint8_t extras_size_{ 0 };
    std::size_t body_size_{ 0 };
    protocol::status status_{};
    std::optional<enhanced_error_info> error_;
    std::uint32_t opaque_{};
    std::uint64_t cas_{};
    cmd_info info_{};

  public:
    client_response() = default;
    explicit client_response(io::mcbp_message& msg)
    {
        header_ = msg.header_data();
        data_ = std::move(msg.body);
        verify_header();
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
        Expects(header_[0] == static_cast<std::uint8_t>(magic::alt_client_response) ||
                header_[0] == static_cast<std::uint8_t>(magic::client_response));
        Expects(header_[1] == static_cast<std::uint8_t>(Body::opcode));
        magic_ = static_cast<magic>(header_[0]);
        opcode_ = static_cast<client_opcode>(header_[1]);
        data_type_ = header_[5];

        uint16_t status = 0;
        memcpy(&status, header_.data() + 6, sizeof(status));
        status = ntohs(status);
        Expects(protocol::is_valid_status(status));
        status_ = static_cast<protocol::status>(status);

        extras_size_ = header_[4];
        if (magic_ == magic::alt_client_response) {
            framing_extras_size_ = header_[2];
            key_size_ = header_[3];
        } else {
            memcpy(&key_size_, header_.data() + 2, sizeof(key_size_));
            key_size_ = ntohs(key_size_);
        }

        uint32_t field = 0;
        memcpy(&field, header_.data() + 8, sizeof(field));
        body_size_ = ntohl(field);
        data_.resize(body_size_);

        memcpy(&opaque_, header_.data() + 12, sizeof(opaque_));

        memcpy(&cas_, header_.data() + 16, sizeof(cas_));
        cas_ = utils::byte_swap_64(cas_);
    }

    [[nodiscard]] std::optional<enhanced_error_info> error_info()
    {
        return error_;
    }

    [[nodiscard]] std::string error_message()
    {
        if (error_) {
            return fmt::format(R"(magic={}, opcode={}, status={}, error={})", magic_, opcode_, status_, *error_);
        }
        return fmt::format("magic={}, opcode={}, status={}", magic_, opcode_, status_);
    }

    void parse_body()
    {
        parse_framing_extras();
        bool parsed = body_.parse(status_, header_, framing_extras_size_, key_size_, extras_size_, data_, info_);
        if (status_ != protocol::status::success && !parsed && has_json_datatype(data_type_)) {
            auto error = tao::json::from_string(std::string(data_.begin() + framing_extras_size_ + extras_size_ + key_size_, data_.end()));
            if (error.is_object()) {
                auto& err_obj = error["error"];
                if (err_obj.is_object()) {
                    enhanced_error_info err{};
                    auto& ref = err_obj["ref"];
                    if (ref.is_string()) {
                        err.reference = ref.get_string();
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

    void parse_framing_extras()
    {
        if (framing_extras_size_ == 0) {
            return;
        }
        size_t offset = 0;
        while (offset < framing_extras_size_) {
            std::uint8_t frame_size = data_[offset] & 0xfU;
            std::uint8_t frame_id = (static_cast<std::uint32_t>(data_[offset]) >> 4U) & 0xfU;
            offset++;
            if (frame_id == static_cast<std::uint8_t>(response_frame_info_id::server_duration)) {
                if (frame_size == 2 && framing_extras_size_ - offset >= frame_size) {
                    std::uint16_t encoded_duration{};
                    std::memcpy(&encoded_duration, data_.data() + offset, sizeof(encoded_duration));
                    encoded_duration = ntohs(encoded_duration);
                    info_.server_duration_us = std::pow(encoded_duration, 1.74) / 2;
                }
            }
            offset += frame_size;
        }
    }

    [[nodiscard]] std::vector<std::uint8_t>& data()
    {
        return data_;
    }
};
} // namespace couchbase::protocol

template<>
struct fmt::formatter<couchbase::protocol::enhanced_error_info> : formatter<std::string> {
    template<typename FormatContext>
    auto format(const couchbase::protocol::enhanced_error_info& error, FormatContext& ctx)
    {
        if (!error.reference.empty() && !error.context.empty()) {
            format_to(ctx.out(), R"((ref: "{}", ctx: "{}"))", error.reference, error.context);
        } else if (!error.reference.empty()) {
            format_to(ctx.out(), R"((ref: "{}"))", error.reference);
        } else if (!error.context.empty()) {
            format_to(ctx.out(), R"((ctx: "{}"))", error.context);
        }
        return formatter<std::string>::format("", ctx);
    }
};
