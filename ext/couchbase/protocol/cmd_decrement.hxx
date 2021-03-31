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

#include <protocol/unsigned_leb128.h>

#include <protocol/client_opcode.hxx>
#include <document_id.hxx>

namespace couchbase::protocol
{

class decrement_response_body
{
  public:
    static const inline client_opcode opcode = client_opcode::decrement;

  private:
    mutation_token token_;
    std::uint64_t content_;

  public:
    std::uint64_t content()
    {
        return content_;
    }

    mutation_token& token()
    {
        return token_;
    }

    bool parse(protocol::status status,
               const header_buffer& header,
               std::uint8_t framing_extras_size,
               std::uint16_t key_size,
               std::uint8_t extras_size,
               const std::vector<uint8_t>& body,
               const cmd_info&)
    {
        Expects(header[1] == static_cast<uint8_t>(opcode));
        if (status == protocol::status::success) {
            using offset_type = std::vector<uint8_t>::difference_type;
            offset_type offset = framing_extras_size;
            if (extras_size == 16) {
                memcpy(&token_.partition_uuid, body.data() + offset, sizeof(token_.partition_uuid));
                token_.partition_uuid = utils::byte_swap_64(token_.partition_uuid);
                offset += 8;

                memcpy(&token_.sequence_number, body.data() + offset, sizeof(token_.sequence_number));
                token_.sequence_number = utils::byte_swap_64(token_.sequence_number);
                offset += 8;
            }
            offset += key_size;
            memcpy(&content_, body.data() + offset, sizeof(content_));
            content_ = utils::byte_swap_64(content_);
            return true;
        }
        return false;
    }
};

class decrement_request_body
{
  public:
    using response_body_type = decrement_response_body;
    static const inline client_opcode opcode = client_opcode::decrement;

  private:
    std::string key_;
    std::vector<std::uint8_t> framing_extras_{};
    std::uint64_t delta_{ 1 };
    std::uint64_t initial_value_{ 0 };
    std::uint32_t expiry_{ 0 };
    std::vector<std::uint8_t> extras_{};

  public:
    void id(const document_id& id)
    {
        key_ = id.key;
        if (id.collection_uid) {
            unsigned_leb128<uint32_t> encoded(*id.collection_uid);
            key_.insert(0, encoded.get());
        }
    }

    void delta(std::uint64_t value)
    {
        delta_ = value;
    }

    void initial_value(std::uint64_t value)
    {
        initial_value_ = value;
    }

    void expiry(std::uint32_t value)
    {
        expiry_ = value;
    }

    void durability(protocol::durability_level level, std::optional<std::uint16_t> timeout)
    {
        if (level == protocol::durability_level::none) {
            return;
        }
        auto frame_id = static_cast<uint8_t>(protocol::request_frame_info_id::durability_requirement);
        auto extras_size = framing_extras_.size();
        if (timeout) {
            framing_extras_.resize(extras_size + 4);
            framing_extras_[extras_size + 0] = static_cast<std::uint8_t>((static_cast<std::uint32_t>(frame_id) << 4U) | 3U);
            framing_extras_[extras_size + 1] = static_cast<std::uint8_t>(level);
            uint16_t val = htons(*timeout);
            memcpy(framing_extras_.data() + extras_size + 2, &val, sizeof(val));
        } else {
            framing_extras_.resize(extras_size + 2);
            framing_extras_[extras_size + 0] = static_cast<std::uint8_t>(static_cast<std::uint32_t>(frame_id) << 4U | 1U);
            framing_extras_[extras_size + 1] = static_cast<std::uint8_t>(level);
        }
    }

    void preserve_expiry()
    {
        auto frame_id = static_cast<uint8_t>(protocol::request_frame_info_id::preserve_ttl);
        auto extras_size = framing_extras_.size();
        framing_extras_.resize(extras_size + 1);
        framing_extras_[extras_size + 0] = static_cast<std::uint8_t>(static_cast<std::uint32_t>(frame_id) << 4U | 0U);
    }

    const std::string& key()
    {
        return key_;
    }

    const std::vector<std::uint8_t>& framing_extras()
    {
        return framing_extras_;
    }

    const std::vector<std::uint8_t>& extras()
    {
        if (extras_.empty()) {
            fill_extras();
        }
        return extras_;
    }

    const std::vector<std::uint8_t>& value()
    {
        static std::vector<std::uint8_t> empty;
        return empty;
    }

    [[nodiscard]] std::size_t size()
    {
        if (extras_.empty()) {
            fill_extras();
        }
        return extras_.size() + key_.size();
    }

  private:
    void fill_extras()
    {
        extras_.resize(sizeof(delta_) + sizeof(initial_value_) + sizeof(expiry_));
        using offset_type = std::vector<uint8_t>::difference_type;
        offset_type offset = 0;

        std::uint64_t num = utils::byte_swap_64(delta_);
        memcpy(extras_.data() + offset, &num, sizeof(num));
        offset += static_cast<offset_type>(sizeof(delta_));

        num = utils::byte_swap_64(initial_value_);
        memcpy(extras_.data() + offset, &num, sizeof(num));
        offset += static_cast<offset_type>(sizeof(delta_));

        std::uint32_t ttl = htonl(expiry_);
        memcpy(extras_.data() + offset, &ttl, sizeof(ttl));
    }
};

} // namespace couchbase::protocol
