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

#include <protocol/status.hxx>
#include <protocol/client_opcode.hxx>
#include <protocol/frame_info_id.hxx>
#include <protocol/unsigned_leb128.h>
#include <protocol/durability_level.hxx>
#include <mutation_token.hxx>
#include <utils/byteswap.hxx>

namespace couchbase::protocol
{

class upsert_response_body
{
  public:
    static const inline client_opcode opcode = client_opcode::upsert;

  private:
    mutation_token token_;

  public:
    mutation_token& token()
    {
        return token_;
    }

    bool parse(protocol::status status,
               const header_buffer& header,
               std::uint8_t framing_extras_size,
               std::uint16_t,
               std::uint8_t extras_size,
               const std::vector<uint8_t>& body,
               const cmd_info&)
    {
        Expects(header[1] == static_cast<uint8_t>(opcode));
        if (status == protocol::status::success) {
            std::vector<uint8_t>::difference_type offset = framing_extras_size;
            if (extras_size == 16) {
                memcpy(&token_.partition_uuid, body.data() + offset, sizeof(token_.partition_uuid));
                token_.partition_uuid = utils::byte_swap_64(token_.partition_uuid);
                offset += 8;

                memcpy(&token_.sequence_number, body.data() + offset, sizeof(token_.sequence_number));
                token_.sequence_number = utils::byte_swap_64(token_.sequence_number);
                return true;
            }
        }
        return false;
    }
};

class upsert_request_body
{
  public:
    using response_body_type = upsert_response_body;
    static const inline client_opcode opcode = client_opcode::upsert;

  private:
    std::string key_{};
    std::vector<std::uint8_t> extras_{};
    std::vector<std::uint8_t> content_{};
    std::uint32_t flags_{};
    std::uint32_t expiry_{};
    std::vector<std::uint8_t> framing_extras_{};

  public:
    void id(const document_id& id)
    {
        key_ = id.key;
        if (id.collection_uid) {
            unsigned_leb128<uint32_t> encoded(*id.collection_uid);
            key_.insert(0, encoded.get());
        }
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

    void content(const std::string& content)
    {
        content_ = { content.begin(), content.end() };
    }

    void flags(uint32_t flags)
    {
        flags_ = flags;
    }

    void expiry(uint32_t value)
    {
        expiry_ = value;
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
            fill_extention();
        }
        return extras_;
    }

    const std::vector<std::uint8_t>& value()
    {
        return content_;
    }

    std::size_t size()
    {
        if (extras_.empty()) {
            fill_extention();
        }
        return framing_extras_.size() + extras_.size() + key_.size() + content_.size();
    }

  private:
    void fill_extention()
    {
        extras_.resize(sizeof(flags_) + sizeof(expiry_));

        uint32_t field = htonl(flags_);
        memcpy(extras_.data(), &field, sizeof(field));

        field = htonl(expiry_);
        memcpy(extras_.data() + sizeof(flags_), &field, sizeof(field));
    }
};

} // namespace couchbase::protocol
