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

#include <protocol/unsigned_leb128.h>

#include <protocol/client_opcode.hxx>
#include <document_id.hxx>

namespace couchbase::protocol
{

class exists_response_body
{
  public:
    static const inline client_opcode opcode = client_opcode::observe;

  private:
    std::uint16_t partition_id_;
    std::string key_;
    std::uint8_t status_;
    std::uint64_t cas_;

  public:
    [[nodiscard]] std::uint16_t partition_id()
    {
        return partition_id_;
    }

    [[nodiscard]] std::uint64_t cas()
    {
        return cas_;
    }

    [[nodiscard]] const std::string& key()
    {
        return key_;
    }

    [[nodiscard]] std::uint8_t status()
    {
        return status_;
    }

    bool parse(protocol::status status,
               const header_buffer& header,
               std::uint8_t framing_extras_size,
               std::uint16_t key_size,
               std::uint8_t extras_size,
               const std::vector<uint8_t>& body,
               const cmd_info& /* info */)
    {
        Expects(header[1] == static_cast<uint8_t>(opcode));
        if (status == protocol::status::success) {
            using offset_type = std::vector<uint8_t>::difference_type;
            offset_type offset = framing_extras_size + extras_size + key_size;

            memcpy(&partition_id_, body.data() + offset, sizeof(partition_id_));
            partition_id_ = ntohs(partition_id_);
            offset += static_cast<offset_type>(sizeof(partition_id_));

            std::uint16_t key_len{};
            memcpy(&key_len, body.data() + offset, sizeof(key_len));
            key_len = ntohs(key_len);
            offset += static_cast<offset_type>(sizeof(key_len));

            key_.resize(key_len);
            memcpy(key_.data(), body.data() + offset, key_len);
            offset += key_len;

            status_ = body[static_cast<std::size_t>(offset)];
            offset++;

            memcpy(&cas_, body.data() + offset, sizeof(cas_));
        }
        return false;
    }
};

class exists_request_body
{
  public:
    using response_body_type = exists_response_body;
    static const inline client_opcode opcode = client_opcode::observe;

  private:
    std::uint16_t partition_id_;
    std::string key_;
    std::vector<std::uint8_t> value_{};

  public:
    void id(std::uint16_t partition_id, const document_id& id)
    {
        partition_id_ = partition_id;
        key_ = id.key;
        if (id.collection_uid) {
            unsigned_leb128<uint32_t> encoded(*id.collection_uid);
            key_.insert(0, encoded.get());
        }
    }

    const std::string& key()
    {
        /* for observe key goes in the body */
        static std::string empty;
        return empty;
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
        if (value_.empty()) {
            fill_body();
        }
        return value_;
    }

    std::size_t size()
    {
        if (value_.empty()) {
            fill_body();
        }
        return value_.size();
    }

  private:
    void fill_body()
    {
        std::vector<std::uint8_t>::size_type offset = 0;

        value_.resize(2 * sizeof(std::uint16_t) + key_.size());

        uint16_t field = htons(partition_id_);
        memcpy(value_.data() + offset, &field, sizeof(field));
        offset += sizeof(field);

        field = htons(static_cast<uint16_t>(key_.size()));
        memcpy(value_.data() + offset, &field, sizeof(field));
        offset += sizeof(field);

        std::memcpy(value_.data() + offset, key_.data(), key_.size());
    }
};

} // namespace couchbase::protocol
