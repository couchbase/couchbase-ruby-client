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
#include <operations/document_id.hxx>

namespace couchbase::protocol
{

class lookup_in_response_body
{
  public:
    static const inline client_opcode opcode = client_opcode::subdoc_multi_lookup;

    struct lookup_in_field {
        protocol::status status;
        std::string value;
    };

  private:
    std::vector<lookup_in_field> fields_;

  public:
    std::vector<lookup_in_field>& fields()
    {
        return fields_;
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
        if (status == protocol::status::success || status == protocol::status::subdoc_multi_path_failure) {
            using offset_type = std::vector<uint8_t>::difference_type;
            offset_type offset = framing_extras_size + key_size + extras_size;
            fields_.reserve(16); /* we won't have more than 16 entries anyway */
            while (static_cast<std::size_t>(offset) < body.size()) {
                lookup_in_field field;

                std::uint16_t entry_status = 0;
                memcpy(&entry_status, body.data() + offset, sizeof(entry_status));
                entry_status = ntohs(entry_status);
                Expects(is_valid_status(entry_status));
                field.status = static_cast<protocol::status>(entry_status);
                offset += static_cast<offset_type>(sizeof(entry_status));

                std::uint32_t entry_size = 0;
                memcpy(&entry_size, body.data() + offset, sizeof(entry_size));
                entry_size = ntohl(entry_size);
                Expects(entry_size < 20 * 1024 * 1024);
                offset += static_cast<offset_type>(sizeof(entry_size));

                field.value.resize(entry_size);
                memcpy(field.value.data(), body.data() + offset, entry_size);
                offset += static_cast<offset_type>(entry_size);

                fields_.emplace_back(field);
            }
            return true;
        }
        return false;
    }
};

class lookup_in_request_body
{
  public:
    using response_body_type = lookup_in_response_body;
    static const inline client_opcode opcode = client_opcode::subdoc_multi_lookup;

    static const inline uint8_t doc_flag_access_deleted = 0x04;

    struct lookup_in_specs {
        static const inline uint8_t path_flag_xattr = 0x04;

        struct entry {
            std::uint8_t opcode;
            std::uint8_t flags;
            std::string path;
        };
        std::vector<entry> entries;

        void add_spec(subdoc_opcode operation, bool xattr, const std::string& path)
        {
            add_spec(static_cast<std::uint8_t>(operation), xattr ? path_flag_xattr : 0, path);
        }

        void add_spec(uint8_t operation, uint8_t flags, const std::string& path)
        {
            Expects(is_valid_subdoc_opcode(operation));
            entries.emplace_back(entry{ operation, flags, path });
        }
    };

  private:
    std::string key_;
    std::vector<std::uint8_t> extras_{};
    std::vector<std::uint8_t> value_{};

    std::uint8_t flags_{ 0 };
    lookup_in_specs specs_;

  public:
    void id(const operations::document_id& id)
    {
        key_ = id.key;
        if (id.collection_uid) {
            unsigned_leb128<uint32_t> encoded(*id.collection_uid);
            key_.insert(0, encoded.get());
        }
    }

    void access_deleted(bool value)
    {
        if (value) {
            flags_ = doc_flag_access_deleted;
        } else {
            flags_ = 0;
        }
    }

    void specs(const lookup_in_specs& specs)
    {
        specs_ = specs;
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
        if (extras_.empty()) {
            fill_extention();
        }
        return extras_;
    }

    const std::vector<std::uint8_t>& value()
    {
        if (value_.empty()) {
            fill_value();
        }
        return value_;
    }

    std::size_t size()
    {
        if (extras_.empty()) {
            fill_extention();
        }
        if (value_.empty()) {
            fill_value();
        }
        return key_.size() + extras_.size() + value_.size();
    }

  private:
    void fill_extention()
    {
        if (flags_ != 0) {
            extras_.resize(sizeof(flags_));
            extras_[0] = flags_;
        }
    }

    void fill_value()
    {
        size_t value_size = 0;
        for (auto& spec : specs_.entries) {
            value_size += sizeof(spec.opcode) + sizeof(spec.flags) + sizeof(std::uint16_t) + spec.path.size();
        }
        Expects(value_size > 0);
        value_.resize(value_size);
        std::vector<std::uint8_t>::size_type offset = 0;
        for (auto& spec : specs_.entries) {
            value_[offset++] = spec.opcode;
            value_[offset++] = spec.flags;
            std::uint16_t path_size = ntohs(gsl::narrow_cast<std::uint16_t>(spec.path.size()));
            std::memcpy(value_.data() + offset, &path_size, sizeof(path_size));
            offset += sizeof(path_size);
            std::memcpy(value_.data() + offset, spec.path.data(), spec.path.size());
            offset += spec.path.size();
        }
    }
};

} // namespace couchbase::protocol
