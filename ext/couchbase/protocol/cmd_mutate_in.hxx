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

class mutate_in_response_body
{
  public:
    static const inline client_opcode opcode = client_opcode::subdoc_multi_mutation;

    struct mutate_in_field {
        std::uint8_t index{};
        protocol::status status{};
        std::string value{};
    };

  private:
    std::vector<mutate_in_field> fields_;
    mutation_token token_;

  public:
    std::vector<mutate_in_field>& fields()
    {
        return fields_;
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
        if (status == protocol::status::success || status == protocol::status::subdoc_multi_path_failure) {
            using offset_type = std::vector<uint8_t>::difference_type;
            offset_type offset = framing_extras_size;
            if (extras_size == 16) {
                memcpy(&token_.partition_uuid, body.data() + offset, sizeof(token_.partition_uuid));
                token_.partition_uuid = utils::byte_swap_64(token_.partition_uuid);
                offset += 8;

                memcpy(&token_.sequence_number, body.data() + offset, sizeof(token_.sequence_number));
                token_.sequence_number = utils::byte_swap_64(token_.sequence_number);
                offset += 8;
            } else {
                offset += extras_size;
            }
            offset += key_size;
            fields_.reserve(16); /* we won't have more than 16 entries anyway */
            while (static_cast<std::size_t>(offset) < body.size()) {
                mutate_in_field field;

                field.index = body[static_cast<std::size_t>(offset)];
                offset++;

                std::uint16_t entry_status = 0;
                memcpy(&entry_status, body.data() + offset, sizeof(entry_status));
                entry_status = ntohs(entry_status);
                Expects(is_valid_status(entry_status));
                field.status = static_cast<protocol::status>(entry_status);
                offset += static_cast<offset_type>(sizeof(entry_status));

                if (field.status == protocol::status::success) {
                    std::uint32_t entry_size = 0;
                    memcpy(&entry_size, body.data() + offset, sizeof(entry_size));
                    entry_size = ntohl(entry_size);
                    Expects(entry_size < 20 * 1024 * 1024);
                    offset += static_cast<offset_type>(sizeof(entry_size));

                    field.value.resize(entry_size);
                    memcpy(field.value.data(), body.data() + offset, entry_size);
                    offset += static_cast<offset_type>(entry_size);
                }

                fields_.emplace_back(field);
            }
            return true;
        }
        return false;
    }
};

class mutate_in_request_body
{
  public:
    using response_body_type = mutate_in_response_body;
    static const inline client_opcode opcode = client_opcode::subdoc_multi_mutation;

    enum class store_semantics_type {
        /**
         * Replace the document, fail if it does not exist. This is the default.
         */
        replace,

        /**
         * Replace the document or create it if it does not exist.
         */
        upsert,

        /**
         * Replace the document or create it if it does not exist.
         */
        insert,
    };

    /**
     * Create the document if it does not exist. Implies `path_flag_create_parents`.
     * and `upsert` mutation semantics. Not valid with `insert`.
     */
    static const inline uint8_t doc_flag_mkdoc = 0b0000'0001;

    /**
     * Add the document only if it does not exist. Implies `path_flag_create_parents`.
     * Not valid with `doc_flag_mkdoc`.
     */
    static const inline uint8_t doc_flag_add = 0b0000'0010;

    /**
     * Allow access to XATTRs for deleted documents (instead of returning KEY_ENOENT).
     */
    static const inline uint8_t doc_flag_access_deleted = 0b0000'0100;

    /**
     * Used with `doc_flag_mkdoc` / `doc_flag_add`; if the document does not exist then creat
     * it in the "Deleted" state, instead of the normal "Alive" state.
     * Not valid unless `doc_flag_mkdoc` or `doc_flag_add` specified.
     */
    static const inline uint8_t doc_flag_create_as_deleted = 0b0000'1000;

    struct mutate_in_specs {
        /**
         * Should non-existent intermediate paths be created
         */
        static const inline uint8_t path_flag_create_parents = 0b0000'0001;

        /**
         * If set, the path refers to an Extended Attribute (XATTR).
         * If clear, the path refers to a path inside the document body.
         */
        static const inline uint8_t path_flag_xattr = 0b0000'0100;

        /**
         * Expand macro values inside extended attributes. The request is
         * invalid if this flag is set without `path_flag_create_parents` being set.
         */
        static const inline uint8_t path_flag_expand_macros = 0b0001'0000;

        struct entry {
            std::uint8_t opcode;
            std::uint8_t flags;
            std::string path;
            std::string param;
            std::size_t original_index{};
        };
        std::vector<entry> entries;

        static inline uint8_t build_path_flags(bool xattr, bool create_parents, bool expand_macros)
        {
            uint8_t flags = 0;
            if (xattr) {
                flags |= path_flag_xattr;
            }
            if (create_parents) {
                flags |= path_flag_create_parents;
            }
            if (expand_macros) {
                flags |= path_flag_expand_macros;
            }
            return flags;
        }

        void add_spec(subdoc_opcode operation,
                      bool xattr,
                      bool create_parents,
                      bool expand_macros,
                      const std::string& path,
                      const std::string& param)
        {
            add_spec(static_cast<std::uint8_t>(operation), build_path_flags(xattr, create_parents, expand_macros), path, param);
        }

        void add_spec(subdoc_opcode operation,
                      bool xattr,
                      bool create_parents,
                      bool expand_macros,
                      const std::string& path,
                      const std::int64_t increment)
        {
            Expects(operation == protocol::subdoc_opcode::counter);
            add_spec(static_cast<std::uint8_t>(operation),
                     build_path_flags(xattr, create_parents, expand_macros),
                     path,
                     std::to_string(increment));
        }

        void add_spec(subdoc_opcode operation, bool xattr, const std::string& path)
        {
            Expects(operation == protocol::subdoc_opcode::remove);
            add_spec(static_cast<std::uint8_t>(operation), build_path_flags(xattr, false, false), path, "");
        }

        void add_spec(uint8_t operation, uint8_t flags, const std::string& path, const std::string& param)
        {
            Expects(is_valid_subdoc_opcode(operation));
            entries.emplace_back(entry{ operation, flags, path, param });
        }
    };

  private:
    std::string key_;
    std::vector<std::uint8_t> extras_{};
    std::vector<std::uint8_t> value_{};

    std::uint32_t expiry_{ 0 };
    std::uint8_t flags_{ 0 };
    mutate_in_specs specs_;
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

    void expiry(uint32_t value)
    {
        expiry_ = value;
    }

    void access_deleted(bool value)
    {
        if (value) {
            flags_ |= doc_flag_access_deleted;
        } else {
            flags_ &= static_cast<std::uint8_t>(~doc_flag_access_deleted);
        }
    }

    void create_as_deleted(bool value)
    {
        if (value) {
            flags_ |= doc_flag_create_as_deleted;
        } else {
            flags_ &= static_cast<std::uint8_t>(~doc_flag_create_as_deleted);
        }
    }

    void store_semantics(store_semantics_type semantics)
    {
        flags_ &= 0b1111'1100; /* reset first two bits */
        switch (semantics) {
            case store_semantics_type::replace:
                /* leave bits as zeros */
                break;
            case store_semantics_type::upsert:
                flags_ |= doc_flag_mkdoc;
                break;
            case store_semantics_type::insert:
                flags_ |= doc_flag_add;
                break;
        }
    }

    void specs(const mutate_in_specs& specs)
    {
        specs_ = specs;
    }

    void durability(protocol::durability_level level, std::optional<std::uint16_t> timeout)
    {
        if (level == protocol::durability_level::none) {
            return;
        }
        auto frame_id = static_cast<uint8_t>(protocol::request_frame_info_id::durability_requirement);
        if (timeout) {
            framing_extras_.resize(4);
            framing_extras_[0] = static_cast<std::uint8_t>((static_cast<std::uint32_t>(frame_id) << 4U) | 3U);
            framing_extras_[1] = static_cast<std::uint8_t>(level);
            uint16_t val = htons(*timeout);
            memcpy(framing_extras_.data() + 2, &val, sizeof(val));
        } else {
            framing_extras_.resize(2);
            framing_extras_[0] = static_cast<std::uint8_t>(static_cast<std::uint32_t>(frame_id) << 4U | 1U);
            framing_extras_[1] = static_cast<std::uint8_t>(level);
        }
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
        return framing_extras_.size() + extras_.size() + key_.size() + value_.size();
    }

  private:
    void fill_extention()
    {
        if (expiry_ != 0) {
            extras_.resize(sizeof(expiry_));
            std::uint32_t field = htonl(expiry_);
            memcpy(extras_.data(), &field, sizeof(field));
        }
        if (flags_ != 0) {
            std::size_t offset = extras_.size();
            extras_.resize(offset + sizeof(flags_));
            extras_[offset] = flags_;
        }
    }

    void fill_value()
    {
        size_t value_size = 0;
        for (auto& spec : specs_.entries) {
            value_size += sizeof(spec.opcode) + sizeof(spec.flags) + sizeof(std::uint16_t) + spec.path.size() + sizeof(std::uint32_t) +
                          spec.param.size();
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

            std::uint32_t param_size = ntohl(gsl::narrow_cast<std::uint32_t>(spec.param.size()));
            std::memcpy(value_.data() + offset, &param_size, sizeof(param_size));
            offset += sizeof(param_size);

            std::memcpy(value_.data() + offset, spec.path.data(), spec.path.size());
            offset += spec.path.size();

            if (param_size != 0u) {
                std::memcpy(value_.data() + offset, spec.param.data(), spec.param.size());
                offset += spec.param.size();
            }
        }
    }
};

} // namespace couchbase::protocol
