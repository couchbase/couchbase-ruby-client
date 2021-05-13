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

#include <cstring>

#include <asio/ip/udp.hpp>
#include <io/dns_message.hxx>

namespace couchbase::io::dns
{
class dns_codec
{
  public:
    static dns_message decode(const std::vector<uint8_t>& payload)
    {
        dns_message message{};
        std::size_t offset = 0;

        std::memcpy(&message.header.id, payload.data() + offset, sizeof(std::uint16_t));
        offset += sizeof(std::uint16_t);
        message.header.id = ntohs(message.header.id);

        uint16_t flags = 0;
        std::memcpy(&flags, payload.data() + offset, sizeof(std::uint16_t));
        offset += sizeof(std::uint16_t);
        message.header.flags.decode(ntohs(flags));

        std::memcpy(&message.header.question_records, payload.data() + offset, sizeof(std::uint16_t));
        offset += sizeof(std::uint16_t);
        message.header.question_records = ntohs(message.header.question_records);

        std::memcpy(&message.header.answer_records, payload.data() + offset, sizeof(std::uint16_t));
        offset += sizeof(std::uint16_t);
        message.header.answer_records = ntohs(message.header.answer_records);

        std::memcpy(&message.header.authority_records, payload.data() + offset, sizeof(std::uint16_t));
        offset += sizeof(std::uint16_t);
        message.header.authority_records = ntohs(message.header.authority_records);

        std::memcpy(&message.header.additional_records, payload.data() + offset, sizeof(std::uint16_t));
        offset += sizeof(std::uint16_t);
        message.header.additional_records = ntohs(message.header.additional_records);

        for (std::uint16_t idx = 0; idx < message.header.question_records; ++idx) {
            question_record qr;
            qr.name = get_name(payload, offset);

            std::uint16_t val = 0;
            std::memcpy(&val, payload.data() + offset, sizeof(std::uint16_t));
            offset += sizeof(std::uint16_t);
            val = ntohs(val);
            qr.type = resource_type(val);

            std::memcpy(&val, payload.data() + offset, sizeof(std::uint16_t));
            offset += sizeof(std::uint16_t);
            val = ntohs(val);
            qr.klass = static_cast<resource_class>(val);

            message.questions.emplace_back(qr);
        }

        message.answers.reserve(message.header.answer_records);
        for (std::uint16_t idx = 0; idx < message.header.answer_records; ++idx) {
            srv_record ar;
            ar.name = get_name(payload, offset);

            std::uint16_t val = 0;
            std::memcpy(&val, payload.data() + offset, sizeof(std::uint16_t));
            offset += sizeof(std::uint16_t);
            val = ntohs(val);
            ar.type = resource_type(val);

            std::memcpy(&val, payload.data() + offset, sizeof(std::uint16_t));
            offset += sizeof(std::uint16_t);
            val = ntohs(val);
            ar.klass = resource_class(val);

            std::memcpy(&ar.ttl, payload.data() + offset, sizeof(std::uint32_t));
            offset += static_cast<std::uint16_t>(4U);
            ar.ttl = ntohl(ar.ttl);

            std::uint16_t size = 0;
            std::memcpy(&size, payload.data() + offset, sizeof(std::uint16_t));
            offset += sizeof(std::uint16_t);
            size = ntohs(size);

            if (ar.klass != resource_class::in || ar.type != resource_type::srv) {
                // ignore everything except SRV answers
                offset += size;
                continue;
            }

            std::memcpy(&val, payload.data() + offset, sizeof(std::uint16_t));
            offset += sizeof(std::uint16_t);
            ar.priority = ntohs(val);

            std::memcpy(&val, payload.data() + offset, sizeof(std::uint16_t));
            offset += sizeof(std::uint16_t);
            ar.weight = ntohs(val);

            std::memcpy(&val, payload.data() + offset, sizeof(std::uint16_t));
            offset += sizeof(std::uint16_t);
            ar.port = ntohs(val);

            ar.target = get_name(payload, offset);

            message.answers.emplace_back(ar);
        }
        return message;
    }

    static std::vector<uint8_t> encode(const dns_message& message)
    {
        std::vector<std::uint8_t> payload;
        payload.resize(message.request_size(), 0);
        std::size_t offset = 0;

        // write header
        {
            uint16_t val;

            val = htons(message.header.id);
            std::memcpy(payload.data() + offset, &val, sizeof(std::uint16_t));
            offset += sizeof(std::uint16_t);

            val = htons(message.header.flags.encode());
            std::memcpy(payload.data() + offset, &val, sizeof(std::uint16_t));
            offset += sizeof(std::uint16_t);

            val = htons(static_cast<std::uint16_t>(message.questions.size()));
            std::memcpy(payload.data() + offset, &val, sizeof(std::uint16_t));
            offset += sizeof(std::uint16_t) + 3 * sizeof(std::uint16_t); // answer, authority, additional are all zeros
        }

        // write body
        for (const auto& question : message.questions) {
            for (const auto& label : question.name.labels) {
                payload[offset] = static_cast<std::uint8_t>(label.size());
                ++offset;
                std::memcpy(payload.data() + offset, label.data(), label.size());
                offset += label.size();
            }
            payload[offset] = '\0';
            ++offset;

            uint16_t val;

            val = htons(static_cast<std::uint16_t>(question.type));
            std::memcpy(payload.data() + offset, &val, sizeof(std::uint16_t));
            offset += sizeof(std::uint16_t);

            val = htons(static_cast<std::uint16_t>(question.klass));
            std::memcpy(payload.data() + offset, &val, sizeof(std::uint16_t));
            offset += sizeof(std::uint16_t);
        }
        return payload;
    }

  private:
    static resource_name get_name(const std::vector<std::uint8_t>& payload, std::size_t& offset)
    {
        resource_name name{};
        std::optional<std::size_t> save_offset{};
        while (true) {
            std::uint8_t len = payload[offset];
            if (len == 0) {
                offset += 1;
                if (save_offset) {
                    // restore offset after pointer jump
                    offset = *save_offset;
                }
                return name;
            }
            if ((len & 0b1100'0000U) != 0) {
                std::uint16_t ptr = 0;
                std::memcpy(&ptr, payload.data() + offset, sizeof(std::uint16_t));
                ptr = ntohs(ptr);
                ptr &= 0b0011'1111'1111'1111U;
                // store old offset and jump to pointer
                save_offset = offset + sizeof(std::uint16_t);
                offset = ptr;
            } else {
                std::string label(payload.data() + offset + 1, payload.data() + offset + 1 + len);
                name.labels.emplace_back(label);
                offset += static_cast<std::uint16_t>(1U + len);
            }
        }
    }
};
} // namespace couchbase::io::dns
