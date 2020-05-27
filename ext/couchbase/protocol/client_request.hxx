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

#ifndef WIN32
#include <arpa/inet.h>
#endif

#include <snappy.h>

#include <gsl/gsl_util>
#include <protocol/client_opcode.hxx>
#include <protocol/magic.hxx>
#include <protocol/client_response.hxx>

namespace couchbase::protocol
{
template<typename Body>
class client_request
{
  public:
    using body_type = Body;
    using response_body_type = typename Body::response_body_type;
    using response_type = client_response<response_body_type>;

  private:
    magic magic_{ magic::client_request };

    client_opcode opcode_{ Body::opcode };
    std::uint16_t partition_{ 0 };
    std::uint32_t opaque_{ 0 };
    Body body_;
    std::vector<std::uint8_t> payload_;

  public:
    client_opcode opcode()
    {
        return opcode_;
    }

    void opaque(std::uint32_t val)
    {
        opaque_ = val;
    }

    std::uint32_t opaque()
    {
        return opaque_;
    }

    void opcode(client_opcode val)
    {
        opcode_ = val;
    }

    void partition(std::uint16_t val)
    {
        partition_ = val;
    }

    Body& body()
    {
        return body_;
    }

    std::vector<std::uint8_t>& data(bool try_to_compress = false)
    {
        switch (opcode_) {
            case protocol::client_opcode::insert:
            case protocol::client_opcode::upsert:
            case protocol::client_opcode::replace:
                write_payload(try_to_compress);
                break;
            default:
                write_payload(false);
                break;
        }
        return payload_;
    }

  private:
    void write_payload(bool try_to_compress)
    {
        payload_.resize(header_size + body_.size(), 0);
        payload_[0] = static_cast<uint8_t>(magic_);
        payload_[1] = static_cast<uint8_t>(opcode_);

        auto framing_extras = body_.framing_extras();

        uint16_t key_size = gsl::narrow_cast<uint16_t>(body_.key().size());
        if (framing_extras.size() == 0) {
            key_size = htons(key_size);
            memcpy(payload_.data() + 2, &key_size, sizeof(key_size));
        } else {
            magic_ = protocol::magic::alt_client_request;
            payload_[0] = static_cast<uint8_t>(magic_);
            payload_[2] = gsl::narrow_cast<std::uint8_t>(framing_extras.size());
            payload_[3] = gsl::narrow_cast<std::uint8_t>(key_size);
        }

        uint8_t ext_size = gsl::narrow_cast<uint8_t>(body_.extension().size());
        memcpy(payload_.data() + 4, &ext_size, sizeof(ext_size));

        uint16_t vbucket = ntohs(gsl::narrow_cast<uint16_t>(partition_));
        memcpy(payload_.data() + 6, &vbucket, sizeof(vbucket));

        uint32_t body_size = htonl(gsl::narrow_cast<uint32_t>(body_.size()));
        memcpy(payload_.data() + 8, &body_size, sizeof(body_size));

        memcpy(payload_.data() + 12, &opaque_, sizeof(opaque_));

        auto body_itr = payload_.begin() + header_size;
        if (framing_extras.size() > 0) {
            body_itr = std::copy(framing_extras.begin(), framing_extras.end(), body_itr);
        }
        body_itr = std::copy(body_.extension().begin(), body_.extension().end(), body_itr);
        body_itr = std::copy(body_.key().begin(), body_.key().end(), body_itr);

        static const std::size_t min_size_to_compress = 32;
        static const double min_ratio = 0.83;
        if (try_to_compress && body_.value().size() > min_size_to_compress) {
            std::string compressed;
            std::size_t compressed_size =
              snappy::Compress(reinterpret_cast<const char*>(body_.value().data()), body_.value().size(), &compressed);
            if (gsl::narrow_cast<double>(compressed_size) / gsl::narrow_cast<double>(body().value().size()) < min_ratio) {
                std::copy(compressed.begin(), compressed.end(), body_itr);
                payload_[5] |= static_cast<uint8_t>(protocol::datatype::snappy);
                size_t new_body_size = body_.size() - (body_.value().size() - compressed_size);
                body_size = htonl(gsl::narrow_cast<uint32_t>(new_body_size));
                memcpy(payload_.data() + 8, &body_size, sizeof(body_size));
                payload_.resize(header_size + new_body_size);
                return;
            }
        }
        std::copy(body_.value().begin(), body_.value().end(), body_itr);
    }
};
} // namespace couchbase::protocol
