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

#include <protocol/hello_feature.hxx>

namespace couchbase::protocol
{

class hello_response_body
{
  public:
    static const inline client_opcode opcode = client_opcode::hello;

  private:
    std::vector<hello_feature> supported_features_;

  public:
    [[nodiscard]] const std::vector<hello_feature>& supported_features() const
    {
        return supported_features_;
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
            std::vector<uint8_t>::difference_type offset = framing_extras_size + key_size + extras_size;
            size_t value_size = body.size() - static_cast<std::size_t>(offset);
            Expects(value_size % 2 == 0);
            size_t num_features = value_size / 2;
            supported_features_.reserve(num_features);
            const auto* value = body.data() + offset;
            for (size_t i = 0; i < num_features; i++) {
                std::uint16_t field = 0;
                std::memcpy(&field, value + i * 2, sizeof(std::uint16_t));
                field = ntohs(field);
                if (is_valid_hello_feature(field)) {
                    supported_features_.push_back(static_cast<hello_feature>(field));
                }
            }
            return true;
        }
        return false;
    }
};

class hello_request_body
{
  public:
    using response_body_type = hello_response_body;
    static const inline client_opcode opcode = client_opcode::hello;

  private:
    std::string key_;
    std::vector<hello_feature> features_{
        hello_feature::tcp_nodelay,
        hello_feature::mutation_seqno,
        hello_feature::xattr,
        hello_feature::xerror,
        hello_feature::select_bucket,
        hello_feature::snappy,
        hello_feature::json,
        hello_feature::duplex,
        hello_feature::clustermap_change_notification,
        hello_feature::unordered_execution,
        hello_feature::alt_request_support,
        hello_feature::tracing,
        hello_feature::sync_replication,
        hello_feature::vattr,
        hello_feature::collections,
        hello_feature::subdoc_create_as_deleted,
    };
    std::vector<std::uint8_t> value_;

  public:
    void user_agent(std::string val)
    {
        key_ = std::move(val);
    }

    [[nodiscard]] const std::string& user_agent() const
    {
        return key_;
    }

    [[nodiscard]] const std::vector<hello_feature>& features() const
    {
        return features_;
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
        return key_.size() + value_.size();
    }

  private:
    void fill_body()
    {
        value_.resize(2 * features_.size());
        for (std::size_t idx = 0; idx < features_.size(); idx++) {
            value_[idx * 2] = 0; // we don't need this byte while feature codes fit the 8-bit
            value_[idx * 2 + 1] = static_cast<uint8_t>(features_[idx]);
        }
    }
};

} // namespace couchbase::protocol
