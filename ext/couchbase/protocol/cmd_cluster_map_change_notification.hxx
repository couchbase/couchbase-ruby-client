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

#include <protocol/server_opcode.hxx>
#include <protocol/cmd_info.hxx>

#include <configuration.hxx>

namespace couchbase::protocol
{

class cluster_map_change_notification_request_body
{
  public:
    static const inline server_opcode opcode = server_opcode::cluster_map_change_notification;

  private:
    uint32_t protocol_revision_;
    std::string bucket_;
    configuration config_;

  public:
    [[nodiscard]] uint32_t protocol_revision()
    {
        return protocol_revision_;
    }

    [[nodiscard]] configuration config()
    {
        return config_;
    }

    bool parse(const header_buffer& header, const std::vector<uint8_t>& body, const cmd_info& info)
    {
        Expects(header[1] == static_cast<uint8_t>(opcode));
        using offset_type = std::vector<uint8_t>::difference_type;
        uint8_t ext_size = header[4];
        offset_type offset = ext_size;
        if (ext_size == 4) {
            memcpy(&protocol_revision_, body.data(), sizeof(protocol_revision_));
            protocol_revision_ = ntohl(protocol_revision_);
        }
        uint16_t key_size = 0;
        memcpy(&key_size, header.data() + 2, sizeof(key_size));
        key_size = ntohs(key_size);
        bucket_.assign(body.begin() + offset, body.begin() + offset + key_size);
        offset += key_size;
        config_ = tao::json::from_string<deduplicate_keys>(std::string(body.begin() + offset, body.end())).as<configuration>();
        for (auto& node : config_.nodes) {
            if (node.this_node && node.hostname.empty()) {
                node.hostname = info.remote_endpoint.address().to_string();
            }
        }
        return true;
    }
};

} // namespace couchbase::protocol
