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

#include <protocol/client_opcode.hxx>
#include <protocol/status.hxx>
#include <protocol/cmd_info.hxx>

#include <configuration.hxx>

namespace couchbase::protocol
{

namespace
{
// For some reason "projector" field gets duplicated in the configuration JSON
template<typename Consumer>
struct deduplicate_keys : Consumer {
    using Consumer::Consumer;

    using Consumer::keys_;
    using Consumer::stack_;
    using Consumer::value;

    void member()
    {
        Consumer::stack_.back().prepare_object().emplace(std::move(Consumer::keys_.back()), std::move(Consumer::value));
        Consumer::keys_.pop_back();
    }
};
} // namespace

class get_cluster_config_response_body
{
  public:
    static const inline client_opcode opcode = client_opcode::get_cluster_config;

  private:
    configuration config_;

  public:
    [[nodiscard]] configuration config()
    {
        return config_;
    }

    bool parse(protocol::status status,
               const header_buffer& header,
               std::uint8_t framing_extras_size,
               std::uint16_t key_size,
               std::uint8_t extras_size,
               const std::vector<uint8_t>& body,
               const cmd_info& info)
    {
        Expects(header[1] == static_cast<uint8_t>(opcode));
        if (status == protocol::status::success) {
            std::vector<uint8_t>::difference_type offset = framing_extras_size + key_size + extras_size;
            config_ = tao::json::from_string<deduplicate_keys>(std::string(body.begin() + offset, body.end())).as<configuration>();
            for (auto& node : config_.nodes) {
                if (node.this_node && node.hostname.empty()) {
                    node.hostname = info.remote_endpoint.address().to_string();
                }
            }
            return true;
        }
        return false;
    }
};

class get_cluster_config_request_body
{
  public:
    using response_body_type = get_cluster_config_response_body;
    static const inline client_opcode opcode = client_opcode::get_cluster_config;

  public:
    const std::string& key()
    {
        static std::string empty;
        return empty;
    }

    const std::vector<std::uint8_t>& framing_extras()
    {
        static std::vector<std::uint8_t> empty;
        return empty;
    }

    const std::vector<std::uint8_t>& extension()
    {
        static std::vector<std::uint8_t> empty;
        return empty;
    }

    const std::vector<std::uint8_t>& value()
    {
        static std::vector<std::uint8_t> empty;
        return empty;
    }

    std::size_t size()
    {
        return 0;
    }
};

} // namespace couchbase::protocol
