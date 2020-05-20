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

#include <error_map.hxx>

namespace couchbase::protocol
{

class get_error_map_response_body
{
  public:
    static const inline client_opcode opcode = client_opcode::get_error_map;

  private:
    error_map errmap_;

  public:
    [[nodiscard]] couchbase::error_map errmap()
    {
        return errmap_;
    }

    bool parse(protocol::status status, const header_buffer& header, const std::vector<uint8_t>& body, const cmd_info&)
    {
        Expects(header[1] == static_cast<uint8_t>(opcode));
        if (status == protocol::status::success) {
            try {
                errmap_ = tao::json::from_string(std::string(body.begin(), body.end())).as<error_map>();
            } catch (tao::json::pegtl::parse_error& e) {
                spdlog::critical("unable to parse JSON: {}", std::string(body.begin(), body.end()));
            }
            return true;
        }
        return false;
    }
};

class get_error_map_request_body
{
  public:
    using response_body_type = get_error_map_response_body;
    static const inline client_opcode opcode = client_opcode::get_error_map;

  private:
    std::uint16_t version_{ 1 };
    std::vector<std::uint8_t> value_;

  public:
    void version(std::uint16_t version)
    {
        version_ = version;
    }

    const std::string& key()
    {
        static std::string empty;
        return empty;
    }

    const std::vector<std::uint8_t>& extension()
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
        std::uint16_t version = htons(version_);
        value_.resize(sizeof(version));
        std::memcpy(value_.data(), &version, sizeof(version));
    }
};

} // namespace couchbase::protocol
