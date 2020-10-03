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

#include <document_id.hxx>
#include <protocol/cmd_lookup_in.hxx>
#include <io/retry_context.hxx>

namespace couchbase::operations
{

struct get_projected_response {
    document_id id;
    std::uint32_t opaque;
    std::error_code ec{};
    std::string value{};
    std::uint64_t cas{};
    std::uint32_t flags{};
    std::optional<std::uint32_t> expiry{};
};

struct get_projected_request {
    using encoded_request_type = protocol::client_request<protocol::lookup_in_request_body>;
    using encoded_response_type = protocol::client_response<protocol::lookup_in_response_body>;

    document_id id;
    std::uint16_t partition{};
    std::uint32_t opaque{};
    std::vector<std::string> projections{};
    bool with_expiry{ false };
    std::vector<std::string> effective_projections{};
    bool preserve_array_indexes{ false };
    std::chrono::milliseconds timeout{ timeout_defaults::key_value_timeout };
    io::retry_context<io::retry_strategy::best_effort> retries{ true };

    void encode_to(encoded_request_type& encoded)
    {
        encoded.opaque(opaque);
        encoded.partition(partition);
        encoded.body().id(id);

        effective_projections = projections;
        std::size_t num_projections = effective_projections.size();
        if (with_expiry) {
            num_projections++;
        }
        if (num_projections > 16) {
            // too many subdoc operations, better fetch full document
            effective_projections.clear();
        }

        protocol::lookup_in_request_body::lookup_in_specs specs{};
        if (with_expiry) {
            specs.add_spec(protocol::subdoc_opcode::get, true, "$document.exptime");
        }
        if (effective_projections.empty()) {
            specs.add_spec(protocol::subdoc_opcode::get_doc, false, "");
        } else {
            for (const auto& path : effective_projections) {
                specs.add_spec(protocol::subdoc_opcode::get, false, path);
            }
        }
        encoded.body().specs(specs);
    }
};

namespace priv
{

std::optional<tao::json::value>
subdoc_lookup(tao::json::value& root, const std::string& path)
{
    std::string::size_type offset = 0;
    tao::json::value* cur = &root;

    while (offset < path.size()) {
        std::string::size_type idx = path.find_first_of(".[]", offset);

        if (idx == std::string::npos) {
            std::string key = path.substr(offset);
            auto* val = cur->find(key);
            if (val != nullptr) {
                return *val;
            }
            break;
        }

        if (path[idx] == '.' || path[idx] == '[') {
            std::string key = path.substr(offset, idx - offset);
            auto* val = cur->find(key);
            if (val == nullptr) {
                break;
            }
            cur = val;
        } else if (path[idx] == ']') {
            if (!cur->is_array()) {
                break;
            }
            std::string key = path.substr(offset, idx - offset);
            int array_index = std::stoi(key);
            if (array_index == -1) {
                cur = &cur->get_array().back();
            } else if (static_cast<std::size_t>(array_index) < cur->get_array().size()) {
                cur = &cur->get_array().back();
            } else {
                break;
            }
            if (idx < path.size() - 1) {
                return *cur;
            }
            idx += 1;
        }
        offset = idx + 1;
    }

    return {};
}

void
subdoc_apply_projection(tao::json::value& root, const std::string& path, tao::json::value& value, bool preserve_array_indexes)
{
    std::string::size_type offset = 0;
    tao::json::value* cur = &root;

    while (offset < path.size()) {
        std::string::size_type idx = path.find_first_of(".[]", offset);

        if (idx == std::string::npos) {
            cur->operator[](path.substr(offset)) = value;
            break;
        }

        if (path[idx] == '.') {
            std::string key = path.substr(offset, idx - offset);
            tao::json::value* child = cur->find(key);
            if (child == nullptr) {
                cur->operator[](key) = tao::json::empty_object;
                child = cur->find(key);
            }
            cur = child;
        } else if (path[idx] == '[') {
            std::string key = path.substr(offset, idx - offset);
            tao::json::value* child = cur->find(key);
            if (child == nullptr) {
                cur->operator[](key) = tao::json::empty_array;
                child = cur->find(key);
            }
            cur = child;
        } else if (path[idx] == ']') {
            tao::json::value child;
            if (idx == path.size() - 1) {
                child = value;
            } else if (path[idx + 1] == '.') {
                child = tao::json::empty_object;
            } else if (path[idx + 1] == '[') {
                child = tao::json::empty_array;
            } else {
                Expects(false);
            }
            if (preserve_array_indexes) {
                int array_index = std::stoi(path.substr(offset, idx - offset));
                if (array_index >= 0) {
                    if (static_cast<std::size_t>(array_index) >= cur->get_array().size()) {
                        cur->get_array().resize(static_cast<std::size_t>(array_index + 1), tao::json::null);
                    }
                    cur->at(static_cast<std::size_t>(array_index)) = child;
                    cur = &cur->at(static_cast<std::size_t>(array_index));
                } else {
                    // index is negative, just append and let user decide what it means
                    cur->get_array().push_back(child);
                    cur = &cur->get_array().back();
                }
            } else {
                cur->get_array().push_back(child);
                cur = &cur->get_array().back();
            }
            ++idx;
        }
        offset = idx + 1;
    }
}
} // namespace priv

get_projected_response
make_response(std::error_code ec, get_projected_request& request, get_projected_request::encoded_response_type encoded)
{
    get_projected_response response{ request.id, encoded.opaque(), ec };
    if (ec && response.opaque == 0) {
        response.opaque = request.opaque;
    }
    if (!ec) {
        response.cas = encoded.cas();
        if (request.with_expiry && !encoded.body().fields()[0].value.empty()) {
            response.expiry = gsl::narrow_cast<std::uint32_t>(std::stoul(encoded.body().fields()[0].value));
        }
        if (request.effective_projections.empty()) {
            // from full document
            if (request.projections.empty() && request.with_expiry) {
                // special case when user only wanted full+expiration
                response.value = encoded.body().fields()[1].value;
            } else {
                tao::json::value full_doc = tao::json::from_string(encoded.body().fields()[request.with_expiry ? 1 : 0].value);
                tao::json::value new_doc;
                for (const auto& projection : request.projections) {
                    auto value_to_apply = priv::subdoc_lookup(full_doc, projection);
                    if (value_to_apply) {
                        priv::subdoc_apply_projection(new_doc, projection, *value_to_apply, request.preserve_array_indexes);
                    } else {
                        response.ec = std::make_error_code(error::key_value_errc::path_not_found);
                        return response;
                    }
                }
                response.value = tao::json::to_string(new_doc);
            }
        } else {
            tao::json::value new_doc = tao::json::empty_object;
            std::size_t offset = request.with_expiry ? 1 : 0;
            for (const auto& projection : request.projections) {
                auto& field = encoded.body().fields()[offset++];
                if (field.status == protocol::status::success && !field.value.empty()) {
                    auto value_to_apply = tao::json::from_string(field.value);
                    priv::subdoc_apply_projection(new_doc, projection, value_to_apply, request.preserve_array_indexes);
                } else {
                    response.ec = std::make_error_code(error::key_value_errc::path_not_found);
                    return response;
                }
            }
            response.value = tao::json::to_string(new_doc);
        }
    }
    return response;
}

} // namespace couchbase::operations
