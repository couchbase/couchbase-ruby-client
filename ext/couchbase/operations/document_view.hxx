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

#include <tao/json.hpp>
#include <operations/design_document.hxx>
#include <utils/url_codec.hxx>

namespace couchbase::operations
{
struct document_view_response {
    struct meta_data {
        std::optional<std::uint64_t> total_rows{};
        std::optional<std::string> debug_info{};
    };

    struct row {
        std::optional<std::string> id;
        std::string key;
        std::string value;
    };

    struct problem {
        std::string code;
        std::string message;
    };

    std::string client_context_id;
    std::error_code ec;
    document_view_response::meta_data meta_data{};
    std::vector<document_view_response::row> rows{};
    std::optional<problem> error{};
};

struct document_view_request {
    using response_type = document_view_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;

    static const inline service_type type = service_type::views;

    std::string client_context_id{ uuid::to_string(uuid::random()) };
    std::chrono::milliseconds timeout{ timeout_defaults::management_timeout };

    std::string bucket_name;
    std::string document_name;
    std::string view_name;
    design_document::name_space name_space;

    std::optional<std::uint64_t> limit;
    std::optional<std::uint64_t> skip;

    enum class scan_consistency {
        not_bounded,
        update_after,
        request_plus,
    };
    std::optional<scan_consistency> consistency;

    std::vector<std::string> keys;

    std::optional<std::string> key;
    std::optional<std::string> start_key;
    std::optional<std::string> end_key;
    std::optional<std::string> start_key_doc_id;
    std::optional<std::string> end_key_doc_id;
    std::optional<bool> inclusive_end;

    std::optional<bool> reduce;
    std::optional<bool> group;
    std::optional<std::uint32_t> group_level;
    bool debug{ false };

    enum class sort_order { ascending, descending };
    std::optional<sort_order> order;

    void encode_to(encoded_request_type& encoded)
    {
        std::vector<std::string> query_string;

        if (debug) {
            query_string.emplace_back("debug=true");
        }
        if (limit) {
            query_string.emplace_back(fmt::format("limit={}", *limit));
        }
        if (skip) {
            query_string.emplace_back(fmt::format("skip={}", *skip));
        }
        if (consistency) {
            switch (*consistency) {
                case scan_consistency::not_bounded:
                    query_string.emplace_back("stale=ok");
                    break;
                case scan_consistency::update_after:
                    query_string.emplace_back("stale=update_after");
                    break;
                case scan_consistency::request_plus:
                    query_string.emplace_back("stale=false");
                    break;
            }
        }
        if (key) {
            query_string.emplace_back(fmt::format("key={}", utils::string_codec::form_encode(*key)));
        }
        if (start_key) {
            query_string.emplace_back(fmt::format("start_key={}", utils::string_codec::form_encode(*start_key)));
        }
        if (end_key) {
            query_string.emplace_back(fmt::format("end_key={}", utils::string_codec::form_encode(*end_key)));
        }
        if (start_key_doc_id) {
            query_string.emplace_back(fmt::format("start_key_doc_id={}", utils::string_codec::form_encode(*start_key_doc_id)));
        }
        if (end_key_doc_id) {
            query_string.emplace_back(fmt::format("end_key_doc_id={}", utils::string_codec::form_encode(*end_key_doc_id)));
        }
        if (inclusive_end) {
            query_string.emplace_back(fmt::format("inclusive_end={}", inclusive_end.value() ? "true" : "false"));
        }
        if (reduce) {
            query_string.emplace_back(fmt::format("reduce={}", reduce.value() ? "true" : "false"));
        }
        if (group) {
            query_string.emplace_back(fmt::format("group={}", group.value() ? "true" : "false"));
        }
        if (group_level) {
            query_string.emplace_back(fmt::format("group_level={}", *group_level));
        }
        if (order) {
            switch (*order) {
                case sort_order::descending:
                    query_string.emplace_back("descending=true");
                    break;
                case sort_order::ascending:
                    query_string.emplace_back("descending=false");
                    break;
            }
        }

        tao::json::value body = tao::json::empty_object;
        if (!keys.empty()) {
            tao::json::value keys_array = tao::json::empty_array;
            for (const auto& entry : keys) {
                keys_array.push_back(tao::json::from_string(entry));
            }
            body["keys"] = keys_array;
        }

        encoded.type = type;
        encoded.method = "POST";
        encoded.headers["content-type"] = "application/json";
        encoded.path = fmt::format("/{}/_design/{}{}/_view/{}?{}",
                                   bucket_name,
                                   name_space == design_document::name_space::development ? "dev_" : "",
                                   document_name,
                                   view_name,
                                   fmt::join(query_string, "&"));
        encoded.body = tao::json::to_string(body);
    }
};

document_view_response
make_response(std::error_code ec, document_view_request& request, document_view_request::encoded_response_type encoded)
{
    document_view_response response{ request.client_context_id, ec };
    if (!ec) {
        if (encoded.status_code == 200) {
            tao::json::value payload = tao::json::from_string(encoded.body);
            const auto* total_rows = payload.find("total_rows");
            if (total_rows != nullptr && total_rows->is_unsigned()) {
                response.meta_data.total_rows = total_rows->get_unsigned();
            }
            const auto* debug_info = payload.find("debug_info");
            if (debug_info != nullptr && debug_info->is_object()) {
                response.meta_data.debug_info.emplace(tao::json::to_string(*debug_info));
            }
            const auto* rows = payload.find("rows");
            if (rows != nullptr && rows->is_array()) {
                for (const auto& entry : rows->get_array()) {
                    document_view_response::row row{};
                    const auto* id = entry.find("id");
                    if (id != nullptr && id->is_string()) {
                        row.id = id->get_string();
                    }
                    row.key = tao::json::to_string(entry.at("key"));
                    row.value = tao::json::to_string(entry.at("value"));
                    response.rows.emplace_back(row);
                }
            }
        } else if (encoded.status_code == 400) {
            tao::json::value payload = tao::json::from_string(encoded.body);
            document_view_response::problem problem{};
            const auto* error = payload.find("error");
            if (error != nullptr && error->is_string()) {
                problem.code = error->get_string();
            }
            const auto* reason = payload.find("reason");
            if (reason != nullptr && reason->is_string()) {
                problem.message = reason->get_string();
            }
            response.error.emplace(problem);
            response.ec = std::make_error_code(error::common_errc::invalid_argument);
        } else if (encoded.status_code == 404) {
            response.ec = std::make_error_code(error::view_errc::design_document_not_found);
        } else {
            response.ec = std::make_error_code(error::common_errc::internal_server_failure);
        }
    }
    return response;
}

} // namespace couchbase::operations
