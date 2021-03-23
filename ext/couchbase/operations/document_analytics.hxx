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

#include <gsl/gsl>

#include <spdlog/spdlog.h>

#include <tao/json.hpp>

#include <version.hxx>

#include <errors.hxx>
#include <mutation_token.hxx>
#include <service_type.hxx>
#include <platform/uuid.h>
#include <timeout_defaults.hxx>
#include <io/http_message.hxx>
#include <error_context/analytics.hxx>

namespace couchbase::operations
{
struct analytics_response_payload {
    struct analytics_metrics {
        std::string elapsed_time;
        std::string execution_time;
        std::uint64_t result_count;
        std::uint64_t result_size;
        std::optional<std::uint64_t> sort_count;
        std::optional<std::uint64_t> mutation_count;
        std::optional<std::uint64_t> error_count;
        std::optional<std::uint64_t> warning_count;
    };

    struct analytics_problem {
        std::uint64_t code;
        std::string message;
    };

    struct analytics_meta_data {
        std::string request_id;
        std::string client_context_id;
        std::string status;
        analytics_metrics metrics;
        std::optional<std::string> signature;
        std::optional<std::string> profile;
        std::optional<std::vector<analytics_problem>> warnings;
        std::optional<std::vector<analytics_problem>> errors;
    };

    analytics_meta_data meta_data{};
    std::vector<std::string> rows{};
};
} // namespace couchbase::operations

namespace tao::json
{
template<>
struct traits<couchbase::operations::analytics_response_payload> {
    template<template<typename...> class Traits>
    static couchbase::operations::analytics_response_payload as(const tao::json::basic_value<Traits>& v)
    {
        couchbase::operations::analytics_response_payload result;
        result.meta_data.request_id = v.at("requestID").get_string();
        result.meta_data.client_context_id = v.at("clientContextID").get_string();
        result.meta_data.status = v.at("status").get_string();
        const auto s = v.find("signature");
        if (s != nullptr) {
            result.meta_data.signature = tao::json::to_string(*s);
        }

        const auto p = v.find("profile");
        if (p != nullptr) {
            result.meta_data.profile = tao::json::to_string(*p);
        }

        const auto m = v.find("metrics");
        if (m != nullptr) {
            result.meta_data.metrics.result_count = m->at("resultCount").get_unsigned();
            result.meta_data.metrics.result_size = m->at("resultSize").get_unsigned();
            result.meta_data.metrics.elapsed_time = m->at("elapsedTime").get_string();
            result.meta_data.metrics.execution_time = m->at("executionTime").get_string();
            result.meta_data.metrics.sort_count = m->template optional<std::uint64_t>("sortCount");
            result.meta_data.metrics.mutation_count = m->template optional<std::uint64_t>("mutationCount");
            result.meta_data.metrics.error_count = m->template optional<std::uint64_t>("errorCount");
            result.meta_data.metrics.warning_count = m->template optional<std::uint64_t>("warningCount");
        }

        const auto e = v.find("errors");
        if (e != nullptr) {
            std::vector<couchbase::operations::analytics_response_payload::analytics_problem> problems{};
            for (auto& err : e->get_array()) {
                couchbase::operations::analytics_response_payload::analytics_problem problem;
                problem.code = err.at("code").get_unsigned();
                problem.message = err.at("msg").get_string();
                problems.emplace_back(problem);
            }
            result.meta_data.errors.emplace(problems);
        }

        const auto w = v.find("warnings");
        if (w != nullptr) {
            std::vector<couchbase::operations::analytics_response_payload::analytics_problem> problems{};
            for (auto& warn : w->get_array()) {
                couchbase::operations::analytics_response_payload::analytics_problem problem;
                problem.code = warn.at("code").get_unsigned();
                problem.message = warn.at("msg").get_string();
                problems.emplace_back(problem);
            }
            result.meta_data.warnings.emplace(problems);
        }
        const auto r = v.find("results");
        if (r != nullptr) {
            result.rows.reserve(result.meta_data.metrics.result_count);
            for (auto& row : r->get_array()) {
                result.rows.emplace_back(tao::json::to_string(row));
            }
        }

        return result;
    }
};
} // namespace tao::json

namespace couchbase::operations
{
struct analytics_response {
    error_context::analytics ctx;
    analytics_response_payload payload{};
};

struct analytics_request {
    using response_type = analytics_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;
    using error_context_type = error_context::analytics;

    enum class scan_consistency_type { not_bounded, request_plus };

    static const inline service_type type = service_type::analytics;

    std::chrono::milliseconds timeout{ timeout_defaults::analytics_timeout };
    std::string statement;
    std::string client_context_id{ uuid::to_string(uuid::random()) };

    bool readonly{ false };
    bool priority{ false };
    std::optional<std::string> bucket_name{};
    std::optional<std::string> scope_name{};
    std::optional<std::string> scope_qualifier{};

    std::optional<scan_consistency_type> scan_consistency{};

    std::map<std::string, tao::json::value> raw{};
    std::vector<tao::json::value> positional_parameters{};
    std::map<std::string, tao::json::value> named_parameters{};

    std::string body_str{};

    [[nodiscard]] std::error_code encode_to(encoded_request_type& encoded, http_context& context)
    {
        tao::json::value body{ { "statement", statement },
                               { "client_context_id", client_context_id },
                               { "timeout", fmt::format("{}ms", timeout.count()) } };
        if (positional_parameters.empty()) {
            for (auto& param : named_parameters) {
                Expects(param.first.empty() == false);
                std::string key = param.first;
                if (key[0] != '$') {
                    key.insert(key.begin(), '$');
                }
                body[key] = param.second;
            }
        } else {
            body["args"] = positional_parameters;
        }
        if (readonly) {
            body["readonly"] = true;
        }
        if (scan_consistency) {
            switch (scan_consistency.value()) {
                case scan_consistency_type::not_bounded:
                    body["scan_consistency"] = "not_bounded";
                    break;
                case scan_consistency_type::request_plus:
                    body["scan_consistency"] = "request_plus";
                    break;
            }
        }
        if (scope_qualifier) {
            body["query_context"] = scope_qualifier;
        } else if (scope_name) {
            if (bucket_name) {
                // for analytics bucket_name.scope_name is quoted as a single unit (unlike n1ql query)
                body["query_context"] = fmt::format("default:`{}.{}`", *bucket_name, *scope_name);
            }
        }
        for (auto& param : raw) {
            body[param.first] = param.second;
        }
        encoded.type = type;
        encoded.headers["content-type"] = "application/json";
        if (priority) {
            encoded.headers["analytics-priority"] = "-1";
        }
        encoded.method = "POST";
        encoded.path = "/query/service";
        body_str = tao::json::to_string(body);
        encoded.body = body_str;
        if (context.options.show_queries) {
            spdlog::info("ANALYTICS: {}", tao::json::to_string(body["statement"]));
        } else {
            spdlog::debug("ANALYTICS: {}", tao::json::to_string(body["statement"]));
        }
        return {};
    }
};

analytics_response
make_response(error_context::analytics&& ctx, analytics_request& request, analytics_request::encoded_response_type&& encoded)
{
    analytics_response response{ ctx };
    response.ctx.statement = request.statement;
    response.ctx.parameters = request.body_str;
    if (!response.ctx.ec) {
        try {
            response.payload = tao::json::from_string(encoded.body).as<analytics_response_payload>();
        } catch (tao::json::pegtl::parse_error& e) {
            response.ctx.ec = std::make_error_code(error::common_errc::parsing_failure);
            return response;
        }
        Expects(response.payload.meta_data.client_context_id == request.client_context_id);
        if (response.payload.meta_data.status != "success") {
            bool server_timeout = false;
            bool job_queue_is_full = false;
            bool dataset_not_found = false;
            bool dataverse_not_found = false;
            bool dataset_exists = false;
            bool dataverse_exists = false;
            bool link_not_found = false;
            bool compilation_failure = false;

            if (response.payload.meta_data.errors) {
                for (const auto& error : *response.payload.meta_data.errors) {
                    switch (error.code) {
                        case 21002: /* Request timed out and will be cancelled */
                            server_timeout = true;
                            break;
                        case 23007: /* Job queue is full with [string] jobs */
                            job_queue_is_full = true;
                            break;
                        case 24044: /* Cannot find dataset [string] because there is no dataverse declared, nor an alias with name [string]!
                                     */
                        case 24045: /* Cannot find dataset [string] in dataverse [string] nor an alias with name [string]! */
                        case 24025: /* Cannot find dataset with name [string] in dataverse [string] */
                            dataset_not_found = true;
                            break;
                        case 24034: /* Cannot find dataverse with name [string] */
                            dataverse_not_found = true;
                            break;
                        case 24040: /* A dataset with name [string] already exists in dataverse [string] */
                            dataset_exists = true;
                            break;
                        case 24039: /* A dataverse with this name [string] already exists. */
                            dataverse_exists = true;
                            break;
                        case 24006: /* Link [string] does not exist | Link [string] does not exist */
                            link_not_found = true;
                            break;
                        default:
                            if (error.code >= 24000 && error.code < 25000) {
                                compilation_failure = true;
                            }
                    }
                }
            }
            if (compilation_failure) {
                response.ctx.ec = std::make_error_code(error::analytics_errc::compilation_failure);
            } else if (link_not_found) {
                response.ctx.ec = std::make_error_code(error::analytics_errc::link_not_found);
            } else if (dataset_not_found) {
                response.ctx.ec = std::make_error_code(error::analytics_errc::dataset_not_found);
            } else if (dataverse_not_found) {
                response.ctx.ec = std::make_error_code(error::analytics_errc::dataverse_not_found);
            } else if (server_timeout) {
                response.ctx.ec = std::make_error_code(error::common_errc::unambiguous_timeout);
            } else if (dataset_exists) {
                response.ctx.ec = std::make_error_code(error::analytics_errc::dataset_exists);
            } else if (dataverse_exists) {
                response.ctx.ec = std::make_error_code(error::analytics_errc::dataverse_exists);
            } else if (job_queue_is_full) {
                response.ctx.ec = std::make_error_code(error::analytics_errc::job_queue_full);
            } else {
                response.ctx.ec = std::make_error_code(error::common_errc::internal_server_failure);
            }
        }
    }
    return response;
}

} // namespace couchbase::operations
