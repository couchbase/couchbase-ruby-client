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

#include <version.hxx>

namespace couchbase::operations
{
struct query_response_payload {
    struct query_metrics {
        std::string elapsed_time;
        std::string execution_time;
        std::uint64_t result_count;
        std::uint64_t result_size;
        std::optional<std::uint64_t> sort_count;
        std::optional<std::uint64_t> mutation_count;
        std::optional<std::uint64_t> error_count;
        std::optional<std::uint64_t> warning_count;
    };

    struct query_problem {
        std::uint64_t code;
        std::string message;
    };

    struct query_meta_data {
        std::string request_id;
        std::string client_context_id;
        std::string status;
        query_metrics metrics;
        std::optional<std::string> signature;
        std::optional<std::string> profile;
        std::optional<std::vector<query_problem>> warnings;
        std::optional<std::vector<query_problem>> errors;
    };

    query_meta_data meta_data{};
    std::vector<std::string> rows{};
};
} // namespace couchbase::operations

namespace tao::json
{
template<>
struct traits<couchbase::operations::query_response_payload> {
    template<template<typename...> class Traits>
    static couchbase::operations::query_response_payload as(const tao::json::basic_value<Traits>& v)
    {
        couchbase::operations::query_response_payload result;
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
            std::vector<couchbase::operations::query_response_payload::query_problem> problems{};
            for (auto& err : e->get_array()) {
                couchbase::operations::query_response_payload::query_problem problem;
                problem.code = err.at("code").get_unsigned();
                problem.message = err.at("msg").get_string();
                problems.emplace_back(problem);
            }
            result.meta_data.errors.emplace(problems);
        }

        const auto w = v.find("warnings");
        if (w != nullptr) {
            std::vector<couchbase::operations::query_response_payload::query_problem> problems{};
            for (auto& warn : w->get_array()) {
                couchbase::operations::query_response_payload::query_problem problem;
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
struct query_response {
    uuid::uuid_t client_context_id;
    std::error_code ec;
    query_response_payload payload{};
};

struct query_request {
    using response_type = query_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;

    static const inline service_type type = service_type::query;

    std::string statement;
    uuid::uuid_t client_context_id{ uuid::random() };

    bool adhoc{ true };
    bool metrics{ false };
    bool readonly{ false };

    std::optional<std::uint64_t> max_parallelism{};
    std::optional<std::uint64_t> scan_cap{};
    std::optional<std::uint64_t> pipeline_batch{};
    std::optional<std::uint64_t> pipeline_cap{};

    enum class profile_mode {
        off,
        phases,
        timings,
    };
    profile_mode profile{ profile_mode::off };

    std::map<std::string, tao::json::value> raw{};
    std::vector<tao::json::value> positional_parameters{};
    std::map<std::string, tao::json::value> named_parameters{};

    void encode_to(encoded_request_type& encoded)
    {
        encoded.headers["content-type"] = "application/json";
        tao::json::value body{ { "statement", statement }, { "client_context_id", uuid::to_string(client_context_id) } };
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
        switch (profile) {
            case profile_mode::phases:
                body["profile"] = "phases";
                break;
            case profile_mode::timings:
                body["profile"] = "timings";
                break;
            case profile_mode::off:
                break;
        }
        if (max_parallelism) {
            body["max_parallelism"] = max_parallelism.value();
        }
        if (pipeline_cap) {
            body["pipeline_cap"] = pipeline_cap.value();
        }
        if (pipeline_batch) {
            body["pipeline_batch"] = pipeline_batch.value();
        }
        if (scan_cap) {
            body["scan_cap"] = scan_cap.value();
        }
        if (!metrics) {
            body["metrics"] = false;
        }
        if (readonly) {
            body["readonly"] = true;
        }
        for (auto& param : raw) {
            body[param.first] = param.second;
        }
        encoded.method = "POST";
        encoded.path = "/query/service";
        encoded.body = tao::json::to_string(body);
    }
};

query_response
make_response(std::error_code ec, query_request& request, query_request::encoded_response_type encoded)
{
    query_response response{ request.client_context_id, ec };
    if (!ec) {
        spdlog::trace("query response: {}", encoded.body);
        response.payload = tao::json::from_string(encoded.body).as<query_response_payload>();
        Expects(response.payload.meta_data.client_context_id == uuid::to_string(request.client_context_id));
    }
    return response;
}

} // namespace couchbase::operations
