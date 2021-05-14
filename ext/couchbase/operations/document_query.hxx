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
#include <io/http_context.hxx>
#include <io/http_message.hxx>
#include <mutation_token.hxx>
#include <platform/uuid.h>
#include <service_type.hxx>
#include <timeout_defaults.hxx>
#include <error_context/query.hxx>

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
        std::uint64_t code{};
        std::string message{};
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
    std::optional<std::string> prepared{};
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

        if (const auto* i = v.find("clientContextID"); i != nullptr) {
            result.meta_data.client_context_id = i->get_string();
        }
        result.meta_data.status = v.at("status").get_string();
        if (const auto* s = v.find("signature"); s != nullptr) {
            result.meta_data.signature = tao::json::to_string(*s);
        }
        if (const auto* c = v.find("prepared"); c != nullptr) {
            result.prepared = c->get_string();
        }
        if (const auto* p = v.find("profile"); p != nullptr) {
            result.meta_data.profile = tao::json::to_string(*p);
        }

        if (const auto* m = v.find("metrics"); m != nullptr) {
            result.meta_data.metrics.result_count = m->at("resultCount").get_unsigned();
            result.meta_data.metrics.result_size = m->at("resultSize").get_unsigned();
            result.meta_data.metrics.elapsed_time = m->at("elapsedTime").get_string();
            result.meta_data.metrics.execution_time = m->at("executionTime").get_string();
            result.meta_data.metrics.sort_count = m->template optional<std::uint64_t>("sortCount");
            result.meta_data.metrics.mutation_count = m->template optional<std::uint64_t>("mutationCount");
            result.meta_data.metrics.error_count = m->template optional<std::uint64_t>("errorCount");
            result.meta_data.metrics.warning_count = m->template optional<std::uint64_t>("warningCount");
        }

        if (const auto* e = v.find("errors"); e != nullptr) {
            std::vector<couchbase::operations::query_response_payload::query_problem> problems{};
            for (auto& err : e->get_array()) {
                couchbase::operations::query_response_payload::query_problem problem;
                problem.code = err.at("code").get_unsigned();
                problem.message = err.at("msg").get_string();
                problems.emplace_back(problem);
            }
            result.meta_data.errors.emplace(problems);
        }

        if (const auto* w = v.find("warnings"); w != nullptr) {
            std::vector<couchbase::operations::query_response_payload::query_problem> problems{};
            for (auto& warn : w->get_array()) {
                couchbase::operations::query_response_payload::query_problem problem;
                problem.code = warn.at("code").get_unsigned();
                problem.message = warn.at("msg").get_string();
                problems.emplace_back(problem);
            }
            result.meta_data.warnings.emplace(problems);
        }

        if (const auto* r = v.find("results"); r != nullptr) {
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
    error_context::query ctx;
    query_response_payload payload{};
};

struct query_request {
    using response_type = query_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;
    using error_context_type = error_context::query;

    enum class scan_consistency_type { not_bounded, request_plus };

    static const inline service_type type = service_type::query;

    std::string statement;
    std::string client_context_id{ uuid::to_string(uuid::random()) };

    bool adhoc{ true };
    bool metrics{ false };
    bool readonly{ false };
    bool flex_index{ false };

    std::optional<std::uint64_t> max_parallelism{};
    std::optional<std::uint64_t> scan_cap{};
    std::optional<std::uint64_t> scan_wait{};
    std::optional<std::uint64_t> pipeline_batch{};
    std::optional<std::uint64_t> pipeline_cap{};
    std::optional<scan_consistency_type> scan_consistency{};
    std::vector<mutation_token> mutation_state{};
    std::chrono::milliseconds timeout{ timeout_defaults::query_timeout };
    std::optional<std::string> bucket_name{};
    std::optional<std::string> scope_name{};
    std::optional<std::string> scope_qualifier{};

    enum class profile_mode {
        off,
        phases,
        timings,
    };
    profile_mode profile{ profile_mode::off };

    std::map<std::string, tao::json::value> raw{};
    std::vector<tao::json::value> positional_parameters{};
    std::map<std::string, tao::json::value> named_parameters{};
    std::optional<http_context> ctx_{};
    bool extract_encoded_plan_{ false };

    std::string body_str{};

    [[nodiscard]] std::error_code encode_to(encoded_request_type& encoded, http_context& context)
    {
        ctx_.emplace(context);
        tao::json::value body{};
        if (adhoc) {
            body["statement"] = statement;
        } else {
            auto entry = ctx_->cache.get(statement);
            if (entry) {
                body["prepared"] = entry->name;
                if (entry->plan) {
                    body["encoded_plan"] = entry->plan.value();
                }
            } else {
                body["statement"] = "PREPARE " + statement;
                if (context.config.supports_enhanced_prepared_statements()) {
                    body["auto_execute"] = true;
                } else {
                    extract_encoded_plan_ = true;
                }
            }
        }
        body["client_context_id"] = client_context_id;
        body["timeout"] = fmt::format(
          "{}ms", ((timeout > std::chrono::milliseconds(5'000)) ? (timeout - std::chrono::milliseconds(500)) : timeout).count());
        if (positional_parameters.empty()) {
            for (const auto& [name, value] : named_parameters) {
                Expects(name.empty() == false);
                std::string key = name;
                if (key[0] != '$') {
                    key.insert(key.begin(), '$');
                }
                body[key] = value;
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
            body["max_parallelism"] = std::to_string(max_parallelism.value());
        }
        if (pipeline_cap) {
            body["pipeline_cap"] = std::to_string(pipeline_cap.value());
        }
        if (pipeline_batch) {
            body["pipeline_batch"] = std::to_string(pipeline_batch.value());
        }
        if (scan_cap) {
            body["scan_cap"] = std::to_string(scan_cap.value());
        }
        if (!metrics) {
            body["metrics"] = false;
        }
        if (readonly) {
            body["readonly"] = true;
        }
        if (flex_index) {
            body["use_fts"] = true;
        }
        bool check_scan_wait = false;
        if (scan_consistency) {
            switch (scan_consistency.value()) {
                case scan_consistency_type::not_bounded:
                    body["scan_consistency"] = "not_bounded";
                    break;
                case scan_consistency_type::request_plus:
                    check_scan_wait = true;
                    body["scan_consistency"] = "request_plus";
                    break;
            }
        } else if (!mutation_state.empty()) {
            check_scan_wait = true;
            body["scan_consistency"] = "at_plus";
            tao::json::value scan_vectors = tao::json::empty_object;
            for (const auto& token : mutation_state) {
                auto* bucket = scan_vectors.find(token.bucket_name);
                if (bucket == nullptr) {
                    scan_vectors[token.bucket_name] = tao::json::empty_object;
                    bucket = scan_vectors.find(token.bucket_name);
                }
                auto& bucket_obj = bucket->get_object();
                bucket_obj[std::to_string(token.partition_id)] =
                  std::vector<tao::json::value>{ token.sequence_number, std::to_string(token.partition_uuid) };
            }
            body["scan_vectors"] = scan_vectors;
        }
        if (check_scan_wait && scan_wait) {
            body["scan_wait"] = fmt::format("{}ms", scan_wait.value());
        }
        if (scope_qualifier) {
            body["query_context"] = scope_qualifier;
        } else if (scope_name) {
            if (bucket_name) {
                body["query_context"] = fmt::format("default:`{}`.`{}`", *bucket_name, *scope_name);
            }
        }
        for (const auto& [name, value] : raw) {
            body[name] = value;
        }
        encoded.type = type;
        encoded.headers["connection"] = "keep-alive";
        encoded.headers["content-type"] = "application/json";
        encoded.method = "POST";
        encoded.path = "/query/service";
        body_str = tao::json::to_string(body);
        encoded.body = body_str;

        tao::json::value stmt = body["statement"];
        tao::json::value prep = body["prepared"];
        if (!stmt.is_string()) {
            stmt = statement;
        }
        if (!prep.is_string()) {
            prep = false;
        }
        if (ctx_->options.show_queries) {
            spdlog::info("QUERY: prep={}, {}", tao::json::to_string(prep), tao::json::to_string(stmt));
        } else {
            spdlog::debug("QUERY: prep={}, {}", tao::json::to_string(prep), tao::json::to_string(stmt));
        }
        return {};
    }
};

query_response
make_response(error_context::query&& ctx, query_request& request, query_request::encoded_response_type&& encoded)
{
    query_response response{ std::move(ctx) };
    response.ctx.statement = request.statement;
    response.ctx.parameters = request.body_str;
    if (!response.ctx.ec) {
        try {
            response.payload = tao::json::from_string(encoded.body).as<query_response_payload>();
        } catch (const tao::json::pegtl::parse_error&) {
            response.ctx.ec = error::common_errc::parsing_failure;
            return response;
        }
        Expects(response.payload.meta_data.client_context_id.empty() ||
                response.payload.meta_data.client_context_id == request.client_context_id);
        if (response.payload.meta_data.status == "success") {
            if (response.payload.prepared) {
                request.ctx_->cache.put(request.statement, response.payload.prepared.value());
            } else if (request.extract_encoded_plan_) {
                request.extract_encoded_plan_ = false;
                if (response.payload.rows.size() == 1) {
                    tao::json::value row{};
                    try {
                        row = tao::json::from_string(response.payload.rows[0]);
                    } catch (const tao::json::pegtl::parse_error&) {
                        response.ctx.ec = error::common_errc::parsing_failure;
                        return response;
                    }
                    auto* plan = row.find("encoded_plan");
                    auto* name = row.find("name");
                    if (plan != nullptr && name != nullptr) {
                        request.ctx_->cache.put(request.statement, name->get_string(), plan->get_string());
                        throw couchbase::priv::retry_http_request{};
                    }
                    response.ctx.ec = error::query_errc::prepared_statement_failure;

                } else {
                    response.ctx.ec = error::query_errc::prepared_statement_failure;
                }
            }
        } else {
            bool prepared_statement_failure = false;
            bool index_not_found = false;
            bool index_failure = false;
            bool planning_failure = false;
            bool syntax_error = false;
            bool server_timeout = false;
            bool invalid_argument = false;
            bool cas_mismatch = false;

            if (response.payload.meta_data.errors) {
                for (const auto& error : *response.payload.meta_data.errors) {
                    switch (error.code) {
                        case 1065: /* IKey: "service.io.request.unrecognized_parameter" */
                            invalid_argument = true;
                            break;
                        case 1080: /* IKey: "timeout" */
                            server_timeout = true;
                            break;
                        case 3000: /* IKey: "parse.syntax_error" */
                            syntax_error = true;
                            break;
                        case 4040: /* IKey: "plan.build_prepared.no_such_name" */
                        case 4050: /* IKey: "plan.build_prepared.unrecognized_prepared" */
                        case 4060: /* IKey: "plan.build_prepared.no_such_name" */
                        case 4070: /* IKey: "plan.build_prepared.decoding" */
                        case 4080: /* IKey: "plan.build_prepared.name_encoded_plan_mismatch" */
                        case 4090: /* IKey: "plan.build_prepared.name_not_in_encoded_plan" */
                            prepared_statement_failure = true;
                            break;
                        case 12009: /* IKey: "datastore.couchbase.DML_error" */
                            if (error.message.find("CAS mismatch") != std::string::npos) {
                                cas_mismatch = true;
                            }
                            break;
                        case 12004: /* IKey: "datastore.couchbase.primary_idx_not_found" */
                        case 12016: /* IKey: "datastore.couchbase.index_not_found" */
                            index_not_found = true;
                            break;
                        default:
                            if ((error.code >= 12000 && error.code < 13000) || (error.code >= 14000 && error.code < 15000)) {
                                index_failure = true;
                            } else if (error.code >= 4000 && error.code < 5000) {
                                planning_failure = true;
                            }
                            break;
                    }
                }
            }
            if (syntax_error) {
                response.ctx.ec = error::common_errc::parsing_failure;
            } else if (invalid_argument) {
                response.ctx.ec = error::common_errc::invalid_argument;
            } else if (server_timeout) {
                response.ctx.ec = error::common_errc::unambiguous_timeout;
            } else if (prepared_statement_failure) {
                response.ctx.ec = error::query_errc::prepared_statement_failure;
            } else if (index_failure) {
                response.ctx.ec = error::query_errc::index_failure;
            } else if (planning_failure) {
                response.ctx.ec = error::query_errc::planning_failure;
            } else if (index_not_found) {
                response.ctx.ec = error::common_errc::index_not_found;
            } else if (cas_mismatch) {
                response.ctx.ec = error::common_errc::cas_mismatch;
            } else {
                response.ctx.ec = error::common_errc::internal_server_failure;
            }
        }
    }
    return response;
}

} // namespace couchbase::operations
