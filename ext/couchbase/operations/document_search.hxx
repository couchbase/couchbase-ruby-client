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

#include <tao/json.hpp>

#include <version.hxx>
#include <error_context/search.hxx>

namespace couchbase::operations
{
struct search_response {
    struct search_metrics {
        std::chrono::nanoseconds took;
        std::uint64_t total_rows;
        double max_score;
        std::uint64_t success_partition_count;
        std::uint64_t error_partition_count;
    };

    struct search_meta_data {
        std::string client_context_id;
        search_metrics metrics;
        std::map<std::string, std::string> errors;
    };

    struct search_location {
        std::string field;
        std::string term;
        std::uint64_t position;
        std::uint64_t start_offset;
        std::uint64_t end_offset;
        std::optional<std::vector<std::uint64_t>> array_positions{};
    };

    struct search_row {
        std::string index;
        std::string id;
        double score;
        std::vector<search_location> locations{};
        std::map<std::string, std::vector<std::string>> fragments{};
        std::string fields{};
        std::string explanation{};
    };

    struct search_facet {
        struct term_facet {
            std::string term{};
            std::uint64_t count{};
        };

        struct date_range_facet {
            std::string name{};
            std::uint64_t count{};
            std::optional<std::string> start{};
            std::optional<std::string> end{};
        };

        struct numeric_range_facet {
            std::string name{};
            std::uint64_t count{};
            std::variant<std::monostate, std::uint64_t, double> min{};
            std::variant<std::monostate, std::uint64_t, double> max{};
        };

        std::string name;
        std::string field;
        std::uint64_t total;
        std::uint64_t missing;
        std::uint64_t other;
        std::vector<term_facet> terms{};
        std::vector<date_range_facet> date_ranges{};
        std::vector<numeric_range_facet> numeric_ranges{};
    };

    error_context::search ctx;
    std::string status{};
    search_meta_data meta_data{};
    std::string error{};
    std::vector<search_row> rows{};
    std::vector<search_facet> facets{};
};

struct search_request {
    using response_type = search_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;
    using error_context_type = error_context::search;

    static const inline service_type type = service_type::search;

    std::string client_context_id{ uuid::to_string(uuid::random()) };
    std::chrono::milliseconds timeout{ timeout_defaults::management_timeout };

    std::string index_name;
    tao::json::value query;

    std::optional<std::uint32_t> limit{};
    std::optional<std::uint32_t> skip{};
    bool explain{ false };
    bool disable_scoring{ false };

    enum class highlight_style_type { html, ansi };
    std::optional<highlight_style_type> highlight_style{};
    std::vector<std::string> highlight_fields{};
    std::vector<std::string> fields{};
    std::optional<std::string> scope_name{};
    std::vector<std::string> collections{};

    enum class scan_consistency_type { not_bounded };
    std::optional<scan_consistency_type> scan_consistency{};
    std::vector<mutation_token> mutation_state{};

    std::vector<std::string> sort_specs{};

    std::map<std::string, std::string> facets{};

    std::map<std::string, tao::json::value> raw{};
    std::string body_str{};

    [[nodiscard]] std::error_code encode_to(encoded_request_type& encoded, http_context& context)
    {
        tao::json::value body = tao::json::value{
            { "query", query },
            { "explain", explain },
            { "ctl", { { "timeout", timeout.count() } } },
        };
        if (limit) {
            body["size"] = *limit;
        }
        if (skip) {
            body["from"] = *skip;
        }
        if (disable_scoring) {
            body["score"] = "none";
        }
        if (highlight_style || !highlight_fields.empty()) {
            tao::json::value highlight;
            if (highlight_style) {
                switch (*highlight_style) {
                    case highlight_style_type::html:
                        highlight["style"] = "html";
                        break;
                    case highlight_style_type::ansi:
                        highlight["style"] = "ansi";
                        break;
                }
            }
            if (!highlight_fields.empty()) {
                highlight["fields"] = highlight_fields;
            }
            body["highlight"] = highlight;
        }
        if (!fields.empty()) {
            body["fields"] = fields;
        }
        if (!sort_specs.empty()) {
            body["sort"] = tao::json::empty_array;
            for (const auto& spec : sort_specs) {
                body["sort"].get_array().push_back(tao::json::from_string(spec));
            }
        }
        if (!facets.empty()) {
            body["facets"] = tao::json::empty_object;
            for (const auto& [name, facet] : facets) {
                body["facets"][name] = tao::json::from_string(facet);
            }
        }
        if (!mutation_state.empty()) {
            tao::json::value scan_vectors = tao::json::empty_object;
            for (const auto& token : mutation_state) {
                auto key = fmt::format("{}/{}", token.partition_id, token.partition_uuid);
                auto* old_val = scan_vectors.find(key);
                if (old_val == nullptr || (old_val->is_integer() && old_val->get_unsigned() < token.sequence_number)) {
                    scan_vectors[key] = token.sequence_number;
                }
            }
            body["ctl"]["consistency"] = tao::json::value{
                { "level", "at_plus" },
                { "vectors", { { index_name, scan_vectors } } },
            };
        }
        if (scope_name) {
            body["scope"] = scope_name.value();
            body["collections"] = collections;
        }

        encoded.type = type;
        encoded.headers["content-type"] = "application/json";
        encoded.method = "POST";
        encoded.path = fmt::format("/api/index/{}/query", index_name);
        body_str = tao::json::to_string(body);
        encoded.body = body_str;
        if (context.options.show_queries) {
            spdlog::info("SEARCH: {}", tao::json::to_string(body["query"]));
        } else {
            spdlog::debug("SEARCH: {}", tao::json::to_string(body["query"]));
        }
        return {};
    }
};

search_response
make_response(error_context::search&& ctx, const search_request& request, search_request::encoded_response_type&& encoded)
{
    search_response response{ std::move(ctx) };
    response.meta_data.client_context_id = request.client_context_id;
    response.ctx.index_name = request.index_name;
    response.ctx.query = tao::json::to_string(request.query);
    response.ctx.parameters = request.body_str;
    if (!response.ctx.ec) {
        if (encoded.status_code == 200) {
            tao::json::value payload{};
            try {
                payload = tao::json::from_string(encoded.body);
            } catch (const tao::pegtl::parse_error& e) {
                response.ctx.ec = error::common_errc::parsing_failure;
                return response;
            }
            response.meta_data.metrics.took = std::chrono::nanoseconds(payload.at("took").get_unsigned());
            response.meta_data.metrics.max_score = payload.at("max_score").as<double>();
            response.meta_data.metrics.total_rows = payload.at("total_hits").get_unsigned();

            if (auto& status_prop = payload.at("status"); status_prop.is_string()) {
                response.status = status_prop.get_string();
                if (response.status == "ok") {
                    return response;
                }
            } else if (status_prop.is_object()) {
                response.meta_data.metrics.error_partition_count = status_prop.at("failed").get_unsigned();
                response.meta_data.metrics.success_partition_count = status_prop.at("successful").get_unsigned();
                if (const auto* errors = status_prop.find("errors"); errors != nullptr && errors->is_object()) {
                    for (const auto& [location, message] : errors->get_object()) {
                        response.meta_data.errors.try_emplace(location, message.get_string());
                    }
                }
            } else {
                response.ctx.ec = error::common_errc::internal_server_failure;
                return response;
            }

            if (const auto* rows = payload.find("hits"); rows != nullptr && rows->is_array()) {
                for (const auto& entry : rows->get_array()) {
                    search_response::search_row row{};
                    row.index = entry.at("index").get_string();
                    row.id = entry.at("id").get_string();
                    row.score = entry.at("score").as<double>();
                    if (const auto* locations_map = entry.find("locations"); locations_map != nullptr && locations_map->is_object()) {
                        for (const auto& [field, terms] : locations_map->get_object()) {
                            for (const auto& [term, locations] : terms.get_object()) {
                                for (const auto& loc : locations.get_array()) {
                                    search_response::search_location location{};
                                    location.field = field;
                                    location.term = term;
                                    location.position = loc.at("pos").get_unsigned();
                                    location.start_offset = loc.at("start").get_unsigned();
                                    location.end_offset = loc.at("end").get_unsigned();
                                    if (const auto* ap = loc.find("array_positions"); ap != nullptr && ap->is_array()) {
                                        location.array_positions.emplace(ap->as<std::vector<std::uint64_t>>());
                                    }
                                    row.locations.emplace_back(location);
                                }
                            }
                        }
                    }

                    if (const auto* fragments_map = entry.find("fragments"); fragments_map != nullptr && fragments_map->is_object()) {
                        for (const auto& [field, fragments] : fragments_map->get_object()) {
                            row.fragments.emplace(field, fragments.as<std::vector<std::string>>());
                        }
                    }
                    if (const auto* fields = entry.find("fields"); fields != nullptr && fields->is_object()) {
                        row.fields = tao::json::to_string(*fields);
                    }
                    if (const auto* explanation = entry.find("explanation"); explanation != nullptr && explanation->is_object()) {
                        row.explanation = tao::json::to_string(*explanation);
                    }
                    response.rows.emplace_back(row);
                }
            }

            if (const auto* facets = payload.find("facets"); facets != nullptr && facets->is_object()) {
                for (const auto& [name, object] : facets->get_object()) {
                    search_response::search_facet facet;
                    facet.name = name;
                    facet.field = object.at("field").get_string();
                    facet.total = object.at("total").get_unsigned();
                    facet.missing = object.at("missing").get_unsigned();
                    facet.other = object.at("other").get_unsigned();

                    if (const auto* date_ranges = object.find("date_ranges"); date_ranges != nullptr && date_ranges->is_array()) {
                        for (const auto& date_range : date_ranges->get_array()) {
                            search_response::search_facet::date_range_facet drf;
                            drf.name = date_range.at("name").get_string();
                            drf.count = date_range.at("count").get_unsigned();
                            if (const auto* start = date_range.find("start"); start != nullptr && start->is_string()) {
                                drf.start = start->get_string();
                            }
                            if (const auto* end = date_range.find("end"); end != nullptr && end->is_string()) {
                                drf.end = end->get_string();
                            }
                            facet.date_ranges.emplace_back(drf);
                        }
                    }

                    if (const auto& numeric_ranges = object.find("numeric_ranges");
                        numeric_ranges != nullptr && numeric_ranges->is_array()) {
                        for (const auto& numeric_range : numeric_ranges->get_array()) {
                            search_response::search_facet::numeric_range_facet nrf;
                            nrf.name = numeric_range.at("name").get_string();
                            nrf.count = numeric_range.at("count").get_unsigned();
                            if (const auto* min = numeric_range.find("min"); min != nullptr) {
                                if (min->is_double()) {
                                    nrf.min = min->as<double>();
                                } else if (min->is_integer()) {
                                    nrf.min = min->get_unsigned();
                                }
                            }
                            if (const auto* max = numeric_range.find("max"); max != nullptr) {
                                if (max->is_double()) {
                                    nrf.max = max->as<double>();
                                } else if (max->is_integer()) {
                                    nrf.max = max->get_unsigned();
                                }
                            }
                            facet.numeric_ranges.emplace_back(nrf);
                        }
                    }

                    if (const auto* terms = object.find("terms"); terms != nullptr && terms->is_array()) {
                        for (const auto& term : terms->get_array()) {
                            search_response::search_facet::term_facet tf;
                            tf.term = term.at("term").get_string();
                            tf.count = term.at("count").get_unsigned();
                            facet.terms.emplace_back(tf);
                        }
                    }

                    response.facets.emplace_back(facet);
                }
            }
            return response;
        }
        if (encoded.status_code == 400) {
            tao::json::value payload{};
            try {
                payload = tao::json::from_string(encoded.body);
            } catch (const tao::pegtl::parse_error& e) {
                response.ctx.ec = error::common_errc::parsing_failure;
                return response;
            }
            response.status = payload.at("status").get_string();
            response.error = payload.at("error").get_string();
            if (response.error.find("index not found") != std::string::npos) {
                response.ctx.ec = error::common_errc::index_not_found;
                return response;
            }
            if (response.error.find("no planPIndexes for indexName") != std::string::npos) {
                response.ctx.ec = error::search_errc::index_not_ready;
                return response;
            }
            if (response.error.find("pindex_consistency mismatched partition") != std::string::npos) {
                response.ctx.ec = error::search_errc::consistency_mismatch;
                return response;
            }
        }
        response.ctx.ec = error::common_errc::internal_server_failure;
    }
    return response;
}

} // namespace couchbase::operations
