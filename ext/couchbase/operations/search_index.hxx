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

namespace couchbase::operations
{
struct search_index {
    std::string uuid;
    std::string name;
    std::string type;
    std::string params_json;

    std::string source_uuid;
    std::string source_name;
    std::string source_type;
    std::string source_params_json;

    std::string plan_params_json;
};

} // namespace couchbase::operations

namespace tao::json
{
template<>
struct traits<couchbase::operations::search_index> {
    template<template<typename...> class Traits>
    static couchbase::operations::search_index as(const tao::json::basic_value<Traits>& v)
    {
        couchbase::operations::search_index result;
        result.uuid = v.at("uuid").get_string();
        result.name = v.at("name").get_string();
        result.type = v.at("type").get_string();
        {
            const auto* params = v.find("params");
            if (params != nullptr && params->is_object()) {
                result.params_json = tao::json::to_string(*params);
            }
        }
        if (v.find("sourceUUID") != nullptr) {
            result.source_uuid = v.at("sourceUUID").get_string();
        }
        if (v.find("sourceName") != nullptr) {
            result.source_name = v.at("sourceName").get_string();
        }
        if (v.find("sourceType") != nullptr) {
            result.source_type = v.at("sourceType").get_string();
        }
        {
            const auto* params = v.find("sourceParams");
            if (params != nullptr && params->is_object()) {
                result.source_params_json = tao::json::to_string(*params);
            }
        }
        {
            const auto* params = v.find("planParams");
            if (params != nullptr && params->is_object()) {
                result.plan_params_json = tao::json::to_string(*params);
            }
        }
        return result;
    }
};
} // namespace tao::json
