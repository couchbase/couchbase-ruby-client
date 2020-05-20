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

namespace couchbase
{
struct error_map {
    struct error_info {
        std::uint16_t code;
        std::string name;
        std::string description;
        std::set<std::string> attributes;
    };
    uuid::uuid_t id;
    uint16_t version;
    uint16_t revision;
    std::map<std::uint16_t, error_info> errors;
};
} // namespace couchbase

namespace tao::json
{
template<>
struct traits<couchbase::error_map> {
    template<template<typename...> class Traits>
    static couchbase::error_map as(const tao::json::basic_value<Traits>& v)
    {
        couchbase::error_map result;
        result.id = couchbase::uuid::random();
        result.version = v.at("revision").template as<std::uint16_t>();
        result.revision = v.at("revision").template as<std::uint16_t>();
        for (const auto& j : v.at("errors").get_object()) {
            couchbase::error_map::error_info ei;
            ei.code = gsl::narrow_cast<std::uint16_t>(std::stoul(j.first, 0, 16));
            const auto& info = j.second.get_object();
            ei.name = info.at("name").get_string();
            ei.description = info.at("desc").get_string();
            for (const auto& a : info.at("attrs").get_array()) {
                ei.attributes.insert(a.get_string());
            }
            result.errors.emplace(ei.code, ei);
        }
        return result;
    }
};
} // namespace tao::json
