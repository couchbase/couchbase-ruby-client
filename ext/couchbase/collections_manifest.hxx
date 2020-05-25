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
struct collections_manifest {
    struct collection {
        std::uint64_t uid;
        std::string name;
    };

    struct scope {
        std::uint64_t uid;
        std::string name;
        std::vector<collection> collections;
    };

    uuid::uuid_t id;
    std::uint64_t uid;
    std::vector<scope> scopes;
};
} // namespace couchbase

template<>
struct fmt::formatter<couchbase::collections_manifest> : formatter<std::string> {
    template<typename FormatContext>
    auto format(const couchbase::collections_manifest& manifest, FormatContext& ctx)
    {
        std::vector<std::string> collections;
        for (const auto& scope : manifest.scopes) {
            for (const auto& collection : scope.collections) {
                collections.emplace_back(fmt::format("{}.{}={}", scope.name, collection.name, collection.uid));
            }
        }

        format_to(ctx.out(),
                  R"(#<manifest:{} uid={}, collections({})=[{}]>)",
                  couchbase::uuid::to_string(manifest.id),
                  manifest.uid,
                  collections.size(),
                  fmt::join(collections, ", "));
        return formatter<std::string>::format("", ctx);
    }
};

namespace tao::json
{
template<>
struct traits<couchbase::collections_manifest> {
    template<template<typename...> class Traits>
    static couchbase::collections_manifest as(const tao::json::basic_value<Traits>& v)
    {
        (void)v;
        couchbase::collections_manifest result;
        result.id = couchbase::uuid::random();
        result.uid = std::stoull(v.at("uid").get_string(), 0, 16);
        for (const auto& s : v.at("scopes").get_array()) {
            couchbase::collections_manifest::scope scope;
            scope.uid = std::stoull(s.at("uid").get_string(), 0, 16);
            scope.name = s.at("name").get_string();
            for (const auto& c : s.at("collections").get_array()) {
                couchbase::collections_manifest::collection collection;
                collection.uid = std::stoull(c.at("uid").get_string(), 0, 16);
                collection.name = c.at("name").get_string();
                scope.collections.emplace_back(collection);
            }
            result.scopes.emplace_back(scope);
        }
        return result;
    }
};
} // namespace tao::json
