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

namespace couchbase
{
enum class service_type {
    key_value = 0,
    query,
    analytics,
    search,
    view,
    management,
};
} // namespace couchbase

template<>
struct fmt::formatter<couchbase::service_type> : formatter<std::string_view> {
    template<typename FormatContext>
    auto format(couchbase::service_type type, FormatContext& ctx)
    {
        string_view name = "unknown";
        switch (type) {
            case couchbase::service_type::key_value:
                name = "kv";
                break;
            case couchbase::service_type::query:
                name = "query";
                break;
            case couchbase::service_type::analytics:
                name = "analytics";
                break;
            case couchbase::service_type::search:
                name = "search";
                break;
            case couchbase::service_type::view:
                name = "views";
                break;
            case couchbase::service_type::management:
                name = "mgmt";
                break;
        }
        return formatter<string_view>::format(name, ctx);
    }
};
