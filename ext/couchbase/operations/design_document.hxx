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
struct design_document {
    enum class name_space {
        development,
        production,
    };

    struct view {
        std::string name;
        std::optional<std::string> map;
        std::optional<std::string> reduce;
    };

    std::string rev;
    std::string name;
    design_document::name_space ns;
    std::map<std::string, view> views;
};

} // namespace couchbase::operations

template<>
struct fmt::formatter<couchbase::operations::design_document::name_space> : formatter<string_view> {
    template<typename FormatContext>
    auto format(couchbase::operations::design_document::name_space ns, FormatContext& ctx)
    {
        string_view name = "unknown";
        switch (ns) {
            case couchbase::operations::design_document::name_space::development:
                name = "development";
                break;

            case couchbase::operations::design_document::name_space::production:
                name = "production";
                break;
        }
        return formatter<string_view>::format(name, ctx);
    }
};
