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

#include <spdlog/fmt/fmt.h>

namespace couchbase
{
enum class bucket_capability {
    couchapi,
    xattr,
    dcp,
    cbhello,
    touch,
    cccp,
    xdcr_checkpointing,
    nodes_ext,
    collections,
    durable_write,
    tombstoned_user_xattrs,
};

enum class cluster_capability {
    n1ql_cost_based_optimizer,
    n1ql_index_advisor,
    n1ql_javascript_functions,
    n1ql_inline_functions,
    n1ql_enhanced_prepared_statements,
};
} // namespace couchbase

template<>
struct fmt::formatter<couchbase::bucket_capability> : formatter<std::string_view> {
    template<typename FormatContext>
    auto format(couchbase::bucket_capability type, FormatContext& ctx)
    {
        string_view name = "unknown";
        switch (type) {
            case couchbase::bucket_capability::couchapi:
                name = "couchapi";
                break;
            case couchbase::bucket_capability::xattr:
                name = "xattr";
                break;
            case couchbase::bucket_capability::dcp:
                name = "dcp";
                break;
            case couchbase::bucket_capability::cbhello:
                name = "cbhello";
                break;
            case couchbase::bucket_capability::touch:
                name = "touch";
                break;
            case couchbase::bucket_capability::cccp:
                name = "cccp";
                break;
            case couchbase::bucket_capability::xdcr_checkpointing:
                name = "xdcr_checkpointing";
                break;
            case couchbase::bucket_capability::nodes_ext:
                name = "nodes_ext";
                break;
            case couchbase::bucket_capability::collections:
                name = "collections";
                break;
            case couchbase::bucket_capability::durable_write:
                name = "durable_write";
                break;
            case couchbase::bucket_capability::tombstoned_user_xattrs:
                name = "tombstoned_user_xattrs";
                break;
        }
        return formatter<string_view>::format(name, ctx);
    }
};

template<>
struct fmt::formatter<couchbase::cluster_capability> : formatter<std::string_view> {
    template<typename FormatContext>
    auto format(couchbase::cluster_capability type, FormatContext& ctx)
    {
        string_view name = "unknown";
        switch (type) {
            case couchbase::cluster_capability::n1ql_cost_based_optimizer:
                name = "n1ql_cost_based_optimizer";
                break;
            case couchbase::cluster_capability::n1ql_index_advisor:
                name = "n1ql_index_advisor";
                break;
            case couchbase::cluster_capability::n1ql_javascript_functions:
                name = "n1ql_javascript_functions";
                break;
            case couchbase::cluster_capability::n1ql_inline_functions:
                name = "n1ql_inline_functions";
                break;
            case couchbase::cluster_capability::n1ql_enhanced_prepared_statements:
                name = "n1ql_enhanced_prepared_statements";
                break;
        }
        return formatter<string_view>::format(name, ctx);
    }
};
