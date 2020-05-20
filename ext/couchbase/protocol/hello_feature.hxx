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

#include <spdlog/spdlog.h>

namespace couchbase::protocol
{
enum class hello_feature : uint16_t {
    tls = 0x02,
    tcp_nodelay = 0x03,
    mutation_seqno = 0x04,
    xattr = 0x06,
    xerror = 0x07,
    select_bucket = 0x08,
    snappy = 0x0a,
    json = 0x0b,
    duplex = 0x0c,
    clustermap_change_notification = 0x0d,
    unordered_execution = 0x0e,
    alt_request_support = 0x10,
    sync_replication = 0x11,
    vattr = 0x15,
};

constexpr inline bool
is_valid_hello_feature(uint16_t code)
{
    switch (static_cast<hello_feature>(code)) {
        case hello_feature::tls:
        case hello_feature::tcp_nodelay:
        case hello_feature::mutation_seqno:
        case hello_feature::xattr:
        case hello_feature::xerror:
        case hello_feature::select_bucket:
        case hello_feature::snappy:
        case hello_feature::json:
        case hello_feature::duplex:
        case hello_feature::clustermap_change_notification:
        case hello_feature::unordered_execution:
        case hello_feature::alt_request_support:
        case hello_feature::sync_replication:
        case hello_feature::vattr:
            return true;
    }
    return false;
}

} // namespace couchbase::protocol

template<>
struct fmt::formatter<couchbase::protocol::hello_feature> : formatter<string_view> {
    template<typename FormatContext>
    auto format(couchbase::protocol::hello_feature feature, FormatContext& ctx)
    {
        string_view name = "unknown";
        switch (feature) {
            case couchbase::protocol::hello_feature::tls:
                name = "tls";
                break;
            case couchbase::protocol::hello_feature::tcp_nodelay:
                name = "tcp_nodelay";
                break;
            case couchbase::protocol::hello_feature::mutation_seqno:
                name = "mutation_seqno";
                break;
            case couchbase::protocol::hello_feature::xattr:
                name = "xattr";
                break;
            case couchbase::protocol::hello_feature::xerror:
                name = "xerror";
                break;
            case couchbase::protocol::hello_feature::select_bucket:
                name = "select_bucket";
                break;
            case couchbase::protocol::hello_feature::snappy:
                name = "snappy";
                break;
            case couchbase::protocol::hello_feature::json:
                name = "json";
                break;
            case couchbase::protocol::hello_feature::duplex:
                name = "duplex";
                break;
            case couchbase::protocol::hello_feature::clustermap_change_notification:
                name = "clustermap_change_notification";
                break;
            case couchbase::protocol::hello_feature::unordered_execution:
                name = "unordered_execution";
                break;
            case couchbase::protocol::hello_feature::alt_request_support:
                name = "alt_request_support";
                break;
            case couchbase::protocol::hello_feature::sync_replication:
                name = "sync_replication";
                break;
            case couchbase::protocol::hello_feature::vattr:
                name = "vattr";
                break;
        }
        return formatter<string_view>::format(name, ctx);
    }
};
