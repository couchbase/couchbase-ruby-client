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

#include <spdlog/spdlog.h>

namespace couchbase::protocol
{
enum class hello_feature : uint16_t {
    /**
     * The client wants to TLS and send STARTTLS
     */
    tls = 0x02,

    /**
     * The client requests the server to set TCP NODELAY on the socket used by this connection.
     */
    tcp_nodelay = 0x03,

    /**
     * The client requests the server to add the sequence number for a mutation to the response packet used in mutations.
     */
    mutation_seqno = 0x04,

    /**
     * The client requests the server to set TCP DELAY on the socket used by this connection
     */
    tcp_delay = 0x05,

    /**
     * The client requests the server to add XATTRs to the stream for commands where it makes sense (GetWithMeta, SetWithMeta, DcpMutation
     * etc)
     */
    xattr = 0x06,

    /**
     * The client requests the server to send extended error codes instead of disconnecting the client when new errors occur (note that some
     * errors may be remapped to more generic error codes instead of disconnecting)
     */
    xerror = 0x07,

    /**
     * This is purely informational (it does not enable/disable anything on the server). It may be used from the client to know if it should
     * be able to run select bucket or not (select bucket was a privileged command pre-spock. In spock all users may run select bucket, but
     * only to a bucket they have access to).
     */
    select_bucket = 0x08,

    /**
     * The client wants to enable support for Snappy compression. A client with support for Snappy compression must update the datatype
     * filed in the requests with the bit representing SNAPPY when sending snappy compressed data to the server. It must be able to receive
     * data from the server compressed with SNAPPY identified by the bit being set in the datatype field
     */
    snappy = 0x0a,

    /**
     * The client wants to enable support for JSON. The client must set this bit when storing JSON documents on the server. The server will
     * set the appropriate bit in the datatype field when returning such documents to the client
     */
    json = 0x0b,

    /**
     * The client allows for full duplex on the socket. This means that the server may send requests back to the client. These messages is
     * identified by the magic values of 0x82 (request) and 0x83 (response). See the document Duplex for more information.
     *
     * https://github.com/couchbase/kv_engine/blob/master/docs/Duplex.md
     */
    duplex = 0x0c,

    /**
     * The client wants the server to notify the client with new cluster maps whenever ns_server push them to memcached. (note that this
     * notification is subject to deduplication of the vbucket map received as part of not my vbucket)
     */
    clustermap_change_notification = 0x0d,

    /**
     * The client allows the server to reorder the execution of commands. See the document UnorderedExecution for more information
     *
     * https://github.com/couchbase/kv_engine/blob/master/docs/UnorderedExecution.md
     */
    unordered_execution = 0x0e,

    /**
     * The client wants the server to include tracing information in the response packet
     */
    tracing = 0x0f,

    /**
     * This is purely informational (it does not enable/disable anything on the server). It may be used from the client to know if it may
     * send the alternative request packet (magic 0x08) containing FrameInfo segments.
     */
    alt_request_support = 0x10,

    /**
     * This is purely informational (it does not enable/disable anything on the server). It may be used from the client to know if it may
     * use synchronous replication tags in the mutation requests.
     */
    sync_replication = 0x11,

    /**
     * The client wants to enable support for Collections
     */
    collections = 0x12,

    /**
     * This is purely informational (it does not enable / disable anything on the server). It may be used from the client to figure out if
     * the server supports OpenTracing or not.)
     */
    open_tracing = 0x13,

    /**
     * This is purely informational (it does not enable / disable anything on the server). It may be used from the client to know if it may
     * use PreserveTtl in the operations who carries the TTL for a document
     */
    preserve_ttl = 0x14,

    /**
     * This is purely information (it does not enable / disable anything on the server). It may be used from the client to determine if the
     * server supports VATTRs in a generic way (can request $ and will either succeed or fail with SubdocXattrUnknownVattr). Requires XATTR.
     */
    vattr = 0x15,

    /**
     * This is purely information (it does not enable / disable anything on the server). It may be used from the client to determine if the
     * server supports Point in Time Recovery
     */
    point_in_time_recovery = 0x16,

    /**
     * Does the server support the subdoc mutation flag create_as_deleted
     */
    subdoc_create_as_deleted = 0x17,

    /**
     * Does the server support using the virtual $document attributes in macro expansion ("${document.CAS}" etc)
     */
    subdoc_document_macro_support = 0x18,
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
        case hello_feature::collections:
        case hello_feature::open_tracing:
        case hello_feature::preserve_ttl:
        case hello_feature::point_in_time_recovery:
        case hello_feature::tcp_delay:
        case hello_feature::tracing:
        case hello_feature::subdoc_create_as_deleted:
        case hello_feature::subdoc_document_macro_support:
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
            case couchbase::protocol::hello_feature::collections:
                name = "collections";
                break;
            case couchbase::protocol::hello_feature::open_tracing:
                name = "open_tracing";
                break;
            case couchbase::protocol::hello_feature::preserve_ttl:
                name = "preserve_ttl";
                break;
            case couchbase::protocol::hello_feature::point_in_time_recovery:
                name = "point_in_time_recovery";
                break;
            case couchbase::protocol::hello_feature::tcp_delay:
                name = "tcp_delay";
                break;
            case couchbase::protocol::hello_feature::tracing:
                name = "tracing";
                break;
            case couchbase::protocol::hello_feature::subdoc_create_as_deleted:
                name = "subdoc_create_as_deleted";
                break;
            case couchbase::protocol::hello_feature::subdoc_document_macro_support:
                name = "subdoc_document_macro_support";
                break;
        }
        return formatter<string_view>::format(name, ctx);
    }
};
