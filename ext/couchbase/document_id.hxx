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
struct document_id {
    std::string bucket;
    std::string collection;
    std::string key;
    std::optional<std::uint32_t> collection_uid{}; // filled with resolved UID during request lifetime
    bool use_collections{ true };
    bool use_any_session{ false };
};
} // namespace couchbase

template<>
struct fmt::formatter<couchbase::document_id> : formatter<std::string> {
    template<typename FormatContext>
    auto format(const couchbase::document_id& id, FormatContext& ctx)
    {
        format_to(ctx.out(), "{}/{}/{}", id.bucket, id.collection, id.key);
        return formatter<std::string>::format("", ctx);
    }
};
