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
struct mutation_token {
    uint64_t partition_uuid{ 0 };
    uint64_t sequence_number{ 0 };
    uint16_t partition_id{ 0 };
    std::string bucket_name{};
};
} // namespace couchbase

template<>
struct fmt::formatter<couchbase::mutation_token> : formatter<std::string> {
    template<typename FormatContext>
    auto format(const couchbase::mutation_token& token, FormatContext& ctx)
    {
        format_to(ctx.out(), "{}:{}:{}:{}", token.bucket_name, token.partition_id, token.partition_uuid, token.sequence_number);
        return formatter<std::string>::format("", ctx);
    }
};
