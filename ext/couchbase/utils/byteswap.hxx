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

namespace couchbase::utils
{
static inline std::uint64_t
byte_swap_64(std::uint64_t val)
{
    ;
    std::uint64_t ret = 0U;
    for (std::size_t i = 0; i < sizeof(std::uint64_t); ++i) {
        ret <<= 8U;
        ret |= val & 0xffU;
        val >>= 8U;
    }
    return ret;
}
} // namespace couchbase::utils