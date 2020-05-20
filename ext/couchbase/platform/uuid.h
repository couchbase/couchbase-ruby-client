/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include <array>
#include <cstdint>
#include <string>

namespace couchbase::uuid
{

using uuid_t = std::array<uint8_t, 16>;

/**
 * Get a random uuid (version 4 of the uuids)
 */
void
random(uuid_t& uuid);

/**
 * Generate a new random uuid and return it
 */
uuid_t
random();

/**
 * Convert a textual version of a UUID to a uuid type
 * @throw std::invalid_argument if the textual uuid is not
 *        formatted correctly
 */
uuid_t
from_string(std::string_view str);

/**
 * Print a textual version of the UUID in the form:
 *
 *     00000000-0000-0000-0000-000000000000
 */
std::string
to_string(const couchbase::uuid::uuid_t& uuid);

} // namespace couchbase::uuid
