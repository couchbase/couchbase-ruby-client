/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include <string>
#include <vector>

namespace couchbase::base64
{

/**
 * Base64 encode data
 *
 * @param source the string to encode
 * @return the base64 encoded value
 */
std::string
encode(std::string_view blob, bool prettyprint = false);

/**
 * Decode a base64 encoded blob (which may be pretty-printed to avoid
 * super-long lines)
 *
 * @param source string to decode
 * @return the decoded data
 */
std::string
decode(std::string_view blob);

} // namespace couchbase::base64
