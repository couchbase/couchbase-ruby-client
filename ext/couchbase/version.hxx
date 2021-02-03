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

#include <build_version.hxx>

#include <string>

namespace couchbase
{
constexpr auto BACKEND_VERSION_MAJOR = 1;
constexpr auto BACKEND_VERSION_MINOR = 3;
constexpr auto BACKEND_VERSION_PATCH = 1;

inline const std::string&
sdk_id()
{
    static const std::string identifier{ std::string("ruby/") + std::to_string(BACKEND_VERSION_MAJOR) + "/" +
                                         std::to_string(BACKEND_VERSION_MINOR) + "/" + std::to_string(BACKEND_VERSION_PATCH) + "/" +
                                         BACKEND_GIT_REVISION };
    return identifier;
}
} // namespace couchbase
