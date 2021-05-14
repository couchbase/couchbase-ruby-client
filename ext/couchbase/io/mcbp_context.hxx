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

#include <vector>
#include <configuration.hxx>
#include <protocol/hello_feature.hxx>

namespace couchbase
{

struct mcbp_context {
    const std::optional<configuration>& config;
    const std::vector<protocol::hello_feature>& supported_features;

    [[nodiscard]] bool supports_feature(protocol::hello_feature feature) const
    {
        return std::find(supported_features.begin(), supported_features.end(), feature) != supported_features.end();
    }
};

} // namespace couchbase
