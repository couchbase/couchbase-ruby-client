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

#include <service_type.hxx>

namespace couchbase::io
{
struct http_request {
    service_type type;
    std::string method;
    std::string path;
    std::map<std::string, std::string> headers;
    std::string body;
};

struct http_response {
    uint32_t status_code;
    std::string status_message;
    std::map<std::string, std::string> headers;
    std::string body;
};
} // namespace couchbase::io
