/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2021 Couchbase, Inc.
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

namespace couchbase::metrics
{

class value_recorder
{
  public:
    value_recorder() = default;
    value_recorder(const value_recorder& other) = default;
    value_recorder(value_recorder&& other) = default;
    value_recorder& operator=(const value_recorder& other) = default;
    value_recorder& operator=(value_recorder&& other) = default;
    virtual ~value_recorder() = default;

    virtual void record_value(std::int64_t value) = 0;
};

class meter
{
  public:
    meter() = default;
    meter(const meter& other) = default;
    meter(meter&& other) = default;
    meter& operator=(const meter& other) = default;
    meter& operator=(meter&& other) = default;
    virtual ~meter() = default;

    virtual value_recorder* get_value_recorder(const std::string& /* name */, const std::map<std::string, std::string>& /* tags */) = 0;
};

} // namespace couchbase::metrics
