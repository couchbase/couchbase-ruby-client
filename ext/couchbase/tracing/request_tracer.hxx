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

#include "constants.hxx"

namespace couchbase::tracing
{

class request_span
{
  public:
    request_span() = default;
    request_span(const request_span& other) = default;
    request_span(request_span&& other) = default;
    request_span& operator=(const request_span& other) = default;
    request_span& operator=(request_span&& other) = default;
    virtual ~request_span() = default;

    explicit request_span(const std::string& name)
      : name_(name)
      , parent_(nullptr)
    {
    }
    request_span(std::string name, request_span* parent)
      : name_(std::move(name))
      , parent_(parent)
    {
    }
    virtual void add_tag(const std::string& name, std::uint64_t value) = 0;
    virtual void add_tag(const std::string& name, const std::string& value) = 0;
    virtual void end() = 0;

    [[nodiscard]] const std::string& name() const
    {
        return name_;
    }

    [[nodiscard]] const request_span* parent() const
    {
        return parent_;
    }

  private:
    std::string name_{};
    request_span* parent_{ nullptr };
};

class request_tracer
{
  public:
    request_tracer() = default;
    request_tracer(const request_tracer& other) = default;
    request_tracer(request_tracer&& other) = default;
    request_tracer& operator=(const request_tracer& other) = default;
    request_tracer& operator=(request_tracer&& other) = default;
    virtual ~request_tracer() = default;

    virtual request_span* start_span(std::string name, request_span* parent) = 0;
};

} // namespace couchbase::tracing
