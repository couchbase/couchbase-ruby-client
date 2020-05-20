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

#include <functional>

#include <asio.hpp>

#include <configuration.hxx>

namespace couchbase
{
class configuration_monitor
{
  public:
    using listener_type = std::function<void(const configuration& conf)>;

    explicit configuration_monitor(asio::io_context& ctx)
      : strand_(ctx.get_executor())
    {
    }

    void post(configuration&& conf)
    {
        asio::post(asio::bind_executor(strand_, [this, conf = std::move(conf)]() {
            for (auto& listener : listeners_) {
                listener.second(conf);
            }
        }));
    }

    void post(const std::string& bucket_name, configuration&& conf)
    {
        auto& listeners = bucket_listeners_[bucket_name];
        if (listeners.empty()) {
            return;
        }
        asio::post(asio::bind_executor(strand_, [&listeners, conf = std::move(conf)]() {
            for (auto& listener : listeners) {
                listener.second(conf);
            }
        }));
    }

    std::size_t subscribe(listener_type&& listener)
    {
        auto token = next_token_++;
        listeners_[token] = listener;
        return token;
    }

    std::size_t subscribe(const std::string& bucket_name, listener_type&& listener)
    {
        auto token = next_token_++;
        bucket_listeners_[bucket_name][token] = listener;
        return token;
    }

    void unsubscribe(size_t token)
    {
        listeners_.erase(token);
    }

    void unsubscribe(const std::string& bucket_name, size_t token)
    {
        auto b = bucket_listeners_.find(bucket_name);
        if (b != bucket_listeners_.end()) {
            b->second.erase(token);
        }
    }

  private:
    asio::strand<asio::io_context::executor_type> strand_;
    std::map<size_t, listener_type> listeners_;
    std::map<std::string, std::map<size_t, listener_type>> bucket_listeners_;
    std::size_t next_token_{ 0 };
};
} // namespace couchbase