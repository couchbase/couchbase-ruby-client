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

#include <utility>

#include <configuration_monitor.hxx>
#include <operations.hxx>

namespace couchbase
{
class bucket
{
  public:
    explicit bucket(asio::io_context& ctx, std::string name, configuration config)
      : ctx_(ctx)
      , name_(std::move(name))
      , config_(std::move(config))
    {
    }

    ~bucket()
    {
        close();
    }

    [[nodiscard]] const std::string& name() const
    {
        return name_;
    }

    void set_node(size_t index, std::shared_ptr<io::mcbp_session> session)
    {
        if (closed_) {
            return;
        }

        sessions_.emplace(index, std::move(session));
    }

    void remove_node(size_t index)
    {
        if (closed_) {
            return;
        }
        sessions_.erase(index);
    }

    template<typename Request, typename Handler>
    void execute(Request request, Handler&& handler)
    {
        if (closed_) {
            return;
        }
        size_t index = 0;
        std::tie(request.partition, index) = config_.map_key(request.id.key);
        auto session = sessions_.at(index);
        auto cmd = std::make_shared<operations::command<Request>>(ctx_, std::move(request));
        cmd->send_to(session, std::forward<Handler>(handler));
    }

    void close()
    {
        if (closed_) {
            return;
        }
        closed_ = true;
        for (auto& session : sessions_) {
            session.second->stop();
        }
    }

  private:
    asio::io_context& ctx_;
    std::string name_;
    configuration config_;
    bool closed_{ false };
    std::map<size_t, std::shared_ptr<io::mcbp_session>> sessions_{};
};
} // namespace couchbase
