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

        auto session_manifest = session->manifest();
        if (!manifest_cache_) {
            if (session_manifest) {
                manifest_cache_ = session_manifest.value();
            }
        } else {
            if (session_manifest && session_manifest->uid > manifest_cache_->uid) {
                manifest_cache_ = session->manifest().value();
            }
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
        if (manifest_cache_) {
            request.id.collection_uid = get_collection_uid(request.id.collection);
        } else {
            if (!request.id.collection.empty() && request.id.collection != "_default._default") {
                handler(make_response(std::make_error_code(error::common_errc::unsupported_operation), request, {}));
                return;
            }
        }
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
    [[nodiscard]] uint32_t get_collection_uid(const std::string& collection_path)
    {
        Expects(manifest_cache_.has_value());
        Expects(!collection_path.empty());
        auto dot = collection_path.find('.');
        Expects(dot != std::string::npos);
        std::string scope = collection_path.substr(0, dot);
        std::string collection = collection_path.substr(dot + 1);
        for (const auto& s : manifest_cache_->scopes) {
            if (s.name == scope) {
                for (const auto& c : s.collections) {
                    if (c.name == collection) {
                        return gsl::narrow_cast<std::uint32_t>(c.uid);
                    }
                }
            }
        }
        Ensures(false); // FIXME: return collection not found
        return 0;
    }

    asio::io_context& ctx_;
    std::string name_;
    configuration config_;
    bool closed_{ false };
    std::optional<collections_manifest> manifest_cache_{};
    std::map<size_t, std::shared_ptr<io::mcbp_session>> sessions_{};
};
} // namespace couchbase
