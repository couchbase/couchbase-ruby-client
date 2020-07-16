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

#include <spdlog/spdlog.h>

#include <io/retry_reason.hxx>
#include <io/retry_action.hxx>

namespace couchbase::io::retry_orchestrator
{

namespace priv
{
template<class Command>
std::chrono::milliseconds
cap_duration(std::chrono::milliseconds uncapped, std::shared_ptr<Command> command)
{
    auto theoretical_deadline = std::chrono::steady_clock::now() + uncapped;
    auto absolute_deadline = command->deadline.expiry();
    auto delta = std::chrono::duration_cast<std::chrono::milliseconds>(theoretical_deadline - absolute_deadline);
    if (delta.count() > 0) {
        auto capped = uncapped - delta;
        if (capped.count() < 0) {
            return uncapped; // something went wrong, return the uncapped one as a safety net
        }
        return capped;
    }
    return uncapped;
}

std::chrono::milliseconds
controlled_backoff(int retry_attempts)
{
    switch (retry_attempts) {
        case 0:
            return std::chrono::milliseconds(1);
        case 1:
            return std::chrono::milliseconds(10);
        case 2:
            return std::chrono::milliseconds(50);
        case 3:
            return std::chrono::milliseconds(100);
        case 4:
            return std::chrono::milliseconds(500);
        default:
            return std::chrono::milliseconds(1000);
    }
}

template<class Manager, class Command>
void
retry_with_duration(std::shared_ptr<Manager> manager,
                    std::shared_ptr<Command> command,
                    retry_reason reason,
                    std::chrono::milliseconds duration)
{
    ++command->request.retries.retry_attempts;
    command->request.retries.reasons.insert(reason);
    command->request.retries.last_duration = duration;
    manager->schedule_for_retry(command, duration);
}

} // namespace priv

template<class Manager, class Command>
void
maybe_retry(std::shared_ptr<Manager> manager, std::shared_ptr<Command> command, retry_reason reason, std::error_code ec)
{
    if (always_retry(reason)) {
        return priv::retry_with_duration(manager, command, reason, priv::controlled_backoff(command->request.retries.retry_attempts));
    }

    retry_action action = command->request.retries.strategy.should_retry(command->request, reason);
    if (action.retry_requested) {
        return priv::retry_with_duration(manager, command, reason, priv::cap_duration(action.duration, command));
    }
    return command->invoke_handler(ec);
}

} // namespace couchbase::io::retry_orchestrator
