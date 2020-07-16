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

#include <gsl/gsl_assert>

#include <io/retry_reason.hxx>
#include <io/retry_action.hxx>

namespace couchbase::io::retry_strategy
{
namespace backoff
{
struct backoff_delay {
    std::chrono::milliseconds delay;
    std::chrono::milliseconds min_delay;
    std::chrono::milliseconds max_delay;
};

class fixed
{
  private:
    backoff_delay delay_;

  public:
    explicit fixed(std::chrono::milliseconds delay)
      : delay_{ delay, delay, delay }
    {
    }

    template<class Request>
    backoff_delay operator()(Request&)
    {
        return delay_;
    }
};

class exponential
{
  public:
    /**
     * Backoff function with exponential backoff delay. Retries are performed after a backoff interval of
     *
     *   first_backoff * (factor^n)
     *
     * where n is the iteration. If max_backoff is not zero, the maximum backoff applied will be limited to max_backoff.
     *
     * If based_on_previous_value is true, backoff will be calculated using
     *
     *   prev_backoff * factor
     *
     * When backoffs are combined with jitter, this value will be different from the actual exponential value for the iteration.
     *
     * @param first_backoff First backoff duration
     * @param factor The multiplicand for calculating backoff
     * @param max_backoff Maximum backoff duration
     * @param based_on_previous_value If true, calculation is based on previous value which may be a backoff with jitter applied
     */
    exponential(std::chrono::milliseconds first_backoff,
                int factor,
                std::chrono::milliseconds max_backoff = {},
                bool based_on_previous_value = false)
    {
        Expects(first_backoff.count() > 0);
        if (max_backoff.count() <= 0) {
            max_backoff = std::chrono::milliseconds{ std::numeric_limits<std::chrono::milliseconds::rep>::max() };
        }
        Expects(max_backoff > first_backoff);
        first_backoff_ = first_backoff;
        max_backoff_ = max_backoff;
        factor_ = factor;
        based_on_previous_value_ = based_on_previous_value;
    }

    template<class Request>
    backoff_delay operator()(Request& request)
    {
        std::chrono::milliseconds previous_backoff = request.retries.last_duration;
        std::chrono::milliseconds next_backoff;

        if (based_on_previous_value_) {
            if (previous_backoff >= max_backoff_) {
                next_backoff = max_backoff_;
            } else {
                next_backoff = previous_backoff * factor_;
            }
            if (next_backoff < first_backoff_) {
                next_backoff = first_backoff_;
            }
        } else {
            if (previous_backoff >= max_backoff_) {
                next_backoff = max_backoff_;
            } else {
                next_backoff = first_backoff_ * static_cast<int>(std::pow(factor_, request.retries.retry_attempts - 1));
            }
        }
        return { next_backoff, first_backoff_, max_backoff_ };
    }

  private:
    std::chrono::milliseconds first_backoff_;
    std::chrono::milliseconds max_backoff_;
    int factor_;
    bool based_on_previous_value_;
};

} // namespace backoff

class best_effort
{
  public:
    best_effort()
      : backoff_(std::chrono::milliseconds(1), 2, std::chrono::milliseconds(500))
    {
    }

    template<class Request>
    retry_action should_retry(Request& request, retry_reason reason)
    {
        if (request.retries.idempotent || allows_non_idempotent_retry(reason)) {
            backoff::backoff_delay delay = backoff_(request);
            return { true, delay.delay };
        }
        return { false };
    }

  private:
    backoff::exponential backoff_;
};

class fail_fast
{
  public:
    template<class Request>
    retry_action should_retry(Request&, retry_reason)
    {
        return { false };
    }
};

} // namespace couchbase::io::retry_strategy
