#  Copyright 2023. Couchbase, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

# frozen_string_literal: true

require_relative '../request_behaviour'
require_relative 'action'
require 'couchbase/errors'
require 'couchbase/logger'

module Couchbase
  module Protostellar
    module Retry
      # @api private
      class Orchestrator
        def self.maybe_retry(request, reason)
          if reason.always_retry
            retry_duration = get_controlled_backoff(request)
            request.add_retry_attempt(reason)
            # TODO: Log retry
            RequestBehaviour.retry(retry_duration)
          else
            retry_action = request.retry_strategy.retry_after(request, reason)
            duration = retry_action.duration
            if duration.nil?
              # TODO: Log not retried
              RequestBehaviour.fail(Couchbase::Error::RequestCanceled.new('Cannot retry request', request.error_context))
            else
              request.add_retry_attempt(reason)
              # TODO: Log retry
              RequestBehaviour.retry(duration)
            end
          end
        end

        def self.get_controlled_backoff(request)
          case request.retry_attempts
          when 0 then 1
          when 1 then 10
          when 2 then 50
          when 3 then 100
          when 4 then 500
          else 1000
          end
        end
      end
    end
  end
end
