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

module Couchbase
  module Protostellar
    module Retry
      module Strategies
        class BestEffort
          attr_reader :backoff_calculator

          def initialize(backoff_calculator = nil)
            # The default backoff calculator starts with a 1 millisecond backoff and doubles it after each retry
            # attempt, up to a maximum of 500 milliseconds
            @backoff_calculator =
              if backoff_calculator.nil?
                lambda { |retry_attempt_count| [2**retry_attempt_count, 500].min }
              else
                backoff_calculator
              end
          end

          def retry_after(request, reason)
            if request.idempotent || reason.allows_non_idempotent_retry
              backoff = @backoff_calculator.call(request.retry_attempts)
              Retry::Action.with_duration(backoff)
            else
              Retry::Action.no_retry
            end
          end

          DEFAULT = BestEffort.new.freeze
        end
      end
    end
  end
end
