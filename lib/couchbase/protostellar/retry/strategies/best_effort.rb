# frozen_string_literal: true

module Couchbase
  module Protostellar
    module Retry
      module Strategies
        class BestEffort
          attr_reader :backoff_calculator

          def initialize(backoff_calculator = nil)
            # The default backoff calculator starts with a 1 millisecond backoff and doubles it after each retry
            # attempt, up to a maximum of 50 milliseconds
            @backoff_calculator =
              if backoff_calculator.nil?
                lambda { |retry_attempt_count| [2**retry_attempt_count, 50].min }
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
