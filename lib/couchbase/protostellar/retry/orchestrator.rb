# frozen_string_literal: true

require_relative '../request_behaviour'
require_relative 'action'
require 'couchbase/errors'
require 'couchbase/logger'

module Couchbase
  module Protostellar
    module Retry
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
              RequestBehaviour.fail(Error::RequestCanceled.new)
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
