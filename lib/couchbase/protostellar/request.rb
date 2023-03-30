# frozen_string_literal: true

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

require "couchbase/utils/time"
require_relative "retry/strategies/best_effort"

module Couchbase
  module Protostellar
    class Request
      attr_reader :service
      attr_reader :rpc
      attr_reader :timeout
      attr_reader :proto_request
      attr_reader :idempotent
      attr_reader :retry_attempts
      attr_reader :retry_reasons
      attr_reader :retry_strategy
      attr_accessor :context

      def initialize(
        service:,
        rpc:,
        proto_request:,
        timeout:,
        retry_strategy: Retry::Strategies::BestEffort::DEFAULT,
        idempotent: false
      )
        @service = service
        @rpc = rpc
        @timeout = timeout
        @deadline = nil
        @proto_request = proto_request
        @idempotent = idempotent
        @retry_attempts = 0
        @retry_reasons = Set.new
        @retry_strategy = retry_strategy
        @context = {}
      end

      def deadline
        if @deadline.nil?
          timeout_secs = 0.001 * Utils::Time.extract_duration(@timeout)
          @deadline = Time.now + timeout_secs
        end
        @deadline
      end

      def add_retry_attempt(reason)
        @retry_reasons.add(reason)
        @retry_attempts += 1
      end

      def error_context
        @context.merge({retry_reasons: @retry_reasons.to_a, retry_attempts: @retry_attempts})
      end
    end
  end
end
