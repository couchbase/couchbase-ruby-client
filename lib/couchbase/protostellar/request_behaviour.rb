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
    # @api private
    class RequestBehaviour
      attr_reader :retry_duration
      attr_reader :error

      def initialize(retry_duration: nil, error: nil)
        @retry_duration = retry_duration
        @error = error
      end

      def self.fail(error)
        RequestBehaviour.new(error: error)
      end

      def self.retry(retry_duration)
        RequestBehaviour.new(retry_duration: retry_duration)
      end

      def self.success
        RequestBehaviour.new(retry_duration: nil, error: nil)
      end
    end
  end
end
