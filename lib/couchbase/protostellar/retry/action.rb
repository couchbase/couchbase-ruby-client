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
      # @api private
      class Action
        attr_reader :duration

        def initialize(duration)
          @duration = duration
        end

        def self.with_duration(duration)
          Action.new(duration)
        end

        def self.no_retry
          Action.new(nil)
        end
      end
    end
  end
end
