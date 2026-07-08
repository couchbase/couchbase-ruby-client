# frozen_string_literal: true

#  Copyright 2026-Present Couchbase, Inc.
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

module FIT
  module Performer
    module Bounds
      # A bounds implementation that executes commands for a specified number of times, based on a global counter.
      # The global counter may be shared with other workloads.
      class Counter
        # Initializes a new instance of the Counter bounds implementation.
        #
        # @param global_counter [Concurrent::AtomicFixnum] The global counter to use for determining if the workload
        # can execute.
        def initialize(global_counter)
          @global_counter = global_counter
        end

        def can_execute?
          @global_counter.decrement >= 0
        end
      end
    end
  end
end
