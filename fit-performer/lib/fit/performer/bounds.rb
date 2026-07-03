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

require_relative 'bounds/simple'
require_relative 'bounds/for_time'
require_relative 'bounds/counter_equality'
require_relative 'bounds/counter'

module FIT
  module Performer
    module Bounds
      def self.create_bounds(raw_workload, global_counters)
        return Simple.new(raw_workload.command.size) unless raw_workload.has_bounds?

        raw_bounds = raw_workload.bounds
        case raw_bounds.bounds
        when :counter
          counter_id = raw_bounds.counter.counter_id
          initial_value = raw_bounds.counter.global.count
          global_counter = global_counters.get_counter(counter_id, initial_value)
          Counter.new(global_counter)

        when :for_time
          ForTime.new(raw_bounds.for_time.seconds)

        when :counter_eq
          counter_id = raw_bounds.counter_eq.counter_id
          initial_value = raw_bounds.counter_eq.global.count
          global_counter = global_counters.get_counter(counter_id, initial_value)
          CounterEquality.new(global_counter, initial_value)

        else
          raise PerformerError, "Bounds type `#{raw_bounds.bounds}` not recognised"
        end
      end
    end
  end
end
