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

require 'concurrent/atomic/atomic_fixnum'

module FIT
  module Performer
    class Counters
      def initialize
        @counters = {}
        @mutex = Mutex.new
      end

      def clear
        @mutex.synchronize do
          @counters.clear
        end
      end

      def set_counter_value(counter_id, value)
        @mutex.synchronize do
          if @counters.key?(counter_id)
            @counters[counter_id].value = value
          else
            @counters[counter_id] = Concurrent::AtomicFixnum.new(value)
          end
        end
      end

      def get_counter(counter_id, initial_value = nil)
        @mutex.synchronize do
          return @counters[counter_id] if @counters.key?(counter_id)

          raise PerformerError, "Counter #{counter_id} does not exist and no initial value was provided" if initial_value.nil?

          @counters[counter_id] = Concurrent::AtomicFixnum.new(initial_value)
        end
      end
    end
  end
end
