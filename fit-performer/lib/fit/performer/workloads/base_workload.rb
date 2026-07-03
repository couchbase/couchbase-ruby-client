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

require 'fit/performer/performer_error'

module FIT
  module Performer
    module Workloads
      class BaseWorkload
        def initialize(raw_workload, connection, run_id, stream_owner, span_owner)
          @logger = Logger.new($stdout)
          @raw_workload = raw_workload
          @connection = connection
          @run_id = run_id
          @stream_owner = stream_owner
          @span_owner = span_owner
          @command_count = @raw_workload.command.size

          # Bounds-related attributes
          @counter_id = nil
          @deadline = nil
          @remaining_command_count = nil
        end

        def execute(_results, _global_counters, _global_semaphore)
          raise "`execute` method must be implemented"
        end

        def set_bounds(global_counters, global_semaphore)
          unless @raw_workload.has_bounds?
            @remaining_command_count = @command_count
            return
          end

          raw_bounds = @raw_workload.bounds
          case raw_bounds.bounds
          when :counter
            raw_counter = raw_bounds.counter
            @counter_id = raw_counter.counter_id

            case raw_counter.counter
            when :global
              global_semaphore.synchronize do
                unless global_counters.include?(@counter_id)
                  initial_count = raw_counter.global.count
                  global_counters[@counter_id] = initial_count
                end
              end
            else
              raise PerformerError, "Counter type `#{counter.counter}` not recognised"
            end
          when :for_time
            @deadline = Time.now + raw_bounds.for_time.seconds
          else
            raise PerformerError, "Bounds type `#{@raw_workload.bounds}` not recognised"
          end
        end

        def within_bounds(global_counters, global_semaphore)
          if !@counter_id.nil?
            global_semaphore.synchronize do
              global_counters[@counter_id] -= 1
              global_counters[@counter_id] >= 0
            end
          elsif !@deadline.nil?
            Time.now < @deadline
          elsif !@remaining_command_count.nil?
            @remaining_command_count -= 1
            @remaining_command_count >= 0
          else
            raise PerformerError, "Bounds have not been set"
          end
        end

        private

        def execute_command(_raw_command, _executed_cmd_count, _results)
          raise '`execute_command` method must be implemented'
        end
      end
    end
  end
end
