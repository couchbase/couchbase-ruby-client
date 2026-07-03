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

require_relative '../commands'
require_relative '../streaming'
require_relative 'base_workload'

module FIT
  module Performer
    module Workloads
      class SdkWorkload < BaseWorkload
        def initialize(raw_workload, connection, run_id, stream_owner, span_owner)
          super
          @logger = Logger.new($stdout)
        end

        def execute(results, global_counters, global_semaphore)
          executed_cmd_count = 0
          while within_bounds(global_counters, global_semaphore)
            raw_cmd = @raw_workload.command[executed_cmd_count % @command_count]
            execute_command(raw_cmd, executed_cmd_count, results)
            executed_cmd_count += 1
          end
        end

        def execute_command(raw_command, executed_cmd_count, results)
          command = Commands.build_command(raw_command, @connection, @span_owner, executed_cmd_count)
          result = command.execute_command

          if result.is_a?(Enumerator)
            @stream_owner.register_and_start_stream(
              Streaming::Stream.build_stream(
                run_id: @run_id,
                results: results,
                sdk_command: command,
                enumerator: result,
              ),
            )
          else
            results.push(result)
          end
        end
      end
    end
  end
end
