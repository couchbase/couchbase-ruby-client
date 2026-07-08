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
require 'fit/performer/bounds'

module FIT
  module Performer
    module Workloads
      class BaseWorkload
        def initialize(raw_workload, connection, run_id, stream_owner, span_owner, global_counters)
          @logger = Logger.new($stdout)
          @raw_workload = raw_workload
          @connection = connection
          @run_id = run_id
          @stream_owner = stream_owner
          @span_owner = span_owner
          @command_count = @raw_workload.command.size
          @bounds = Bounds.create_bounds(@raw_workload, global_counters)
        end

        def execute(_results)
          raise "`execute` method must be implemented"
        end

        private

        def execute_command(_raw_command, _executed_cmd_count, _results)
          raise '`execute_command` method must be implemented'
        end
      end
    end
  end
end
