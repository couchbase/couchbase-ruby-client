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

require 'logger'
require 'fit/performer/workloads'

module FIT
  module Performer
    class MultiThreadExecutor
      attr_reader :results

      def initialize(connection, run_id, stream_owner, span_owner, global_counters)
        @logger = Logger.new($stdout)
        @results = Queue.new
        @connection = connection
        @run_id = run_id
        @stream_owner = stream_owner
        @span_owner = span_owner
        @global_counters = global_counters
      end

      def build_workloads(request)
        @workloads = request.workloads.horizontal_scaling.map do |h|
          h.workloads.map do |w|
            Workloads.build_workload(w, @connection, @run_id, @stream_owner, @span_owner, @global_counters)
          end
        end
      end

      def start_workload_thread(workloads)
        Thread.new do # rubocop:disable ThreadSafety/NewThread
          workloads.each do |w|
            w.execute(@results)
          end
        end
      end

      def execute_workloads
        Thread.new do # rubocop:disable ThreadSafety/NewThread
          threads = []
          @workloads.each do |w|
            thread = start_workload_thread(w)
            threads.append(thread)
          end

          # Wait for all threads to finish their workloads
          threads.each(&:join)

          # Wait for all streams to finish
          @stream_owner.wait_for_all_streams_from_run(@run_id)

          # Close the result queue to indicate that no more workloads will be executed
          @results.close
        end
      end

      def self.build_executor(request, connection, run_id, stream_owner, span_owner, global_counters)
        executor = MultiThreadExecutor.new(connection, run_id, stream_owner, span_owner, global_counters)
        executor.build_workloads(request)
        executor
      end
    end
  end
end
