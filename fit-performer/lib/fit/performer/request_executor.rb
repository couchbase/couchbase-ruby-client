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

module FIT
  module Performer
    class RequestExecutor
      def initialize(workload_executor, request)
        @logger = Logger.new($stdout)
        @workload_executor = workload_executor
        @request = request
        @conn_id = @request.workloads.cluster_connection_id
      end

      def set_batch_size
        @batch_size = 1
        return unless @request.has_config? && @request.config.has_streaming_config? && @request.config.streaming_config.has_batch_size?

        @batch_size = @request.config.streaming_config.batch_size
      end

      def execute_request
        @workload_executor.execute_workloads
      end

      def results
        Enumerator.new do |enum|
          loop do
            result = next_result
            break if result.nil?

            enum << result
          end
        end
      end

      def self.build_request(workload_executor, request)
        executor = RequestExecutor.new(workload_executor, request)
        executor.set_batch_size
        executor
      end

      private

      def next_result
        batch = []
        deadline = Time.now + 0.1
        loop do
          break if batch.size == @batch_size || Time.now > deadline

          result = @workload_executor.results.pop
          break if result.nil? # No more results

          batch.append(result)
        end

        return nil if batch.empty?
        return batch[0] if batch.size == 1

        batched_result = FIT::Protocol::Run::BatchedResult.new(result: batch)
        FIT::Protocol::Run::Result.new(batched: batched_result)
      end
    end
  end
end
