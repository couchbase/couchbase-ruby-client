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

require 'fit/protocol/sdk.query.index_manager_pb'
require 'fit/protocol/sdk.workload_pb'
require 'fit/protocol/shared.basic_pb'

require_relative '../shared_results'

module FIT
  module Performer
    module Commands
      module QueryIndexManager
        class Results < SharedResults
          def self.as_query_indexes(return_result:, initiated:)
            begin
              start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
              result = yield
              end_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            rescue StandardError => e
              return to_exception(error: e, initiated: initiated)
            end
            nanos = ((end_time - start_time) * (10**9)).round
            if return_result
              to_query_indexes(result: result, initiated: initiated, elapsed_nanos: nanos)
            else
              to_success(elapsed_nanos: nanos, initiated: initiated)
            end
          end

          def self.to_query_indexes(result:, initiated:, elapsed_nanos:)
            indexes_pb = FIT::Protocol::SDK::Query::IndexManager::QueryIndexes.new(
              indexes: result.map do |index|
                # TODO: Keyspace is missing from SDK result
                kwargs = {
                  name: index.name,
                  is_primary: index.is_primary,
                  type: index.type.upcase,
                  state: index.state.to_s,
                  index_key: index.index_key,
                  bucket_name: index.bucket,
                }
                kwargs[:condition] = index.condition unless index.condition.nil?
                kwargs[:partition] = index.partition unless index.partition.nil?
                kwargs[:scope_name] = index.scope unless index.scope.nil?
                kwargs[:collection_name] = index.collection unless index.collection.nil?

                FIT::Protocol::SDK::Query::IndexManager::QueryIndex.new(kwargs)
              end,
            )
            create_run_result(
              sdk_result: FIT::Protocol::SDK::Result.new(query_indexes: indexes_pb),
              initiated: initiated,
              elapsed_nanos: elapsed_nanos,
            )
          end
        end
      end
    end
  end
end
