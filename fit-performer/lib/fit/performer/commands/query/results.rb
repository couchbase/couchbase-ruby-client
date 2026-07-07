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
    module Commands
      module Query
        class Results < SharedResults
          def self.as_query_result(return_result:, initiated:, content_as:)
            begin
              start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
              result = yield
              end_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            rescue StandardError => e
              return to_exception(error: e, initiated: initiated)
            end
            nanos = ((end_time - start_time) * (10**9)).round
            if return_result
              to_query_result(result: result, initiated: initiated, elapsed_nanos: nanos, content_as: content_as)
            else
              to_success(elapsed_nanos: nanos, initiated: initiated)
            end
          end

          def self.to_query_result(result:, initiated:, elapsed_nanos:, content_as:)
            query_meta_data_pb = FIT::Protocol::SDK::Query::QueryMetaData.new(
              request_id: result.meta_data.request_id,
              client_context_id: result.meta_data.client_context_id,
              status: result.meta_data.status.upcase,
            )
            query_meta_data_pb.signature = result.meta_data.signature.to_json.b unless result.meta_data.signature.nil?
            query_meta_data_pb.profile = result.meta_data.profile.to_json.b unless result.meta_data.profile.nil?

            unless result.meta_data.metrics.elapsed_time.nil?
              query_meta_data_pb.metrics = FIT::Protocol::SDK::Query::QueryMetrics.new(
                elapsed_time: result.meta_data.metrics.elapsed_time,
                execution_time: result.meta_data.metrics.execution_time,
                sort_count: result.meta_data.metrics.sort_count,
                result_count: result.meta_data.metrics.result_count,
                result_size: result.meta_data.metrics.result_size,
                mutation_count: result.meta_data.metrics.mutation_count,
                error_count: result.meta_data.metrics.error_count,
                warning_count: result.meta_data.metrics.warning_count,
              )
            end

            result.meta_data&.warnings&.each do |warning|
              query_meta_data_pb.warnings << FIT::Protocol::SDK::Query::QueryWarning(
                code: warning.code, message: warning.message,
              )
            end

            query_result_pb = FIT::Protocol::SDK::Query::QueryResult.new(meta_data: query_meta_data_pb)
            result.rows.each do |r|
              query_result_pb.content << get_content(content: r, content_as: content_as)
            end

            create_run_result(
              sdk_result: FIT::Protocol::SDK::Result.new(query_result: query_result_pb),
              initiated: initiated,
              elapsed_nanos: elapsed_nanos,
            )
          end
        end
      end
    end
  end
end
