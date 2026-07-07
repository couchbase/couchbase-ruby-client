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

require 'fit/protocol/sdk.search.index_manager_pb'

module FIT
  module Performer
    module Commands
      module SearchIndexManager
        class Results < SharedResults
          def self.as_search_index(initiated:)
            begin
              start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
              res = yield
              end_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            rescue StandardError => e
              return to_exception(error: e, initiated: initiated)
            end
            nanos = ((end_time - start_time) * (10**9)).round
            create_run_result(
              sdk_result: FIT::Protocol::SDK::Result.new(
                search_index_manager_result: FIT::Protocol::SDK::Search::IndexManager::Result.new(
                  index: to_search_index(res),
                ),
              ),
              initiated: initiated,
              elapsed_nanos: nanos,
            )
          end

          def self.as_search_indexes(initiated:)
            begin
              start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
              res = yield
              end_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            rescue StandardError => e
              return to_exception(error: e, initiated: initiated)
            end
            nanos = ((end_time - start_time) * (10**9)).round
            create_run_result(
              sdk_result: FIT::Protocol::SDK::Result.new(
                search_index_manager_result: FIT::Protocol::SDK::Search::IndexManager::Result.new(
                  indexes: to_search_indexes(res),
                ),
              ),
              initiated: initiated,
              elapsed_nanos: nanos,
            )
          end

          def self.as_indexed_documents_count(initiated:)
            begin
              start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
              res = yield
              end_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            rescue StandardError => e
              return to_exception(error: e, initiated: initiated)
            end
            nanos = ((end_time - start_time) * (10**9)).round
            create_run_result(
              sdk_result: FIT::Protocol::SDK::Result.new(
                search_index_manager_result: FIT::Protocol::SDK::Search::IndexManager::Result.new(
                  indexed_document_counts: res,
                ),
              ),
              initiated: initiated,
              elapsed_nanos: nanos,
            )
          end

          def self.as_analyze_document_result(initiated:)
            begin
              start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
              res = yield
              end_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            rescue StandardError => e
              return to_exception(error: e, initiated: initiated)
            end
            nanos = ((end_time - start_time) * (10**9)).round

            proto_res = FIT::Protocol::SDK::Search::IndexManager::AnalyzeDocumentResult.new
            res.each do |r|
              proto_res.results << r.to_json
            end

            create_run_result(
              sdk_result: FIT::Protocol::SDK::Result.new(
                search_index_manager_result: FIT::Protocol::SDK::Search::IndexManager::Result.new(
                  analyze_document: proto_res,
                ),
              ),
              initiated: initiated,
              elapsed_nanos: nanos,
            )
          end

          def self.to_search_index(res)
            FIT::Protocol::SDK::Search::IndexManager::SearchIndex.new(
              uuid: res.uuid,
              name: res.name,
              type: res.type,
              source_uuid: res.source_uuid,
              source_type: res.source_type,
              params: (res.params.empty? ? nil : res.params).to_json,
              source_params: (res.source_params.empty? ? nil : res.source_params).to_json,
              plan_params: (res.plan_params.empty? ? nil : res.plan_params).to_json,
            )
          end

          def self.to_search_indexes(res)
            proto_res = FIT::Protocol::SDK::Search::IndexManager::SearchIndexes.new
            res.each do |r|
              proto_res.indexes << to_search_index(r)
            end
            proto_res
          end
        end
      end
    end
  end
end
