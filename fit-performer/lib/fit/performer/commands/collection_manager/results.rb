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

require 'fit/protocol/sdk.bucket.collection_manager_pb'

require_relative '../shared_results'

module FIT
  module Performer
    module Commands
      module CollectionManager
        class Results < SharedResults
          def self.as_get_all_scopes_result(return_result:, initiated:)
            begin
              start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
              result = yield
              end_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            rescue StandardError => e
              return to_exception(error: e, initiated: initiated)
            end
            nanos = ((end_time - start_time) * (10**9)).round
            if return_result
              to_get_all_scopes_result(result: result, initiated: initiated, elapsed_nanos: nanos)
            else
              to_success(elapsed_nanos: nanos, initiated: initiated)
            end
          end

          def self.to_get_all_scopes_result(result:, initiated:, elapsed_nanos:)
            proto_result = Protocol::SDK::Bucket::CollectionManager::GetAllScopesResult.new
            result.each do |scope_spec|
              proto_scope_spec = Protocol::SDK::Bucket::CollectionManager::ScopeSpec.new(name: scope_spec.name)
              scope_spec.collections.each do |coll_spec|
                proto_coll_spec = Protocol::SDK::Bucket::CollectionManager::CollectionSpec.new(
                  name: coll_spec.name,
                  scope_name: coll_spec.scope_name,
                )
                proto_coll_spec.expiry_secs = coll_spec.max_expiry unless coll_spec.max_expiry.nil?
                proto_coll_spec.history = coll_spec.history unless coll_spec.history.nil?

                proto_scope_spec.collections << proto_coll_spec
              end
              proto_result.result << proto_scope_spec
            end

            create_run_result(
              sdk_result: Protocol::SDK::Result.new(
                collection_manager_result: Protocol::SDK::Bucket::CollectionManager::Result.new(
                  get_all_scopes_result: proto_result,
                ),
              ),
              initiated: initiated,
              elapsed_nanos: elapsed_nanos,
            )
          end
        end
      end
    end
  end
end
