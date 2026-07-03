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

require_relative '../shared_results'

module FIT
  module Performer
    module Commands
      module BucketManager
        class Results < SharedResults
          def self.as_get_all_buckets_result(return_result:, initiated:)
            begin
              start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
              result = yield
              end_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            rescue StandardError => e
              return to_exception(error: e, initiated: initiated)
            end
            nanos = ((end_time - start_time) * (10**9)).round
            if return_result
              create_run_result(
                sdk_result: Protocol::SDK::Result.new(
                  bucket_manager_result: Protocol::SDK::Cluster::BucketManager::Result.new(
                    get_all_buckets_result: to_get_all_buckets_result(result: result),
                  ),
                ),
                initiated: initiated,
                elapsed_nanos: nanos,
              )
            else
              to_success(elapsed_nanos: nanos, initiated: initiated)
            end
          end

          def self.as_bucket_settings(return_result:, initiated:)
            begin
              start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
              result = yield
              end_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            rescue StandardError => e
              return to_exception(error: e, initiated: initiated)
            end
            nanos = ((end_time - start_time) * (10**9)).round
            if return_result
              create_run_result(
                sdk_result: Protocol::SDK::Result.new(
                  bucket_manager_result: Protocol::SDK::Cluster::BucketManager::Result.new(
                    bucket_settings: to_bucket_settings(result: result),
                  ),
                ),
                initiated: initiated,
                elapsed_nanos: nanos,
              )
            else
              to_success(elapsed_nanos: nanos, initiated: initiated)
            end
          end

          def self.to_get_all_buckets_result(result:)
            proto_result = Protocol::SDK::Cluster::BucketManager::GetAllBucketsResult.new
            result.each do |settings|
              proto_result.result[settings.name] = to_bucket_settings(result: settings)
            end
            proto_result
          end

          def self.to_bucket_settings(result:)
            proto_result = Protocol::SDK::Cluster::BucketManager::BucketSettings.new(
              name: result.name, ram_quota_MB: result.ram_quota_mb,
            )
            proto_result.flush_enabled = result.flush_enabled unless result.flush_enabled.nil?
            proto_result.num_replicas = result.num_replicas unless result.num_replicas.nil?
            proto_result.replica_indexes = result.replica_indexes unless result.replica_indexes.nil?
            proto_result.bucket_type = result.bucket_type.upcase unless result.bucket_type.nil?
            proto_result.eviction_policy = result.eviction_policy.upcase unless result.eviction_policy.nil?
            proto_result.max_expiry_seconds = result.max_expiry unless result.max_expiry.nil?
            proto_result.compression_mode = result.compression_mode.upcase unless result.compression_mode.nil?
            proto_result.minimum_durability_level = result.minimum_durability_level.upcase unless result.minimum_durability_level.nil?
            proto_result.storage_backend = result.storage_backend.upcase unless result.storage_backend.nil?
            unless result.history_retention_collection_default.nil?
              proto_result.history_retention_collection_default = result.history_retention_collection_default
            end
            proto_result.history_retention_seconds = result.history_retention_duration unless result.history_retention_duration.nil?
            proto_result.history_retention_bytes = result.history_retention_bytes unless result.history_retention_bytes.nil?
            proto_result.num_vbuckets = result.num_vbuckets unless result.num_vbuckets.nil?
            proto_result
          end
        end
      end
    end
  end
end
