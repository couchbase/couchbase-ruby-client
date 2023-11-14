#  Copyright 2023. Couchbase, Inc.
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

# frozen_string_literal: true

require "couchbase/management/bucket_manager"

require_relative "../../request_generator/admin/bucket"

module Couchbase
  module Protostellar
    module ResponseConverter
      module Admin
        class Bucket
          BUCKET_TYPE_MAP = RequestGenerator::Admin::Bucket::BUCKET_TYPE_MAP.invert.freeze
          EVICTION_MODE_MAP = RequestGenerator::Admin::Bucket::EVICTION_MODE_MAP.invert.freeze
          COMPRESSION_MODE_MAP = RequestGenerator::Admin::Bucket::COMPRESSION_MODE_MAP.invert.freeze
          STORAGE_BACKEND_MAP = RequestGenerator::Admin::Bucket::STORAGE_BACKEND_MAP.invert.freeze
          CONFLICT_RESOLUTION_TYPE_MAP = RequestGenerator::Admin::Bucket::CONFLICT_RESOLUTION_TYPE_MAP.invert.freeze
          DURABILITY_LEVEL_MAP = RequestGenerator::Admin::Bucket::DURABILITY_LEVEL_MAP.invert.freeze

          def self.to_bucket_settings_array(resp)
            resp.buckets.map do |r|
              Couchbase::Management::BucketSettings.new do |b|
                b.name = r.bucket_name
                b.flush_enabled = r.flush_enabled
                b.ram_quota_mb = r.ram_quota_mb
                b.num_replicas = r.num_replicas
                b.replica_indexes = r.replica_indexes
                b.bucket_type = BUCKET_TYPE_MAP[r.bucket_type]
                b.max_expiry = r.max_expiry_secs
                b.eviction_policy = EVICTION_MODE_MAP[r.eviction_mode]
                b.minimum_durability_level = DURABILITY_LEVEL_MAP[r.minimum_durability_level]
                b.compression_mode = COMPRESSION_MODE_MAP[r.compression_mode]
                b.conflict_resolution_type = CONFLICT_RESOLUTION_TYPE_MAP[r.conflict_resolution_type]
                b.storage_backend = STORAGE_BACKEND_MAP[r.storage_backend]
              end
            end
          end
        end
      end
    end
  end
end
