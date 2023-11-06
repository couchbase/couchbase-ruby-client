# frozen_string_literal: true

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

require_relative "../../request"
require_relative "../../generated/admin/bucket/v1/bucket_pb"
require_relative "../kv"

module Couchbase
  module Protostellar
    module RequestGenerator
      module Admin
        class Bucket
          BUCKET_TYPE_MAP = {
            :couchbase => :BUCKET_TYPE_COUCHBASE,
            :memcached => :BUCKET_TYPE_MEMCACHED,
            :ephemeral => :BUCKET_TYPE_EPHEMERAL,
          }.freeze

          EVICTION_MODE_MAP = {
            :full => :EVICTION_MODE_FULL,
            :value_only => :EVICTION_MODE_VALUE_ONLY,
            :no_eviction => :EVICTION_MODE_NONE,
            :not_recently_used => :EVICTION_MODE_NOT_RECENTLY_USED,
          }.freeze

          COMPRESSION_MODE_MAP = {
            :off => :COMPRESSION_MODE_OFF,
            :passive => :COMPRESSION_MODE_PASSIVE,
            :active => :COMPRESSION_MODE_ACTIVE,
          }.freeze

          STORAGE_BACKEND_MAP = {
            :couchstore => :STORAGE_BACKEND_COUCHSTORE,
            :magma => :STORAGE_BACKEND_MAGMA,
          }.freeze

          CONFLICT_RESOLUTION_TYPE_MAP = {
            :timestamp => :CONFLICT_RESOLUTION_TYPE_TIMESTAMP,
            :sequence_number => :CONFLICT_RESOLUTION_TYPE_SEQUENCE_NUMBER,
            :custom => :CONFLICT_RESOLUTION_TYPE_CUSTOM,
          }.freeze

          DURABILITY_LEVEL_MAP = RequestGenerator::KV::DURABILITY_LEVEL_MAP

          def list_buckets_request(options)
            proto_req = Generated::Admin::Bucket::V1::ListBucketsRequest.new
            create_request(proto_req, :list_buckets, options, idempotent: true)
          end

          def create_bucket_request(settings, options)
            proto_req = Generated::Admin::Bucket::V1::CreateBucketRequest.new(
              extract_bucket_settings(settings, create: true)
            )
            create_request(proto_req, :create_bucket, options)
          end

          def update_bucket_request(settings, options)
            proto_req = Generated::Admin::Bucket::V1::UpdateBucketRequest.new(
              extract_bucket_settings(settings, create: false)
            )
            create_request(proto_req, :update_bucket, options)
          end

          def delete_bucket_request(bucket_name, options)
            proto_req = Generated::Admin::Bucket::V1::DeleteBucketRequest.new(
              bucket_name: bucket_name
            )
            create_request(proto_req, :delete_bucket, options)
          end

          private

          def extract_bucket_settings(settings, create: false)
            s = {bucket_name: settings.name}
            s[:bucket_type] = BUCKET_TYPE_MAP[settings.bucket_type] if create
            s[:ram_quota_mb] = settings.ram_quota_mb if create || !settings.ram_quota_mb.nil?
            s[:num_replicas] = settings.num_replicas if create || !settings.num_replicas.nil?
            s[:flush_enabled] = settings.flush_enabled unless settings.flush_enabled.nil?
            s[:replica_indexes] = settings.replica_indexes unless settings.replica_indexes.nil?
            s[:eviction_mode] = EVICTION_MODE_MAP[settings.eviction_policy] unless settings.eviction_policy.nil?
            s[:max_expiry_secs] = settings.max_expiry unless settings.max_expiry.nil?
            s[:compression_mode] = COMPRESSION_MODE_MAP[settings.compression_mode] unless settings.compression_mode.nil?
            s[:minimum_durability_level] = DURABILITY_LEVEL_MAP[settings.minimum_durability_level] unless settings.minimum_durability_level.nil?
            s[:storage_backend] = STORAGE_BACKEND_MAP[settings.storage_backend] if create && !settings.storage_backend.nil?
            s[:conflict_resolution_type] = CONFLICT_RESOLUTION_TYPE_MAP[settings.conflict_resolution_type] unless settings.conflict_resolution_type.nil?
            s
          end

          def create_request(proto_request, rpc, options, idempotent: false)
            Request.new(
              service: :bucket_admin,
              rpc: rpc,
              proto_request: proto_request,
              idempotent: idempotent,
              timeout: options.timeout
            )
          end
        end
      end
    end
  end
end
