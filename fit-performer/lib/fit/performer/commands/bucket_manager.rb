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

require_relative 'bucket_manager/get_bucket_command'
require_relative 'bucket_manager/get_all_buckets_command'
require_relative 'bucket_manager/create_bucket_command'
require_relative 'bucket_manager/drop_bucket_command'
require_relative 'bucket_manager/flush_bucket_command'
require_relative 'bucket_manager/update_bucket_command'

module FIT
  module Performer
    module Commands
      module BucketManager
        def self.build_command(raw_cmd, cluster, cmd_kwargs)
          cmd_type = raw_cmd.command
          cmd = raw_cmd.public_send(cmd_type)

          cmd_kwargs[:manager] = cluster.buckets
          cmd_kwargs[:raw_options] = cmd.options if cmd.has_options?

          case cmd_type
          when :get_bucket
            cmd_kwargs[:bucket_name] = cmd.bucket_name
            GetBucketCommand.create_command(**cmd_kwargs)
          when :get_all_buckets
            GetAllBucketsCommand.create_command(**cmd_kwargs)
          when :create_bucket
            cmd_kwargs[:settings] = get_create_bucket_settings(cmd)
            CreateBucketCommand.create_command(**cmd_kwargs)
          when :drop_bucket
            cmd_kwargs[:bucket_name] = cmd.bucket_name
            DropBucketCommand.create_command(**cmd_kwargs)
          when :flush_bucket
            cmd_kwargs[:bucket_name] = cmd.bucket_name
            FlushBucketCommand.create_command(**cmd_kwargs)
          when :update_bucket
            cmd_kwargs[:settings] = get_bucket_settings(cmd)
            UpdateBucketCommand.create_command(**cmd_kwargs)
          else
            raise PerformerError, "Bucket management command `#{cmd_type}` not supported"
          end
        end

        def self.get_bucket_settings(raw_mgmt_cmd)
          proto_settings = raw_mgmt_cmd.settings

          Couchbase::Management::BucketSettings.new do |s|
            s.name = proto_settings.name
            s.flush_enabled = proto_settings.flush_enabled if proto_settings.has_flush_enabled?
            s.ram_quota_mb = proto_settings.ram_quota_MB
            s.num_replicas = proto_settings.num_replicas if proto_settings.has_num_replicas?
            s.replica_indexes = proto_settings.replica_indexes if proto_settings.has_replica_indexes?
            s.bucket_type = proto_settings.bucket_type.downcase if proto_settings.has_bucket_type?
            s.eviction_policy = proto_settings.eviction_policy.downcase if proto_settings.has_eviction_policy?
            s.max_expiry = proto_settings.max_expiry_seconds if proto_settings.has_max_expiry_seconds?
            s.compression_mode = proto_settings.compression_mode.downcase if proto_settings.has_compression_mode?
            s.minimum_durability_level = proto_settings.minimum_durability_level.downcase if proto_settings.has_minimum_durability_level?
            s.storage_backend = proto_settings.storage_backend.downcase if proto_settings.has_storage_backend?
            if proto_settings.has_history_retention_collection_default?
              s.history_retention_collection_default = proto_settings.history_retention_collection_default
            end
            s.history_retention_duration = proto_settings.history_retention_seconds if proto_settings.has_history_retention_seconds?
            s.history_retention_bytes = proto_settings.history_retention_bytes if proto_settings.has_history_retention_bytes?
            s.num_vbuckets = proto_settings.num_vbuckets if proto_settings.has_num_vbuckets?
          end
        end

        def self.get_create_bucket_settings(raw_mgmt_cmd)
          proto_create_settings = raw_mgmt_cmd.settings
          settings = get_bucket_settings(proto_create_settings)
          if proto_create_settings.has_conflict_resolution_type?
            settings.conflict_resolution_type = proto_create_settings.conflict_resolution_type.downcase
          end
          settings
        end
      end
    end
  end
end
