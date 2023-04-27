# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: couchbase/admin/bucket/v1/bucket.proto

require 'google/protobuf'

require 'couchbase/protostellar/generated/kv/v1/kv_pb'

Google::Protobuf::DescriptorPool.generated_pool.build do
  add_file("couchbase/admin/bucket/v1/bucket.proto", :syntax => :proto3) do
    add_message "couchbase.admin.bucket.v1.ListBucketsRequest" do
    end
    add_message "couchbase.admin.bucket.v1.ListBucketsResponse" do
      repeated :buckets, :message, 1, "couchbase.admin.bucket.v1.ListBucketsResponse.Bucket"
    end
    add_message "couchbase.admin.bucket.v1.ListBucketsResponse.Bucket" do
      optional :bucket_name, :string, 1
      optional :flush_enabled, :bool, 2
      optional :ram_quota_bytes, :uint64, 3
      optional :num_replicas, :uint32, 4
      optional :replica_indexes, :bool, 5
      optional :bucket_type, :enum, 6, "couchbase.admin.bucket.v1.BucketType"
      optional :eviction_mode, :enum, 7, "couchbase.admin.bucket.v1.EvictionMode"
      optional :max_expiry_secs, :uint32, 8
      optional :compression_mode, :enum, 9, "couchbase.admin.bucket.v1.CompressionMode"
      optional :minimum_durability_level, :enum, 10, "couchbase.kv.v1.DurabilityLevel"
      optional :storage_backend, :enum, 11, "couchbase.admin.bucket.v1.StorageBackend"
      optional :conflict_resolution_type, :enum, 12, "couchbase.admin.bucket.v1.ConflictResolutionType"
    end
    add_message "couchbase.admin.bucket.v1.CreateBucketRequest" do
      optional :bucket_name, :string, 1
      optional :bucket_type, :enum, 2, "couchbase.admin.bucket.v1.BucketType"
      optional :ram_quota_bytes, :uint64, 3
      optional :num_replicas, :uint32, 4
      proto3_optional :flush_enabled, :bool, 5
      proto3_optional :replica_indexes, :bool, 6
      proto3_optional :eviction_mode, :enum, 7, "couchbase.admin.bucket.v1.EvictionMode"
      proto3_optional :max_expiry_secs, :uint32, 8
      proto3_optional :compression_mode, :enum, 9, "couchbase.admin.bucket.v1.CompressionMode"
      proto3_optional :minimum_durability_level, :enum, 10, "couchbase.kv.v1.DurabilityLevel"
      proto3_optional :storage_backend, :enum, 11, "couchbase.admin.bucket.v1.StorageBackend"
      proto3_optional :conflict_resolution_type, :enum, 12, "couchbase.admin.bucket.v1.ConflictResolutionType"
    end
    add_message "couchbase.admin.bucket.v1.CreateBucketResponse" do
      optional :bucket_uuid, :string, 1
    end
    add_message "couchbase.admin.bucket.v1.UpdateBucketRequest" do
      optional :bucket_name, :string, 1
      proto3_optional :ram_quota_bytes, :uint64, 3
      proto3_optional :num_replicas, :uint32, 4
      proto3_optional :flush_enabled, :bool, 5
      proto3_optional :replica_indexes, :bool, 6
      proto3_optional :eviction_mode, :enum, 7, "couchbase.admin.bucket.v1.EvictionMode"
      proto3_optional :max_expiry_secs, :uint32, 8
      proto3_optional :compression_mode, :enum, 9, "couchbase.admin.bucket.v1.CompressionMode"
      proto3_optional :minimum_durability_level, :enum, 10, "couchbase.kv.v1.DurabilityLevel"
      proto3_optional :conflict_resolution_type, :enum, 12, "couchbase.admin.bucket.v1.ConflictResolutionType"
    end
    add_message "couchbase.admin.bucket.v1.UpdateBucketResponse" do
    end
    add_message "couchbase.admin.bucket.v1.DeleteBucketRequest" do
      optional :bucket_name, :string, 1
    end
    add_message "couchbase.admin.bucket.v1.DeleteBucketResponse" do
    end
    add_enum "couchbase.admin.bucket.v1.BucketType" do
      value :BUCKET_TYPE_COUCHBASE, 0
      value :BUCKET_TYPE_MEMCACHED, 1
      value :BUCKET_TYPE_EPHEMERAL, 2
    end
    add_enum "couchbase.admin.bucket.v1.EvictionMode" do
      value :EVICTION_MODE_FULL, 0
      value :EVICTION_MODE_NOT_RECENTLY_USED, 1
      value :EVICTION_MODE_VALUE_ONLY, 2
      value :EVICTION_MODE_NONE, 3
    end
    add_enum "couchbase.admin.bucket.v1.CompressionMode" do
      value :COMPRESSION_MODE_OFF, 0
      value :COMPRESSION_MODE_PASSIVE, 1
      value :COMPRESSION_MODE_ACTIVE, 2
    end
    add_enum "couchbase.admin.bucket.v1.StorageBackend" do
      value :STORAGE_BACKEND_COUCHSTORE, 0
      value :STORAGE_BACKEND_MAGMA, 1
    end
    add_enum "couchbase.admin.bucket.v1.ConflictResolutionType" do
      value :CONFLICT_RESOLUTION_TYPE_TIMESTAMP, 0
      value :CONFLICT_RESOLUTION_TYPE_SEQUENCE_NUMBER, 1
      value :CONFLICT_RESOLUTION_TYPE_CUSTOM, 2
    end
  end
end

module Couchbase
  module Protostellar
    module Generated
      module Admin
        module Bucket
          module V1
            ListBucketsRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.admin.bucket.v1.ListBucketsRequest").msgclass
            ListBucketsResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.admin.bucket.v1.ListBucketsResponse").msgclass
            ListBucketsResponse::Bucket = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.admin.bucket.v1.ListBucketsResponse.Bucket").msgclass
            CreateBucketRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.admin.bucket.v1.CreateBucketRequest").msgclass
            CreateBucketResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.admin.bucket.v1.CreateBucketResponse").msgclass
            UpdateBucketRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.admin.bucket.v1.UpdateBucketRequest").msgclass
            UpdateBucketResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.admin.bucket.v1.UpdateBucketResponse").msgclass
            DeleteBucketRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.admin.bucket.v1.DeleteBucketRequest").msgclass
            DeleteBucketResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.admin.bucket.v1.DeleteBucketResponse").msgclass
            BucketType = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.admin.bucket.v1.BucketType").enummodule
            EvictionMode = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.admin.bucket.v1.EvictionMode").enummodule
            CompressionMode = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.admin.bucket.v1.CompressionMode").enummodule
            StorageBackend = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.admin.bucket.v1.StorageBackend").enummodule
            ConflictResolutionType = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.admin.bucket.v1.ConflictResolutionType").enummodule
          end
        end
      end
    end
  end
end
