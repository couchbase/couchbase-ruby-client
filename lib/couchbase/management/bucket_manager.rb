#    Copyright 2020 Couchbase, Inc.
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

require "couchbase/errors"

module Couchbase
  module Management
    class BucketManager
      alias_method :inspect, :to_s

      # @param [Couchbase::Backend] backend
      def initialize(backend)
        @backend = backend
      end

      # Creates new bucket
      #
      # @param [BucketSettings] settings bucket settings
      # @param [CreateBucketOptions] options
      #
      # @raise [ArgumentError]
      # @raise [Error::BucketExists]
      def create_bucket(settings, options = CreateBucketOptions.new)
        @backend.bucket_create(
            name: settings.name,
            flush_enabled: settings.flush_enabled,
            ram_quota_mb: settings.ram_quota_mb,
            num_replicas: settings.num_replicas,
            replica_indexes: settings.replica_indexes,
            bucket_type: settings.bucket_type,
            ejection_policy: settings.ejection_policy,
            max_expiry: settings.max_expiry,
            compression_mode: settings.compression_mode,
            conflict_resolution_type: settings.conflict_resolution_type,
        )
      end

      # Updates the bucket settings
      #
      # @param [BucketSettings] settings bucket settings
      # @param [UpdateBucketOptions] options
      #
      # @raise [ArgumentError]
      # @raise [Error::BucketNotFound]
      def update_bucket(settings, options = UpdateBucketOptions.new)
        @backend.bucket_update(
            name: settings.name,
            flush_enabled: settings.flush_enabled,
            ram_quota_mb: settings.ram_quota_mb,
            num_replicas: settings.num_replicas,
            replica_indexes: settings.replica_indexes,
            bucket_type: settings.bucket_type,
            ejection_policy: settings.ejection_policy,
            max_expiry: settings.max_expiry,
            compression_mode: settings.compression_mode,
        )
      end

      # Removes a bucket
      #
      # @param [String] bucket_name name of the bucket
      # @param [DropBucketOptions] options
      #
      # @raise [ArgumentError]
      # @raise [Error::BucketNotFound]
      def drop_bucket(bucket_name, options = DropBucketOptions.new)
        @backend.bucket_drop(bucket_name)
      end

      # Fetch settings of the bucket
      #
      # @param [String] bucket_name name of the bucket
      # @param [GetBucketOptions] options
      #
      # @return [BucketSettings]
      #
      # @raise [ArgumentError]
      # @raise [Error::BucketNotFound]
      def get_bucket(bucket_name, options = GetBucketOptions.new)
        res = @backend.bucket_get(bucket_name)
        extract_bucket_settings(res)
      end

      # Get settings for all buckets
      #
      # @param [GetAllBucketsOptions] options
      # @return [Array<BucketSettings>]
      def get_all_buckets(options = GetAllBucketsOptions.new)
        res = @backend.bucket_get_all
        res.map(&method(:extract_bucket_settings))
      end

      # @param [String] bucket_name name of the bucket
      # @raise [ArgumentError]
      # @raise [Error::BucketNotFound]
      # @raise [Error::BucketNotFlushable]
      def flush_bucket(bucket_name, options = FlushBucketOptions.new)
        @backend.bucket_flush(bucket_name)
      end

      class CreateBucketOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        def initialize
          yield self if block_given?
        end
      end

      class UpdateBucketOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        def initialize
          yield self if block_given?
        end
      end

      class DropBucketOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        def initialize
          yield self if block_given?
        end
      end

      class GetBucketOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        def initialize
          yield self if block_given?
        end
      end

      class GetAllBucketsOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        def initialize
          yield self if block_given?
        end
      end

      class FlushBucketOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        def initialize
          yield self if block_given?
        end
      end

      private

      def extract_bucket_settings(entry)
        BucketSettings.new do |bucket|
          bucket.name = entry[:name]
          bucket.flush_enabled = entry[:flush_enabled]
          bucket.ram_quota_mb = entry[:ram_quota_mb]
          bucket.num_replicas = entry[:num_replicas]
          bucket.replica_indexes = entry[:replica_indexes]
          bucket.bucket_type = entry[:bucket_type]
          bucket.max_expiry = entry[:max_expiry]
          bucket.ejection_policy = entry[:max_expiry]
          bucket.compression_mode = entry[:compression_mode]
          bucket.instance_variable_set("@healthy", entry[:nodes].all? { |node| node[:status] == "healthy" })
        end
      end
    end

    class BucketSettings
      # @return [String] name of the bucket
      attr_accessor :name

      # @return [Boolean] whether or not flush should be enabled on the bucket. Defaults to false.
      attr_accessor :flush_enabled

      # @return [Integer] RAM quota in megabytes for the bucket
      attr_accessor :ram_quota_mb

      # @return [Integer] number of replicas for documents
      attr_accessor :num_replicas

      # @return [Boolean] whether replica indexes should be enabled for the bucket
      attr_accessor :replica_indexes

      # @return [:couchbase, :memcached, :ephemeral] the type of the bucket. Defaults to +:couchbase+
      attr_accessor :bucket_type

      # @return [:full, :value_only] the eviction policy to use
      attr_accessor :ejection_policy

      # @return [Integer] value of TTL (expiration) in seconds for new documents created without expiration
      attr_accessor :max_expiry

      # @return [:off, :passive, :active] the compression mode to use
      attr_accessor :compression_mode

      # @return [:timestamp, :sequence_number] conflict resolution policy
      attr_accessor :conflict_resolution_type

      # @api private
      # @return [Boolean] false if status of the bucket is not healthy
      def healthy?
        @healthy
      end

      # @yieldparam [BucketSettings] self
      def initialize
        @bucket_type = :couchbase
        @name = nil
        @healthy = true
        @flush_enabled = false
        @ram_quota_mb = 100
        @num_replicas = 1
        @replica_indexes = false
        @max_expiry = 0
        @compression_mode = :passive
        @conflict_resolution_type = :sequence_number
        @ejection_policy = :value_only
        yield self if block_given?
      end
    end
  end
end
