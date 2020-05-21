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
      # @param [CreateBucketSettings] settings bucket settings
      # @param [CreateBucketOptions] options
      #
      # @raise [ArgumentError]
      # @raise [Error::BucketExists]
      def create_bucket(settings, options = CreateBucketOptions.new)
        # POST /pools/default/buckets
      end

      # Updates the bucket settings
      #
      # @param [BucketSettings] settings bucket settings
      # @param [UpdateBucketOptions] options
      #
      # @raise [ArgumentError]
      # @raise [Error::BucketNotFound]
      def update_bucket(settings, options = UpdateBucketOptions.new)
        # POST /pools/default/buckets/#{settings.name}
      end

      # Removes a bucket
      #
      # @param [String] bucket_name name of the bucket
      # @param [DropBucketOptions] options
      #
      # @raise [ArgumentError]
      # @raise [Error::BucketNotFound]
      def drop_bucket(bucket_name, options = DropBucketOptions.new)
        # DELETE /pools/default/buckets/#{bucket_name}
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
        # GET /pools/default/buckets/#{bucket_name}
      end

      # Get settings for all buckets
      #
      # @param [GetAllBucketsOptions] options
      # @return [Array<BucketSettings>]
      def get_all_buckets(options = GetAllBucketsOptions.new)
        # GET /pools/default/buckets
      end

      # @param [String] bucket_name name of the bucket
      # @raise [ArgumentError]
      # @raise [Error::BucketNotFound]
      # @raise [Error::BucketNotFlushable]
      def flush_bucket(bucket_name, options = FlushBucketOptions.new)
        # POST /pools/default/buckets/#{bucket_name}/controller/doFlush
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

      # @return [:full_eviction, :value_only] the eviction policy to use
      attr_accessor :ejection_method

      # @return [Integer] value of TTL (expiration) in seconds for new documents created without TTL
      attr_accessor :max_ttl

      # @return [:off, :passive, :active] the compression mode to use
      attr_accessor :compression_mode

      def initialize
        obj.flush_enabled = true
        obj.bucket_type = :couchbase
        yield self if block_given?
      end
    end

    class CreateBucketSettings < BucketSettings
      # @return [:timestamp, :sequence_number] conflict resolution policy
      attr_accessor :conflict_resolution_type

      def initialize
        super()
        yield self if block_given?
      end
    end
  end
end