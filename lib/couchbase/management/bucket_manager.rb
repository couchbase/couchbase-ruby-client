# frozen_string_literal: true

#  Copyright 2020-2021 Couchbase, Inc.
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

require "rubygems/deprecate"

require "couchbase/errors"
require "couchbase/options"

module Couchbase
  module Management
    module Options
      module Bucket
        # Options for {BucketManager#create_bucket}
        class CreateBucket < ::Couchbase::Options::Base
          # Creates an instance of options for {BucketManager#create_bucket}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Hash, nil] client_context the client context data, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [CreateBucket] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         client_context: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end
        end

        # Options for {BucketManager#update_bucket}
        class UpdateBucket < ::Couchbase::Options::Base
          # Creates an instance of options for {BucketManager#update_bucket}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Hash, nil] client_context the client context data, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [UpdateBucket] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         client_context: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end
        end

        # Options for {BucketManager#drop_bucket}
        class DropBucket < ::Couchbase::Options::Base
          # Creates an instance of options for {BucketManager#drop_bucket}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Hash, nil] client_context the client context data, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [DropBucket] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         client_context: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end
        end

        # Options for {BucketManager#get_bucket}
        class GetBucket < ::Couchbase::Options::Base
          # Creates an instance of options for {BucketManager#get_bucket}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Hash, nil] client_context the client context data, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [GetBucket] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         client_context: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end
        end

        # Options for {BucketManager#get_all_buckets}
        class GetAllBuckets < ::Couchbase::Options::Base
          # Creates an instance of options for {BucketManager#get_all_buckets}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Hash, nil] client_context the client context data, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [GetAllBuckets] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         client_context: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end
        end

        # Options for {BucketManager#flush_bucket}
        class FlushBucket < ::Couchbase::Options::Base
          # Creates an instance of options for {BucketManager#flush_bucket}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Hash, nil] client_context the client context data, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [FlushBucket] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         client_context: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end
        end

        # rubocop:disable Naming/MethodName -- constructor shortcuts
        module_function

        # Construct {CreateBucket} options for {BucketManager#create_bucket}
        #
        # @return [CreateBucket]
        def CreateBucket(**args)
          CreateBucket.new(**args)
        end

        # Construct {UpdateBucket} options for {BucketManager#update_bucket}
        #
        # @return [UpdateBucket]
        def UpdateBucket(**args)
          UpdateBucket.new(**args)
        end

        # Construct {DropBucket} options for {BucketManager#drop_bucket}
        #
        # @return [DropBucket]
        def DropBucket(**args)
          DropBucket.new(**args)
        end

        # Construct {GetBucket} options for {BucketManager#get_bucket}
        #
        # @return [GetBucket]
        def GetBucket(**args)
          GetBucket.new(**args)
        end

        # Construct {GetAllBuckets} options for {BucketManager#get_all_buckets}
        #
        # @return [GetAllBuckets]
        def GetAllBuckets(**args)
          GetAllBuckets.new(**args)
        end

        # Construct {FlushBucket} options for {BucketManager#flush_bucket}
        #
        # @return [FlushBucket]
        def FlushBucket(**args)
          FlushBucket.new(**args)
        end

        # rubocop:enable Naming/MethodName
      end
    end

    class BucketManager
      alias inspect to_s

      # @param [Couchbase::Backend] backend
      # @param [Couchbase::Observability::Wrapper] observability wrapper
      #
      # @api private
      def initialize(backend, observability)
        @backend = backend
        @observability = observability
      end

      # Creates new bucket
      #
      # @param [BucketSettings] settings bucket settings
      # @param [Options::Bucket::CreateBucket] options
      #
      # @return void
      #
      # @raise [ArgumentError]
      # @raise [Error::BucketExists]
      def create_bucket(settings, options = Options::Bucket::CreateBucket.new)
        @observability.record_operation(Observability::OP_BM_CREATE_BUCKET, options.parent_span, self, :management) do |obs_handler|
          obs_handler.add_bucket_name(settings.name)

          @backend.bucket_create(
            {
              name: settings.name,
              flush_enabled: settings.flush_enabled,
              ram_quota_mb: settings.ram_quota_mb,
              num_replicas: settings.num_replicas,
              replica_indexes: settings.replica_indexes,
              bucket_type: settings.bucket_type,
              eviction_policy: settings.eviction_policy,
              max_expiry: settings.max_expiry,
              compression_mode: settings.compression_mode,
              minimum_durability_level: settings.minimum_durability_level,
              conflict_resolution_type: settings.conflict_resolution_type,
              storage_backend: settings.storage_backend,
              history_retention_collection_default: settings.history_retention_collection_default,
              history_retention_duration: settings.history_retention_duration,
              history_retention_bytes: settings.history_retention_bytes,
              num_vbuckets: settings.num_vbuckets,
            },
            options.to_backend,
            obs_handler,
          )
        end
      end

      # Updates the bucket settings
      #
      # @param [BucketSettings] settings bucket settings
      # @param [Options::Bucket::UpdateBucket] options
      #
      # @return void
      #
      # @raise [ArgumentError]
      # @raise [Error::BucketNotFound]
      def update_bucket(settings, options = Options::Bucket::UpdateBucket.new)
        @observability.record_operation(Observability::OP_BM_UPDATE_BUCKET, options.parent_span, self, :management) do |obs_handler|
          obs_handler.add_bucket_name(settings.name)

          @backend.bucket_update(
            {
              name: settings.name,
              flush_enabled: settings.flush_enabled,
              ram_quota_mb: settings.ram_quota_mb,
              num_replicas: settings.num_replicas,
              replica_indexes: settings.replica_indexes,
              bucket_type: settings.bucket_type,
              eviction_policy: settings.eviction_policy,
              max_expiry: settings.max_expiry,
              compression_mode: settings.compression_mode,
              minimum_durability_level: settings.minimum_durability_level,
              storage_backend: settings.storage_backend,
              history_retention_collection_default: settings.history_retention_collection_default,
              history_retention_bytes: settings.history_retention_bytes,
              history_retention_duration: settings.history_retention_duration,
              num_vbuckets: settings.num_vbuckets,
            },
            options.to_backend,
            obs_handler,
          )
        end
      end

      # Removes a bucket
      #
      # @param [String] bucket_name name of the bucket
      # @param [Options::Bucket::DropBucket] options
      #
      # @return void
      #
      # @raise [ArgumentError]
      # @raise [Error::BucketNotFound]
      def drop_bucket(bucket_name, options = Options::Bucket::DropBucket.new)
        @observability.record_operation(Observability::OP_BM_DROP_BUCKET, options.parent_span, self, :management) do |obs_handler|
          obs_handler.add_bucket_name(bucket_name)

          @backend.bucket_drop(bucket_name, options.to_backend, obs_handler)
        end
      end

      # Fetch settings of the bucket
      #
      # @param [String] bucket_name name of the bucket
      # @param [Options::Bucket::GetBucket] options
      #
      # @return [BucketSettings]
      #
      # @raise [ArgumentError]
      # @raise [Error::BucketNotFound]
      def get_bucket(bucket_name, options = Options::Bucket::GetBucket.new)
        @observability.record_operation(Observability::OP_BM_GET_BUCKET, options.parent_span, self, :management) do |obs_handler|
          obs_handler.add_bucket_name(bucket_name)

          extract_bucket_settings(@backend.bucket_get(bucket_name, options.to_backend, obs_handler))
        end
      end

      # Get settings for all buckets
      #
      # @param [Options::Bucket::GetAllBuckets] options
      # @return [Array<BucketSettings>]
      def get_all_buckets(options = Options::Bucket::GetAllBuckets.new)
        @observability.record_operation(Observability::OP_BM_GET_ALL_BUCKETS, options.parent_span, self, :management) do |obs_handler|
          @backend.bucket_get_all(options.to_backend, obs_handler)
                  .map { |entry| extract_bucket_settings(entry) }
        end
      end

      # @param [String] bucket_name name of the bucket
      # @param [Options::Bucket::FlushBucket] options
      #
      # @return void
      #
      # @raise [ArgumentError]
      # @raise [Error::BucketNotFound]
      # @raise [Error::BucketNotFlushable]
      def flush_bucket(bucket_name, options = Options::Bucket::FlushBucket.new)
        @observability.record_operation(Observability::OP_BM_FLUSH_BUCKET, options.parent_span, self, :management) do |obs_handler|
          @backend.bucket_flush(bucket_name, options.to_backend, obs_handler)
        end
      end

      # @api private
      # TODO: deprecate after 3.2
      CreateBucketOptions = ::Couchbase::Management::Options::Bucket::CreateBucket

      # @api private
      # TODO: deprecate after 3.2
      UpdateBucketOptions = ::Couchbase::Management::Options::Bucket::UpdateBucket

      # @api private
      # TODO: deprecate after 3.2
      DropBucketOptions = ::Couchbase::Management::Options::Bucket::DropBucket

      # @api private
      # TODO: deprecate after 3.2
      GetBucketOptions = ::Couchbase::Management::Options::Bucket::GetBucket

      # @api private
      # TODO: deprecate after 3.2
      GetAllBucketsOptions = ::Couchbase::Management::Options::Bucket::GetAllBuckets

      # @api private
      # TODO: deprecate after 3.2
      FlushBucketOptions = ::Couchbase::Management::Options::Bucket::FlushBucket

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
          bucket.eviction_policy = entry[:eviction_policy]
          bucket.minimum_durability_level = entry[:minimum_durability_level]
          bucket.compression_mode = entry[:compression_mode]
          bucket.instance_variable_set(:@healthy, entry[:nodes].all? { |node| node[:status] == "healthy" })
          bucket.storage_backend = entry[:storage_backend]
          bucket.history_retention_collection_default = entry[:history_retention_collection_default]
          bucket.history_retention_bytes = entry[:history_retention_bytes]
          bucket.history_retention_duration = entry[:history_retention_duration]
          bucket.num_vbuckets = entry[:num_vbuckets]
        end
      end
    end

    class BucketSettings
      extend Gem::Deprecate

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

      # @return [nil, :couchstore, :magma] the type of the storage backend of the bucket
      attr_accessor :storage_backend

      # Eviction policy to use
      #
      # :full:: During ejection, only the value will be ejected (key and metadata will remain in memory). Value Ejection
      #         needs more system memory, but provides better performance than Full Ejection. This value is only valid for
      #         buckets of type +:couchbase+.
      #
      # :value_only:: During ejection, everything (including key, metadata, and value) will be ejected. Full Ejection
      #               reduces the memory overhead requirement, at the cost of performance. This value is only valid for
      #               buckets of type +:couchbase+.
      #
      # :no_eviction:: Couchbase Server keeps all data until explicitly deleted, but will reject any new data if you
      #                reach the quota (dedicated memory) you set for your bucket. This value is only valid for buckets
      #                of type +:ephemeral+.
      #
      # :not_recently_used:: When the memory quota is reached, Couchbase Server ejects data that has not been used
      #                      recently. This value is only valid for buckets of type +:ephemeral+.
      #
      # @return [:full, :value_only, :no_eviction, :not_recently_used] the eviction policy to use
      attr_accessor :eviction_policy

      # @return [Integer] value of TTL (expiration) in seconds for new documents created without expiration
      attr_accessor :max_expiry

      # @return [:off, :passive, :active] the compression mode to use
      attr_accessor :compression_mode

      # @return [:timestamp, :sequence_number, :custom] conflict resolution policy
      attr_accessor :conflict_resolution_type

      # @return [nil, :none, :majority, :majority_and_persist_to_active, :persist_to_majority] the minimum durability level
      attr_accessor :minimum_durability_level

      # @return [Boolean, nil] whether to enable history retention on collections by default
      attr_accessor :history_retention_collection_default

      # @return [Integer, nil] the maximum size, in bytes, of the change history that is written to disk for all
      # collections in this bucket
      attr_accessor :history_retention_bytes

      # @return [Integer, nil] the maximum duration, in seconds, to be covered by the change history that is written to disk for all
      # collections in this bucket
      attr_accessor :history_retention_duration

      # @return [Integer, nil] the number of vBuckets the bucket should have. If not set, the server default will be used
      attr_accessor :num_vbuckets

      # @api private
      # @return [Boolean] false if status of the bucket is not healthy
      def healthy?
        @healthy
      end

      # @deprecated Use {#eviction_policy} instead
      def ejection_policy
        @eviction_policy
      end

      deprecate :ejection_policy, :eviction_policy, 2021, 1

      # @deprecated Use {#eviction_policy=} instead
      def ejection_policy=(val)
        @eviction_policy = val
      end

      deprecate :ejection_policy=, :eviction_policy=, 2021, 1

      # @yieldparam [BucketSettings] self
      def initialize
        yield self if block_given?
      end
    end
  end
end
