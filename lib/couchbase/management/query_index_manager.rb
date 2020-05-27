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
    class QueryIndexManager
      alias_method :inspect, :to_s

      # @param [Couchbase::Backend] backend
      def initialize(backend)
        @backend = backend
      end

      # Fetches all indexes from the server
      #
      # @param [String] bucket_name name of the bucket
      # @param [GetAllIndexOptions] options
      #
      # @return [Array<QueryIndex>]
      #
      # @raise [ArgumentError]
      def get_all_indexes(bucket_name, options = GetAllIndexOptions.new)
        res = @backend.query_index_get_all(bucket_name)
        res[:indexes].map do |idx|
          QueryIndex.new do |index|
            index.name = idx[:name]
            index.is_primary = idx[:is_primary]
            index.type = idx[:type]
            index.state = idx[:state]
            index.key_space = idx[:keyspace_id]
            index.name_space = idx[:namespace_id]
            index.index_key = idx[:index_key]
            index.condition = idx[:condition]
          end
        end
      end

      # Creates a new index
      #
      # @param [String] bucket_name  name of the bucket
      # @param [String] index_name name of the index
      # @param [Array<String>] fields the lists of fields to create th index over
      # @param [CreateIndexOptions] options
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexExists]
      def create_index(bucket_name, index_name, fields, options = CreateIndexOptions.new)
        @backend.query_index_create(bucket_name, index_name, fields, {
            ignore_if_exists: options.ignore_if_exists,
            condition: options.condition,
            deferred: options.deferred,
            num_replicas: options.num_replicas,
        })
      end

      # Creates new primary index
      #
      # @param [String] bucket_name name of the bucket
      # @param [CreatePrimaryIndexOptions] options
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexExists]
      def create_primary_index(bucket_name, options = CreatePrimaryIndexOptions.new)
        @backend.query_index_create_primary(bucket_name, {
            ignore_if_exists: options.ignore_if_exists,
            deferred: options.deferred,
            num_replicas: options.num_replicas,
        })
      end

      # Drops the index
      #
      # @param [String] bucket_name name of the bucket
      # @param [String] index_name name of the index
      # @param [DropIndexOptions] options
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def drop_index(bucket_name, index_name, options = DropIndexOptions.new)
        @backend.query_index_drop(bucket_name, index_name, {
            ignore_if_does_not_exist: options.ignore_if_does_not_exist,
        })
        true
      end

      # Drops the primary index
      #
      # @param [String] bucket_name name of the bucket
      # @param [DropPrimaryIndexOptions] options
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def drop_primary_index(bucket_name, options = DropPrimaryIndexOptions.new)
        @backend.query_index_drop_primary(bucket_name, {
            ignore_if_does_not_exist: options.ignore_if_does_not_exist,
            index_name: options.index_name,
        })
        true
      end

      # Build all indexes which are currently in deferred state
      #
      # @param [String] bucket_name name of the bucket
      # @param [BuildDeferredIndexOptions] options
      #
      # @raise [ArgumentError]
      def build_deferred_indexes(bucket_name, options = BuildDeferredIndexOptions.new)
        @backend.query_index_build_deferred(bucket_name, {})
      end

      # Polls indexes until they are online
      #
      # @param [String] bucket_name name of the bucket
      # @param [Array<String>] index_names names of the indexes to watch
      # @param [Integer] timeout
      # @param [WatchIndexesOptions] options
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def watch_indexes(bucket_name, index_names, timeout, options = WatchIndexesOptions.new)
        @backend.query_index_watch(bucket_name, index_names, timeout, {})
      end

      class GetAllIndexOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [GetAllIndexOptions] self
        def initialize
          yield self if block_given?
        end
      end

      class CreateIndexOptions
        # @return [Boolean] do not raise error if the index already exist
        attr_accessor :ignore_if_exists

        # @return [Integer] the number of replicas that this index should have
        attr_accessor :num_replicas

        # @return [Boolean] whether the index should be created as a deferred index.
        attr_accessor :deferred

        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @return [String] condition to apply to the index
        attr_accessor :condition

        # @yieldparam [CreateIndexOptions] self
        def initialize
          @ignore_if_exists = false
          yield self if block_given?
        end
      end

      class CreatePrimaryIndexOptions
        # @return [String] name of the index
        attr_accessor :index_name

        # @return [Boolean] do not raise error if the index already exist
        attr_accessor :ignore_if_exists

        # @return [Integer] the number of replicas that this index should have
        attr_accessor :num_replicas

        # @return [Boolean] whether the index should be created as a deferred index.
        attr_accessor :deferred

        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [CreatePrimaryIndexOptions] self
        def initialize
          @ignore_if_exists = false
          yield self if block_given?
        end
      end

      class DropIndexOptions
        # @return [Boolean] do not raise error if the index does not exist
        attr_accessor :ignore_if_does_not_exist

        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [DropIndexOptions] self
        def initialize
          @ignore_if_does_not_exist = false
          yield self if block_given?
        end
      end

      class DropPrimaryIndexOptions
        # @return [String] name of the index
        attr_accessor :index_name

        # @return [Boolean] do not raise error if the index does not exist
        attr_accessor :ignore_if_does_not_exist

        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [DropPrimaryIndexOptions] self
        def initialize
          @ignore_if_does_not_exist = false
          yield self if block_given?
        end
      end

      class WatchIndexesOptions
        # @return [Boolean] whether or not to watch the primary index
        attr_accessor :watch_primary

        # @yieldparam [WatchIndexesOptions] self
        def initialize
          yield self if block_given?
        end
      end

      class BuildDeferredIndexOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [BuildDeferredIndexOptions] self
        def initialize
          yield self if block_given?
        end
      end
    end

    class QueryIndex
      # @return [String] name of the index
      attr_accessor :name

      # @return [Boolean] true if this is a primary index
      attr_accessor :is_primary
      alias_method :primary?, :is_primary

      # @return [:gsi, :view] type of the index
      attr_accessor :type

      # @return [Symbol] state
      attr_accessor :state

      # @return [String] the key space for the index, typically the bucket name.
      attr_accessor :key_space

      # @return [String] the namespace for the index. A namespace is a resource pool that contains multiple key spaces.
      attr_accessor :name_space

      # @return [Array<String>] an array of Strings that represent the index key(s). The array is empty in the case of a
      #   PRIMARY INDEX.
      #
      # @note the query service can present the key in a slightly different manner from when you declared the index: for
      #   instance, it will show the indexed fields in an escaped format (surrounded by backticks).
      attr_accessor :index_key


      # @return [String] the string representation of the index's condition (the WHERE clause of the index),
      #   or an empty Optional if no condition was set.
      #
      # @note that the query service can present the condition in a slightly different manner from when you declared the
      #   index. For instance it will wrap expressions with parentheses and show the fields in an escaped format
      #   (surrounded by backticks).
      attr_accessor :condition

      # @yieldparam [QueryIndex] self
      def initialize
        yield self if block_given?
      end
    end
  end
end
