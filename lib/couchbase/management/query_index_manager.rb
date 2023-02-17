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

require "couchbase/errors"
require "couchbase/options"
require "couchbase/utils/time"

module Couchbase
  module Management
    module Options
      module Query
        # Options for {QueryIndexManager#get_all_indexes}
        class GetAllIndexes < ::Couchbase::Options::Base
          # Creates an instance of options for {QueryIndexManager#get_all_indexes}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Hash, nil] client_context the client context data, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [GetAllScopes] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         client_context: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end
        end

        # Options for {QueryIndexManager#create_index}
        class CreateIndex < ::Couchbase::Options::Base
          attr_accessor :ignore_if_exists # @return [Boolean]
          attr_accessor :num_replicas # @return [Integer, nil]
          attr_accessor :deferred # @return [Boolean]
          attr_accessor :condition # @return [String, nil]
          attr_accessor :scope_name # @return [String, nil]
          attr_accessor :collection_name # @return [String, nil]

          # Creates an instance of options for {QueryIndexManager#create_index}
          #
          # @param [Boolean] ignore_if_exists do not raise error if the index already exist
          # @param [Integer] num_replicas the number of replicas that this index should have
          # @param [Boolean] deferred whether the index should be created as a deferred index.
          # @param [String, nil] condition to apply to the index
          # @param [String, nil] scope_name the name of the scope
          # @param [String, nil] collection_name the name of the collection
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Hash, nil] client_context the client context data, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [CreateIndex] self
          def initialize(ignore_if_exists: false,
                         num_replicas: nil,
                         deferred: false,
                         condition: nil,
                         scope_name: nil,
                         collection_name: nil,
                         timeout: nil,
                         retry_strategy: nil,
                         client_context: nil,
                         parent_span: nil)
            super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
            @ignore_if_exists = ignore_if_exists
            @num_replicas = num_replicas
            @deferred = deferred
            @condition = condition
            @scope_name = scope_name
            @collection_name = collection_name
            yield self if block_given?
          end

          # @api private
          def to_backend
            {
              timeout: Utils::Time.extract_duration(@timeout),
              ignore_if_exists: @ignore_if_exists,
              condition: @condition,
              deferred: @deferred,
              num_replicas: @num_replicas,
              scope_name: @scope_name,
              collection_name: @collection_name,
            }
          end
        end

        # Options for {QueryIndexManager#create_primary_index}
        class CreatePrimaryIndex < ::Couchbase::Options::Base
          attr_accessor :ignore_if_exists # @return [Boolean]
          attr_accessor :num_replicas # @return [Integer, nil]
          attr_accessor :deferred # @return [Boolean]
          attr_accessor :scope_name # @return [String, nil]
          attr_accessor :collection_name # @return [String, nil]

          # Creates an instance of options for {QueryIndexManager#create_primary_index}
          #
          # @param [Boolean] ignore_if_exists do not raise error if the index already exist
          # @param [Integer] num_replicas the number of replicas that this index should have
          # @param [Boolean] deferred whether the index should be created as a deferred index.
          # @param [String, nil] scope_name the name of the scope
          # @param [String, nil] collection_name the name of the collection
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Hash, nil] client_context the client context data, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [CreatePrimaryIndex] self
          def initialize(ignore_if_exists: false,
                         num_replicas: nil,
                         deferred: false,
                         scope_name: nil,
                         collection_name: nil,
                         timeout: nil,
                         retry_strategy: nil,
                         client_context: nil,
                         parent_span: nil)
            super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
            @ignore_if_exists = ignore_if_exists
            @num_replicas = num_replicas
            @deferred = deferred
            @scope_name = scope_name
            @collection_name = collection_name
            yield self if block_given?
          end

          # @api private
          def to_backend
            {
              timeout: Utils::Time.extract_duration(@timeout),
              ignore_if_exists: @ignore_if_exists,
              deferred: @deferred,
              num_replicas: @num_replicas,
              scope_name: @scope_name,
              collection_name: @collection_name,
            }
          end
        end

        # Options for {QueryIndexManager#drop_index}
        class DropIndex < ::Couchbase::Options::Base
          attr_accessor :ignore_if_does_not_exist # @return [Boolean]
          attr_accessor :scope_name # @return [String, nil]
          attr_accessor :collection_name # @return [String, nil]

          # Creates an instance of options for {QueryIndexManager#drop_index}
          #
          # @param [Boolean] ignore_if_does_not_exist do not raise error if the index does not exist
          # @param [String, nil] scope_name the name of the scope
          # @param [String, nil] collection_name the name of the collection
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Hash, nil] client_context the client context data, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [DropIndex] self
          def initialize(ignore_if_does_not_exist: false,
                         scope_name: nil,
                         collection_name: nil,
                         timeout: nil,
                         retry_strategy: nil,
                         client_context: nil,
                         parent_span: nil)
            super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
            @ignore_if_does_not_exist = ignore_if_does_not_exist
            @scope_name = scope_name
            @collection_name = collection_name
            yield self if block_given?
          end

          # @api private
          def to_backend
            {
              timeout: Utils::Time.extract_duration(@timeout),
              ignore_if_does_not_exist: @ignore_if_does_not_exist,
              scope_name: @scope_name,
              collection_name: @collection_name,
            }
          end
        end

        # Options for {QueryIndexManager#drop_primary_index}
        class DropPrimaryIndex < ::Couchbase::Options::Base
          attr_accessor :ignore_if_does_not_exist # @return [Boolean]
          attr_accessor :scope_name # @return [String, nil]
          attr_accessor :collection_name # @return [String, nil]

          # Creates an instance of options for {QueryIndexManager#drop_primary_index}
          #
          # @param [Boolean] ignore_if_does_not_exist do not raise error if the index does not exist
          # @param [String, nil] scope_name the name of the scope
          # @param [String, nil] collection_name the name of the collection
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Hash, nil] client_context the client context data, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [DropPrimaryIndex] self
          def initialize(ignore_if_does_not_exist: false,
                         scope_name: nil,
                         collection_name: nil,
                         timeout: nil,
                         retry_strategy: nil,
                         client_context: nil,
                         parent_span: nil)
            super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
            @ignore_if_does_not_exist = ignore_if_does_not_exist
            @scope_name = scope_name
            @collection_name = collection_name
            yield self if block_given?
          end

          # @api private
          def to_backend
            {
              timeout: Utils::Time.extract_duration(@timeout),
              ignore_if_does_not_exist: @ignore_if_does_not_exist,
              scope_name: @scope_name,
              collection_name: @collection_name,
            }
          end
        end

        # Options for {QueryIndexManager#build_deferred_indexes}
        class BuildDeferredIndexes < ::Couchbase::Options::Base
          # Creates an instance of options for {QueryIndexManager#build_deferred_indexes}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Hash, nil] client_context the client context data, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [GetAllScopes] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         client_context: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end
        end

        # Options for {QueryIndexManager#watch_indexes}
        class WatchIndexes < ::Couchbase::Options::Base
          attr_accessor :watch_primary # @return [Boolean]

          # Creates an instance of options for {QueryIndexManager#watch_indexes}
          #
          # @param [Boolean] watch_primary whether or not to watch the primary index
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Hash, nil] client_context the client context data, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [GetAllScopes] self
          def initialize(watch_primary: false,
                         retry_strategy: nil,
                         client_context: nil,
                         parent_span: nil)
            super(timeout: nil, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
            @watch_primary = watch_primary
            yield self if block_given?
          end

          # @api private
          def to_backend
            {
              watch_primary: @watch_primary,
            }
          end
        end

        # rubocop:disable Naming/MethodName constructor shortcuts
        module_function

        # Construct {GetAllIndexes} options for {QueryIndexManager#get_all_indexes}
        #
        # @return [GetAllIndexes]
        def GetAllIndexes(**args)
          GetAllIndexes.new(**args)
        end

        # Construct {CreateIndex} options for {QueryIndexManager#create_index}
        #
        # @return [CreateIndex]
        def CreateIndex(**args)
          CreateIndex.new(**args)
        end

        # Construct {CreatePrimaryIndex} options for {QueryIndexManager#create_index}
        #
        # @return [CreatePrimaryIndex]
        def CreatePrimaryIndex(**args)
          CreatePrimaryIndex.new(**args)
        end

        # Construct {DropIndex} options for {QueryIndexManager#drop_index}
        #
        # @return [DropIndex]
        def DropIndex(**args)
          DropIndex.new(**args)
        end

        # Construct {DropPrimaryIndex} options for {QueryIndexManager#drop_primary_index}
        #
        # @return [DropPrimaryIndex]
        def DropPrimaryIndex(**args)
          DropPrimaryIndex.new(**args)
        end

        # rubocop:enable Naming/MethodName
      end
    end

    class QueryIndexManager
      alias inspect to_s

      # @param [Couchbase::Backend] backend
      def initialize(backend)
        @backend = backend
      end

      # Fetches all indexes from the server
      #
      # @param [String] bucket_name name of the bucket
      # @param [Options::Query::GetAllIndexes] options
      #
      # @return [Array<QueryIndex>]
      #
      # @raise [ArgumentError]
      def get_all_indexes(bucket_name, options = GetAllIndexOptions.new)
        res = @backend.query_index_get_all(bucket_name, options.to_backend)
        res[:indexes].map do |idx|
          QueryIndex.new do |index|
            index.name = idx[:name]
            index.is_primary = idx[:is_primary]
            index.type = idx[:type]
            index.state = idx[:state]
            index.bucket = idx[:bucket_name]
            index.scope = idx[:scope_name]
            index.collection = idx[:collection_name]
            index.index_key = idx[:index_key]
            index.condition = idx[:condition]
            index.partition = idx[:partition]
          end
        end
      end

      # Creates a new index
      #
      # @param [String] bucket_name  name of the bucket
      # @param [String] index_name name of the index
      # @param [Array<String>] fields the lists of fields to create th index over
      # @param [Options::Query::CreateIndex] options
      #
      # @return void
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexExists]
      def create_index(bucket_name, index_name, fields, options = Options::Query::CreateIndex.new)
        @backend.query_index_create(bucket_name, index_name, fields, options.to_backend)
      end

      # Creates new primary index
      #
      # @param [String] bucket_name name of the bucket
      # @param [Options::Query::CreatePrimaryIndex] options
      #
      # @return void
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexExists]
      def create_primary_index(bucket_name, options = Options::Query::CreatePrimaryIndex.new)
        @backend.query_index_create_primary(bucket_name, options.to_backend)
      end

      # Drops the index
      #
      # @param [String] bucket_name name of the bucket
      # @param [String] index_name name of the index
      # @param [Options::Query::DropIndex] options
      #
      # @return void
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def drop_index(bucket_name, index_name, options = Options::Query::DropIndex.new)
        @backend.query_index_drop(bucket_name, index_name, options.to_backend)
        true
      end

      # Drops the primary index
      #
      # @param [String] bucket_name name of the bucket
      # @param [Options::Query::DropPrimaryIndex] options
      #
      # @return void
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def drop_primary_index(bucket_name, options = Options::Query::DropPrimaryIndex.new)
        @backend.query_index_drop_primary(bucket_name, options.to_backend)
        true
      end

      # Build all indexes which are currently in deferred state
      #
      # @param [String] bucket_name name of the bucket
      # @param [Options::Query::BuildDeferredIndexes] options
      #
      # @return void
      #
      # @raise [ArgumentError]
      def build_deferred_indexes(bucket_name, options = Options::Query::BuildDeferredIndexes.new)
        @backend.query_index_build_deferred(bucket_name, options.to_backend)
      end

      # Polls indexes until they are online
      #
      # @param [String] bucket_name name of the bucket
      # @param [Array<String>] index_names names of the indexes to watch
      # @param [Integer, #in_milliseconds] timeout the time in milliseconds allowed for the operation to complete
      # @param [Options::Query::WatchIndexes] options
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def watch_indexes(bucket_name, index_names, timeout, options = Options::Query::WatchIndexes.new)
        index_names.append("#primary") if options.watch_primary

        interval_millis = 50
        deadline = Time.now + (Utils::Time.extract_duration(timeout) * 0.001)
        while Time.now <= deadline
          get_all_opts = Options::Query::GetAllIndexes.new(timeout: ((deadline - Time.now) * 1000).round)
          indexes = get_all_indexes(bucket_name, get_all_opts).select { |idx| index_names.include? idx.name }
          indexes_not_found = index_names - indexes.map(&:name)
          raise Error::IndexNotFound, "Failed to find the indexes: #{indexes_not_found.join(', ')}" unless indexes_not_found.empty?

          all_online = indexes.all? { |idx| idx.state == :online }
          return if all_online

          sleep(interval_millis / 1000)
          interval_millis += 500
          interval_millis = 1000 if interval_millis > 1000
        end
        raise Error::UnambiguousTimeout, "Failed to find all indexes online within the allotted time"
      end

      # @api private
      # TODO: deprecate after 3.2
      GetAllIndexOptions = ::Couchbase::Management::Options::Query::GetAllIndexes

      # @api private
      # TODO: deprecate after 3.2
      CreateIndexOptions = ::Couchbase::Management::Options::Query::CreateIndex

      # @api private
      # TODO: deprecate after 3.2
      CreatePrimaryIndexOptions = ::Couchbase::Management::Options::Query::CreatePrimaryIndex

      # @api private
      # TODO: deprecate after 3.2
      DropIndexOptions = ::Couchbase::Management::Options::Query::DropIndex

      # @api private
      # TODO: deprecate after 3.2
      DropPrimaryIndexOptions = ::Couchbase::Management::Options::Query::DropPrimaryIndex

      # @api private
      # TODO: deprecate after 3.2
      BuildDeferredIndexOptions = ::Couchbase::Management::Options::Query::BuildDeferredIndexes

      # @api private
      # TODO: deprecate after 3.2
      WatchIndexesOptions = ::Couchbase::Management::Options::Query::WatchIndexes
    end

    class QueryIndex
      # @return [String] name of the index
      attr_accessor :name

      # @return [Boolean] true if this is a primary index
      attr_accessor :is_primary
      alias primary? is_primary

      # @return [:gsi, :view] type of the index
      attr_accessor :type

      # @return [Symbol] state
      attr_accessor :state

      # @return [String, nil] the name of the bucket
      attr_accessor :bucket

      # @return [String, nil] the name of the scope
      attr_accessor :scope

      # @return [String, nil] the name of the collection
      attr_accessor :collection

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

      # @return [String] the string representation of the index's partition
      attr_accessor :partition

      # @yieldparam [QueryIndex] self
      def initialize
        yield self if block_given?
      end
    end
  end
end
