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
      module Collection
        # Options for {CollectionManager#get_all_scopes}
        class GetAllScopes < ::Couchbase::Options::Base
          # Creates an instance of options for {CollectionManager#get_all_scopes}
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
            super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
            yield self if block_given?
          end

          # @api private
          def to_backend
            {
              timeout: @timeout.respond_to?(:in_milliseconds) ? @timeout.public_send(:in_milliseconds) : @timeout,
            }
          end
        end

        # Options for {CollectionManager#create_scope}
        class CreateScope < ::Couchbase::Options::Base
          # Creates an instance of options for {CollectionManager#create_scope}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Hash, nil] client_context the client context data, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [CreateScope] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         client_context: nil,
                         parent_span: nil)
            super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
            yield self if block_given?
          end

          # @api private
          def to_backend
            {
              timeout: @timeout.respond_to?(:in_milliseconds) ? @timeout.public_send(:in_milliseconds) : @timeout,
            }
          end
        end

        # Options for {CollectionManager#drop_scope}
        class DropScope < ::Couchbase::Options::Base
          # Creates an instance of options for {CollectionManager#drop_scope}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Hash, nil] client_context the client context data, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [DropScope] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         client_context: nil,
                         parent_span: nil)
            super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
            yield self if block_given?
          end

          # @api private
          def to_backend
            {
              timeout: @timeout.respond_to?(:in_milliseconds) ? @timeout.public_send(:in_milliseconds) : @timeout,
            }
          end
        end

        # Options for {CollectionManager#create_collection}
        class CreateCollection < ::Couchbase::Options::Base
          # Creates an instance of options for {CollectionManager#create_collection}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Hash, nil] client_context the client context data, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [CreateCollection] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         client_context: nil,
                         parent_span: nil)
            super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
            yield self if block_given?
          end

          # @api private
          def to_backend
            {
              timeout: @timeout.respond_to?(:in_milliseconds) ? @timeout.public_send(:in_milliseconds) : @timeout,
            }
          end
        end

        # Options for {CollectionManager#drop_collection}
        class DropCollection < ::Couchbase::Options::Base
          # Creates an instance of options for {CollectionManager#drop_collection}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Hash, nil] client_context the client context data, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [DropCollection] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         client_context: nil,
                         parent_span: nil)
            super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
            yield self if block_given?
          end

          # @api private
          def to_backend
            {
              timeout: @timeout.respond_to?(:in_milliseconds) ? @timeout.public_send(:in_milliseconds) : @timeout,
            }
          end
        end

        # rubocop:disable Naming/MethodName constructor shortcuts
        module_function

        # Construct {GetAllScopes} options for {CollectionManager#get_all_scopes}
        #
        # @return [GetAllScopes]
        def GetAllScopes(**args)
          GetAllScopes.new(**args)
        end

        # Construct {CreateScope} options for {CollectionManager#create_scope}
        #
        # @return [CreateScope]
        def CreateScope(**args)
          CreateScope.new(**args)
        end

        # Construct {DropScope} options for {CollectionManager#drop_scope}
        #
        # @return [DropScope]
        def DropScope(**args)
          DropScope.new(**args)
        end

        # Construct {CreateCollection} options for {CollectionManager#create_collection}
        #
        # @return [CreateCollection]
        def CreateCollection(**args)
          CreateCollection.new(**args)
        end

        # Construct {DropCollection} options for {CollectionManager#drop_collection}
        #
        # @return [DropCollection]
        def DropCollection(**args)
          DropCollection.new(**args)
        end

        # rubocop:enable Naming/MethodName
      end
    end

    class CollectionManager
      extend Gem::Deprecate

      alias inspect to_s

      # @param [Couchbase::Backend] backend
      # @param [String] bucket_name
      def initialize(backend, bucket_name)
        @backend = backend
        @bucket_name = bucket_name
      end

      # Get all scopes
      #
      # @param [Options::Collection::GetAllScopes] options
      #
      # @return [Array<ScopeSpec>]
      def get_all_scopes(options = Options::Collection::GetAllScopes.new)
        res = @backend.scope_get_all(@bucket_name, options.to_backend)
        res[:scopes].map do |s|
          ScopeSpec.new do |scope|
            scope.name = s[:name]
            scope.collections = s[:collections].map do |c|
              CollectionSpec.new do |collection|
                collection.name = c[:name]
                collection.scope_name = s[:name]
              end
            end
          end
        end
      end

      # Get a scope by name
      #
      # @param [String] scope_name name of the scope
      # @param [GetScopeOptions] options
      #
      # @deprecated Use {#get_all_scopes} with filter by name
      #
      # @return [ScopeSpec]
      #
      # @raise [Error::ScopeNotFound]
      def get_scope(scope_name, options = GetScopeOptions.new)
        get_all_scopes(Options::Collection::GetAllScopes(timeout: options.timeout))
          .find { |scope| scope.name == scope_name } or raise Error::ScopeNotFound, "unable to find scope #{scope_name}"
      end

      deprecate :get_scope, :get_all_scopes, 2021, 6

      # Creates a new scope
      #
      # @param [String] scope_name name of the scope
      # @param [Options::Collection::CreateScope] options
      #
      # @return void
      #
      # @raise [ArgumentError]
      def create_scope(scope_name, options = Options::Collection::CreateScope.new)
        @backend.scope_create(@bucket_name, scope_name, options.to_backend)
      end

      # Removes a scope
      #
      # @param [String] scope_name name of the scope
      # @param [Option::Collection::DropScope] options
      #
      # @return void
      #
      # @raise [Error::ScopeNotFound]
      def drop_scope(scope_name, options = Option::Collection::DropScope.new)
        @backend.scope_drop(@bucket_name, scope_name, options.to_backend)
      end

      # Creates a new collection
      #
      # @param [CollectionSpec] collection specification of the collection
      # @param [Options::Collection::CreateCollection] options
      #
      # @return void
      #
      # @raise [ArgumentError]
      # @raise [Error::CollectionExist]
      # @raise [Error::ScopeNotFound]
      def create_collection(collection, options = Options::Collection::CreateCollection.new)
        @backend.collection_create(@bucket_name, collection.scope_name, collection.name, collection.max_expiry, options.to_backend)
      end

      # Removes a collection
      #
      # @param [CollectionSpec] collection specification of the collection
      # @param [Options::Collection::DropCollection] options
      #
      # @return void
      #
      # @raise [Error::CollectionNotFound]
      def drop_collection(collection, options = Options::Collection::DropCollection.new)
        @backend.collection_drop(@bucket_name, collection.scope_name, collection.name, options.to_backend)
      end

      # @deprecated use {CollectionManager#get_all_scopes} instead
      class GetScopeOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [GetScopeOptions] self
        def initialize
          yield self if block_given?
        end
      end

      # @api private
      # TODO: deprecate after 3.2
      GetAllScopesOptions = ::Couchbase::Management::Options::Collection::GetAllScopes

      # @api private
      # TODO: deprecate after 3.2
      CreateScopeOptions = ::Couchbase::Management::Options::Collection::CreateScope

      # @api private
      # TODO: deprecate after 3.2
      DropScopeOptions = ::Couchbase::Management::Options::Collection::DropScope

      # @api private
      # TODO: deprecate after 3.2
      CreateCollectionOptions = ::Couchbase::Management::Options::Collection::CreateCollection

      # @api private
      # TODO: deprecate after 3.2
      DropCollectionOptions = ::Couchbase::Management::Options::Collection::DropCollection
    end

    class ScopeSpec
      # @return [String] name of the scope
      attr_accessor :name

      # @return [Array<CollectionSpec>] list of collections associated with the scope
      attr_accessor :collections

      # @yieldparam [ScopeSpec] self
      def initialize
        yield self if block_given?
      end
    end

    class CollectionSpec
      # @return [String] name of the collection
      attr_accessor :name

      # @return [String] name of the scope
      attr_accessor :scope_name

      # @return [Integer] time in seconds of the expiration for new documents in the collection (set to +nil+ to disable it)
      attr_accessor :max_expiry

      # @yieldparam [CollectionSpec] self
      def initialize
        yield self if block_given?
      end
    end
  end
end
