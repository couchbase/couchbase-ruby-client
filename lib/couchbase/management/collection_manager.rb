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
            super
            yield self if block_given?
          end

          DEFAULT = GetAllScopes.new.freeze
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
            super
            yield self if block_given?
          end

          DEFAULT = CreateScope.new.freeze
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
            super
            yield self if block_given?
          end

          DEFAULT = DropScope.new.freeze
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
            super
            yield self if block_given?
          end

          DEFAULT = CreateCollection.new.freeze
        end

        # Options for {CollectionManager#update_collection}
        class UpdateCollection < ::Couchbase::Options::Base
          # Creates an instance of options for {CollectionManager#update_collection}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Hash, nil] client_context the client context data, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [UpdateCollection] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         client_context: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end

          DEFAULT = UpdateCollection.new.freeze
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
            super
            yield self if block_given?
          end

          DEFAULT = DropCollection.new.freeze
        end

        # rubocop:disable Naming/MethodName -- constructor shortcuts
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
                collection.max_expiry = c[:max_expiry]
                collection.history = c[:history]
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
      # @param [Options::Collection::DropScope] options
      #
      # @return void
      #
      # @raise [Error::ScopeNotFound]
      def drop_scope(scope_name, options = Options::Collection::DropScope.new)
        @backend.scope_drop(@bucket_name, scope_name, options.to_backend)
      end

      # Creates a new collection
      # @overload create_collection(scope_name, collection_name, settings = CreateCollectionSettings::DEFAULT,
      #  options = Options::Collection::CreateCollection::DEFAULT)
      #   @param [String] scope_name the name of the scope the collection will be created in
      #   @param [String] collection_name the name of the collection to be created
      #   @param [CreateCollectionSettings] settings settings for the new collection
      #   @param [Options::Collection::CreateCollection] options
      #
      # @overload create_collection(collection, options = Options::Collection::CreateCollection)
      #   @param [CollectionSpec] collection specification of the collection
      #   @param [Options::Collection::CreateCollection] options
      #
      #   @deprecated Use +#create_collection(scope_name, collection_name, settings, options)+ instead
      #
      # @return void
      #
      # @raise [ArgumentError]
      # @raise [Error::CollectionExists]
      # @raise [Error::ScopeNotFound]
      def create_collection(*args)
        if args[0].is_a?(CollectionSpec)
          collection = args[0]
          options = args[1] || Options::Collection::CreateCollection::DEFAULT
          settings = CreateCollectionSettings.new(max_expiry: collection.max_expiry, history: collection.history)

          warn "Calling create_collection with a CollectionSpec object has been deprecated, supply scope name, " \
               "collection name and optionally a CreateCollectionSettings instance"

          @backend.collection_create(@bucket_name, collection.scope_name, collection.name, settings.to_backend, options.to_backend)
        else
          scope_name = args[0]
          collection_name = args[1]
          settings = args[2] || CreateCollectionSettings::DEFAULT
          options = args[3] || Options::Collection::CreateCollection::DEFAULT
          @backend.collection_create(@bucket_name, scope_name, collection_name, settings.to_backend, options.to_backend)
        end
      end

      # Updates the settings of an existing collection
      #
      # @param [String] scope_name the name of the scope the collection is in
      # @param [String] collection_name the name of the collection to be updated
      # @param [UpdateCollectionSettings] settings the settings that should be updated
      #
      # @raise [ArgumentError]
      # @raise [Error::CollectionNotFound]
      # @raise [Error::ScopeNotFound]
      def update_collection(scope_name, collection_name, settings = UpdateCollectionSettings::DEFAULT,
                            options = Options::Collection::UpdateCollection::DEFAULT)
        @backend.collection_update(@bucket_name, scope_name, collection_name, settings.to_backend, options.to_backend)
      end

      # Removes a collection
      # @overload drop_collection(scope_name, collection_name, settings = CreateCollectionSettings::DEFAULT,
      #   options = Options::Collection::CreateCollection::DEFAULT)
      #   @param [String] scope_name the name of the scope the collection is in
      #   @param [String] collection_name the name of the collection to be removed
      #
      # @overload drop_collection(collection, options = Options::Collection::CreateCollection)
      #   @param [CollectionSpec] collection specification of the collection
      #   @param [Options::Collection::CreateCollection] options
      #
      #   @deprecated Use +#drop_collection(scope_name, collection_name, options)+ instead
      #
      # @return void
      #
      # @raise [ArgumentError]
      # @raise [Error::CollectionNotFound]
      # @raise [Error::ScopeNotFound]
      def drop_collection(*args)
        if args[0].is_a?(CollectionSpec)
          collection = args[0]
          options = args[1] || Options::Collection::CreateCollection::DEFAULT

          warn "Calling drop_collection with a CollectionSpec object has been deprecated, supply scope name and collection name"

          @backend.collection_drop(@bucket_name, collection.scope_name, collection.name, options.to_backend)
        else
          scope_name = args[0]
          collection_name = args[1]
          options = args[2] || Options::Collection::CreateCollection::DEFAULT
          @backend.collection_drop(@bucket_name, scope_name, collection_name, options.to_backend)
        end
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

    class CreateCollectionSettings
      # @return [Integer, nil] time in seconds of the maximum expiration time for new documents in the collection
      # (set to +nil+ to use the bucket-level setting, and to +-1+ set it to no-expiry)
      attr_accessor :max_expiry

      # @return [Boolean, nil] whether history retention override should be enabled in the collection (set to +nil+ to
      # default to the bucket-level setting)
      attr_accessor :history

      def initialize(max_expiry: nil, history: nil)
        @max_expiry = max_expiry
        @history = history

        yield self if block_given?
      end

      # @api private
      def to_backend
        {
          max_expiry: @max_expiry,
          history: @history,
        }
      end

      DEFAULT = CreateCollectionSettings.new.freeze
    end

    class UpdateCollectionSettings
      # @return [Integer, nil] time in seconds of the maximum expiration time for new documents in the collection
      # (set to +nil+ to not update it, and to +-1+ set it to no-expiry)
      attr_accessor :max_expiry

      # @return [Boolean, nil] whether history retention override should be enabled in the collection (set to +nil+ to
      # not update it)
      attr_accessor :history

      def initialize(max_expiry: nil, history: nil)
        @max_expiry = max_expiry
        @history = history

        yield self if block_given?
      end

      # @api private
      def to_backend
        {
          max_expiry: @max_expiry,
          history: @history,
        }
      end

      DEFAULT = UpdateCollectionSettings.new.freeze
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

      # @return [Boolean, nil] whether history retention is enabled for this collection
      attr_accessor :history

      # @yieldparam [CollectionSpec] self
      def initialize
        yield self if block_given?
      end
    end
  end
end
