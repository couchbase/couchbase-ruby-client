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

module Couchbase
  module Management
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
      # @param [GetAllScopesOptions] options
      #
      # @return [Array<ScopeSpec>]
      def get_all_scopes(options = GetAllScopesOptions.new)
        res = @backend.scope_get_all(@bucket_name, options.timeout)
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
        get_all_scopes(GetAllScopesOptions.new { |o| o.timeout = options.timeout })
          .find { |scope| scope.name == scope_name } or raise Error::ScopeNotFound, "unable to find scope #{scope_name}"
      end
      deprecate :get_scope, :get_all_scopes, 2021, 6

      # Creates a new scope
      #
      # @param [String] scope_name name of the scope
      # @param [CreateScopeOptions] options
      #
      # @return void
      #
      # @raise [ArgumentError]
      def create_scope(scope_name, options = CreateScopeOptions.new)
        @backend.scope_create(@bucket_name, scope_name, options.timeout)
      end

      # Removes a scope
      #
      # @param [String] scope_name name of the scope
      # @param [DropScopeOptions] options
      #
      # @return void
      #
      # @raise [Error::ScopeNotFound]
      def drop_scope(scope_name, options = DropScopeOptions.new)
        @backend.scope_drop(@bucket_name, scope_name, options.timeout)
      end

      # Creates a new collection
      #
      # @param [CollectionSpec] collection specification of the collection
      # @param [CreateCollectionOptions] options
      #
      # @return void
      #
      # @raise [ArgumentError]
      # @raise [Error::CollectionExist]
      # @raise [Error::ScopeNotFound]
      def create_collection(collection, options = CreateCollectionOptions.new)
        @backend.collection_create(@bucket_name, collection.scope_name, collection.name, collection.max_expiry, options.timeout)
      end

      # Removes a collection
      #
      # @param [CollectionSpec] collection specification of the collection
      # @param [DropCollectionOptions] options
      #
      # @return void
      #
      # @raise [Error::CollectionNotFound]
      def drop_collection(collection, options = DropCollectionOptions.new)
        @backend.collection_drop(@bucket_name, collection.scope_name, collection.name, options.timeout)
      end

      class GetScopeOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [GetScopeOptions] self
        def initialize
          yield self if block_given?
        end
      end

      class GetAllScopesOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [GetAllScopesOptions] self
        def initialize
          yield self if block_given?
        end
      end

      class CreateScopeOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [CreateScopeOptions] self
        def initialize
          yield self if block_given?
        end
      end

      class DropScopeOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [DropScopeOptions] self
        def initialize
          yield self if block_given?
        end
      end

      class CreateCollectionOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [CreateCollectionOptions] self
        def initialize
          yield self if block_given?
        end
      end

      class DropCollectionOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [DropCollectionOptions] self
        def initialize
          yield self if block_given?
        end
      end
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
