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
    class CollectionManager
      alias_method :inspect, :to_s

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
        # GET /pools/default/buckets/{bucket}/collections
      end

      def get_scope(scope_name, options = GetScopeOptions.new) end

      # Creates a new scope
      #
      # @param [String] scope_name name of the scope
      # @param [CreateScopeOptions] options
      #
      # @raise [ArgumentError]
      def create_scope(scope_name, options = CreateScopeOptions.new)
        # POST /pools/default/buckets/{bucket}/collections -d name={scope_name}
      end

      # Removes a scope
      #
      # @param [String] scope_name name of the scope
      # @param [DropScopeOptions] options
      #
      # @raise [Error::ScopeNotFound]
      def drop_scope(scope_name, options = DropScopeOptions.new)
        # DELETE /pools/default/buckets/{bucket}/collections/{scope_name}
      end

      # Creates a new collection
      #
      # @param [CollectionSpec] collection specification of the collection
      # @param [CreateCollectionOptions] options
      #
      # @raise [ArgumentError]
      # @raise [Error::CollectionExist]
      # @raise [Error::ScopeNotFound]
      def create_collection(collection, options = CreateCollectionOptions.new)
        # POST /pools/default/buckets/{bucket}/collections/{scope_name} -d name={collection_name} -D maxTTL={maxTTL}
      end

      # Removes a collection
      #
      # @param [CollectionSpec] collection specification of the collection
      # @param [DropCollectionOptions] options
      #
      # @raise [Error::CollectionNotFound]
      def drop_collection(collection, options = DropCollectionOptions.new)
        # DELETE /pools/default/buckets/{bucket}/collections/{scope_name}/{collection_name}
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
      attr_reader :collections

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
