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

require 'couchbase/scope'
require 'couchbase/management/collection_manager'

module Couchbase
  class Bucket
    alias_method :inspect, :to_s

    # @param [Couchbase::Backend] backend
    def initialize(backend, name)
      backend.open_bucket(name)
      @backend = backend
      @name = name
    end

    # Get default scope
    #
    # @return [Scope]
    def default_scope
      Scope.new(@backend, @name, :_default)
    end

    # Get a named scope
    #
    # @param [String] scope_name name of the scope
    #
    # @return [Scope]
    def scope(scope_name)
      Scope.new(@backend, @name, scope_name)
    end

    # Opens the named collection in the default scope of the bucket
    #
    # @param [String] collection_name name of the collection
    #
    # @return [Collection]
    def collection(collection_name)
      default_scope.collection(collection_name)
    end

    # Opens the default collection for this bucket
    #
    # @return [Collection]
    def default_collection
      default_scope.default_collection
    end

    # @return [Management::CollectionManager]
    def collections
      Management::CollectionManager.new(@backend, @name)
    end

    # Performs application-level ping requests against services in the couchbase cluster
    #
    # @return [PingResult]
    def ping(options = PingOptions.new) end

    class PingOptions
      # @return [String] Holds custom report id.
      attr_accessor :report_id

      # @return [Array<Symbol>] The service types to limit this diagnostics request
      attr_accessor :service_types

      # @return [Integer] the time in milliseconds allowed for the operation to complete
      attr_accessor :timeout

      def initialize
        @service_types = [:kv, :query, :analytics, :search, :views, :management]
        yield self if block_given?
      end
    end
  end
end
