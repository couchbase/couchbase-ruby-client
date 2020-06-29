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

require "couchbase/scope"
require "couchbase/management/collection_manager"
require "couchbase/management/view_index_manager"
require "couchbase/view_options"

module Couchbase
  class Bucket
    # @return [String] name of the bucket
    attr_reader :name

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

    # Performs query to view index.
    #
    # @param [String] design_document_name name of the design document
    # @param [String] view_name name of the view to query
    # @param [ViewOptions] options
    #
    # @return [ViewQueryResult]
    def view_query(design_document_name, view_name, options = ViewOptions.new)
      resp = @backend.document_view(@name, design_document_name, view_name, options.namespace, {
          timeout: options.timeout,
          scan_consistency: options.scan_consistency,
          skip: options.skip,
          limit: options.limit,
          start_key: (JSON.generate(options.start_key) unless options.start_key.nil?),
          end_key: (JSON.generate(options.end_key) unless options.end_key.nil?),
          start_key_doc_id: options.start_key_doc_id,
          end_key_doc_id: options.end_key_doc_id,
          inclusive_end: options.inclusive_end,
          group: options.group,
          group_level: options.group_level,
          key: (JSON.generate(options.key) unless options.key.nil?),
          keys: options.keys&.map { |key| JSON.generate(key) },
          order: options.order,
          reduce: options.reduce,
          on_error: options.on_error,
          debug: options.debug,
      })
      ViewResult.new do |res|
        res.meta_data = ViewMetaData.new do |meta|
          meta.total_rows = resp[:meta][:total_rows]
          meta.debug_info = resp[:meta][:debug_info]
        end
        res.rows = resp[:rows].map do |entry|
          ViewRow.new do |row|
            row.id = entry[:id] if entry.key?(:id)
            row.key = JSON.parse(entry[:key])
            row.value = JSON.parse(entry[:value])
          end
        end
      end
    end

    # @return [Management::CollectionManager]
    def collections
      Management::CollectionManager.new(@backend, @name)
    end

    # @return [Management::ViewIndexManager]
    def view_indexes
      Management::ViewIndexManager.new(@backend, @name)
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
