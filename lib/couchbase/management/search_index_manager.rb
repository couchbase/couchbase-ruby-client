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

module Couchbase
  module Management
    class SearchIndexManager
      alias inspect to_s

      # @param [Couchbase::Backend] backend
      def initialize(backend)
        @backend = backend
      end

      # Fetches an index from the server if it exists
      #
      # @param [String] index_name name of the index
      # @param [GetIndexOptions] options
      #
      # @return [SearchIndex]
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def get_index(index_name, options = GetIndexOptions.new)
        res = @backend.search_index_get(nil, nil, index_name, options.timeout)
        self.class.extract_search_index(res)
      end

      # Fetches all indexes from the server
      #
      # @param [GetAllIndexesOptions] options
      #
      # @return [Array<SearchIndex>]
      def get_all_indexes(options = GetAllIndexesOptions.new)
        res = @backend.search_index_get_all(nil, nil, options.timeout)
        res[:indexes].map { |idx| self.class.extract_search_index(idx) }
      end

      # Creates or updates the index
      #
      # @param [SearchIndex] index_definition the index definition
      # @param [UpsertIndexOptions] options
      #
      # @return void
      #
      # @raise [ArgumentError] if name, type or source_type is empty
      def upsert_index(index_definition, options = UpsertIndexOptions.new)
        @backend.search_index_upsert(
          nil,
          nil,
          {
            name: index_definition.name,
            type: index_definition.type,
            uuid: index_definition.uuid,
            params: (JSON.generate(index_definition.params) if index_definition.params),
            source_name: index_definition.source_name,
            source_type: index_definition.source_type,
            source_uuid: index_definition.source_uuid,
            source_params: (JSON.generate(index_definition.source_params) if index_definition.source_params),
            plan_params: (JSON.generate(index_definition.plan_params) if index_definition.plan_params),
          }, options.timeout
        )
      end

      # Drops the index
      #
      # @param [String] index_name name of the index
      # @param [DropIndexOptions] options
      #
      # @return void
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def drop_index(index_name, options = DropIndexOptions.new)
        @backend.search_index_drop(nil, nil, index_name, options.timeout)
      end

      # Retrieves the number of documents that have been indexed for an index
      #
      # @param [String] index_name name of the index
      # @param [GetIndexedDocumentsCountOptions] options
      #
      # @return [Integer]
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def get_indexed_documents_count(index_name, options = GetIndexedDocumentsCountOptions.new)
        res = @backend.search_index_get_documents_count(nil, nil, index_name, options.timeout)
        res[:count]
      end

      # Retrieves metrics, timings and counters for a given index
      #
      # @api uncommitted
      #
      # @param [String] index_name name of the index
      # @param [GetIndexStatsOptions] options
      #
      # @return [Integer]
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def get_index_stats(index_name, options = GetIndexStatsOptions.new)
        res = @backend.search_index_get_stats(index_name, options.timeout)
        JSON.parse(res)
      end

      # Retrieves statistics on search service. Information is provided on documents, partition indexes, mutations,
      # compactions, queries, and more.
      #
      # @api uncommitted
      #
      # @param [GetIndexStatsOptions] options
      #
      # @return [Integer]
      #
      # @raise [ArgumentError]
      def get_stats(options = GetIndexStatsOptions.new)
        res = @backend.search_get_stats(options.timeout)
        JSON.parse(res)
      end

      # Pauses updates and maintenance for the index
      #
      # @param [String] index_name name of the index
      # @param [PauseIngestOptions] options
      #
      # @return void
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def pause_ingest(index_name, options = PauseIngestOptions.new)
        @backend.search_index_pause_ingest(nil, nil, index_name, options.timeout)
      end

      # Resumes updates and maintenance for an index
      #
      # @param [String] index_name name of the index
      # @param [ResumeIngestOptions] options
      #
      # @return void
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def resume_ingest(index_name, options = ResumeIngestOptions.new)
        @backend.search_index_resume_ingest(nil, nil, index_name, options.timeout)
      end

      # Allows querying against the index
      #
      # @param [String] index_name name of the index
      # @param [AllowQueryingOptions] options
      #
      # @return void
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def allow_querying(index_name, options = AllowQueryingOptions.new)
        @backend.search_index_allow_querying(nil, nil, index_name, options.timeout)
      end

      # Disallows querying against the index
      #
      # @param [String] index_name name of the index
      # @param [DisallowQueryingOptions] options
      #
      # @return void
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def disallow_querying(index_name, options = DisallowQueryingOptions.new)
        @backend.search_index_disallow_querying(nil, nil, index_name, options.timeout)
      end

      # Freeze the assignment of index partitions to nodes
      #
      # @param [String] index_name name of the index
      # @param [FreezePlanOptions] options
      #
      # @return void
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def freeze_plan(index_name, options = FreezePlanOptions.new)
        @backend.search_index_freeze_plan(nil, nil, index_name, options.timeout)
      end

      # Unfreeze the assignment of index partitions to nodes
      #
      # @param [String] index_name name of the index
      # @param [UnfreezePlanOptions] options
      #
      # @return void
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def unfreeze_plan(index_name, options = UnfreezePlanOptions.new)
        @backend.search_index_unfreeze_plan(nil, nil, index_name, options.timeout)
      end

      # Allows to see how a document is analyzed against a specific index
      #
      # @param [String] index_name name of the index
      # @param [Hash] document the document to be analyzed
      #
      # @return [Array<Hash>]
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def analyze_document(index_name, document, options = AnalyzeDocumentOptions.new)
        res = @backend.search_index_analyze_document(nil, nil, index_name, JSON.generate(document), options.timeout)
        JSON.parse(res[:analysis])
      end

      class GetIndexOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [GetIndexOptions] self
        def initialize
          yield self if block_given?
        end
      end

      class GetAllIndexesOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [GetAllIndexesOptions] self
        def initialize
          yield self if block_given?
        end
      end

      class UpsertIndexOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [UpsertIndexOptions] self
        def initialize
          yield self if block_given?
        end
      end

      class DropIndexOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [DropIndexOptions] self
        def initialize
          yield self if block_given?
        end
      end

      class GetIndexedDocumentsCountOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [GetIndexedDocumentCountOptions] self
        def initialize
          yield self if block_given?
        end
      end

      class GetIndexStatsOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [GetStatsOptions] self
        def initialize
          yield self if block_given?
        end
      end

      class PauseIngestOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [PauseIngestOptions] self
        def initialize
          yield self if block_given?
        end
      end

      class ResumeIngestOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [ResumeIngestOptions] self
        def initialize
          yield self if block_given?
        end
      end

      class AllowQueryingOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [AllowQueryingOptions] self
        def initialize
          yield self if block_given?
        end
      end

      class DisallowQueryingOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [DisallowQueryingOptions] self
        def initialize
          yield self if block_given?
        end
      end

      class FreezePlanOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [FreezePlanOptions] self
        def initialize
          yield self if block_given?
        end
      end

      class UnfreezePlanOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [UnfreezePlanOptions] self
        def initialize
          yield self if block_given?
        end
      end

      class AnalyzeDocumentOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [AnalyzeDocumentOptions] self
        def initialize
          yield self if block_given?
        end
      end

      # @api private
      def self.extract_search_index(resp)
        SearchIndex.new do |index|
          index.name = resp[:name]
          index.type = resp[:type]
          index.uuid = resp[:uuid]
          index.params = resp[:params] ? JSON.parse(resp[:params]) : {}
          index.source_name = resp[:source_name]
          index.source_type = resp[:source_type]
          index.source_uuid = resp[:source_uuid]
          index.source_params = resp[:source_params] ? JSON.parse(resp[:source_params]) : {}
          index.plan_params = resp[:plan_params] ? JSON.parse(resp[:plan_params]) : {}
        end
      end
    end

    class SearchIndex
      # @return [String] name of the index
      attr_accessor :name

      # @return [String] type of the index
      attr_accessor :type

      # @return [String] UUID is required for update. It provides means of ensuring consistency.
      attr_accessor :uuid

      # @return [Hash] index properties such as store type and mappings
      attr_accessor :params

      # @return [String] name of the source of the data for the index (e.g. bucket name)
      attr_accessor :source_name

      # @return [String] type of the data source
      attr_accessor :source_type

      # @return [String] the UUID of the ata source, this can be used to more tightly tie the index to a source
      attr_accessor :source_uuid

      # @return [Hash] extra parameters for the source. These are usually things like advanced connection and tuning.
      attr_accessor :source_params

      # @return [Hash] plan properties such a number of replicas and number of partitions
      attr_accessor :plan_params

      # @yieldparam [SearchIndex] self
      def initialize
        @type = "fulltext-index"
        @source_type = "couchbase"
        yield self if block_given?
      end

      # @api private
      def to_backend
        {
          name: name,
          type: type,
          uuid: uuid,
          params: (JSON.generate(params) if params),
          source_name: source_name,
          source_type: source_type,
          source_uuid: source_uuid,
          source_params: (JSON.generate(source_params) if source_params),
          plan_params: (JSON.generate(plan_params) if plan_params),
        }
      end
    end
  end
end
