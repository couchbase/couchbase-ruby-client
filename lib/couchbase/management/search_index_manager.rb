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
    class SearchIndexManager
      alias_method :inspect, :to_s

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
        # GET /api/index/{name}
      end

      # Fetches all indexes from the server
      #
      # @param [GetAllIndexesOptions] options
      #
      # @return [Array<SearchIndex>]
      def get_all_indexes(options = GetAllIndexesOptions.new)
        # GET /api/index
      end

      # Creates or updates the index
      #
      # @param [SearchIndex] index_definition the index definition
      # @param [UpsertIndexOptions] options
      #
      # @raise [ArgumentError] if name, type or source_type is empty
      def upsert_index(index_definition, options = UpsertIndexOptions.new)
        # PUT /api/index/{index_name} "cache-control: no-cache"
      end

      # Drops the index
      #
      # @param [String] index_name name of the index
      # @param [DropIndexOptions] options
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def drop_index(index_name, options = DropIndexOptions.new)
        # DELETE /api/index/{index_name}
      end

      # Retrieves the number of documents that have been indexed for an index
      #
      # @param [String] index_name name of the index
      # @param [GetIndexedDocumentsOptions] options
      #
      # @return [Integer]
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def get_indexed_documents_count(index_name, options = GetIndexedDocumentsCountOptions.new)
        # GET /api/index/{index_name}/count
      end

      # Pauses updates and maintenance for the index
      #
      # @param [String] index_name name of the index
      # @param [PauseIngestOptions] options
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def pause_ingest(index_name, options = PauseIngestOptions.new)
        # POST /api/index/{index_name}/ingestControl/pause
      end

      # Resumes updates and maintenance for an index
      #
      # @param [String] index_name name of the index
      # @param [ResumeIngestOptions] options
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def resume_ingest(index_name, options = ResumeIngestOptions.new)
        # POST /api/index/{index_name}/ingestControl/resume
      end

      # Allows querying against the index
      #
      # @param [String] index_name name of the index
      # @param [AllowQueryingOptions] options
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def allow_querying(index_name, options = AllowQueryingOptions.new)
        # POST /api/index/{index_name}/queryControl/allow
      end

      # Disallows querying against the index
      #
      # @param [String] index_name name of the index
      # @param [DisallowQueryingOptions] options
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def disallow_querying(index_name, options = DisallowQueryingOptions.new)
        # POST /api/index/{index_name}/queryControl/disallow
      end

      # Freeze the assignment of index partitions to nodes
      #
      # @param [String] index_name name of the index
      # @param [FreezePlanOptions] options
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def freeze_plan(index_name, options = FreezePlanOptions.new)
        # POST /api/index/{index_name}/planFreezeControl/freeze
      end

      # Unfreeze the assignment of index partitions to nodes
      #
      # @param [String] index_name name of the index
      # @param [UnfreezePlanOptions] options
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def unfreeze_plan(index_name, options = UnfreezePlanOptions.new)
        # POST /api/index/{index_name}/planFreezeControl/unfreeze
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
        # POST /api/index/{index_name}/analyzeDoc
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

      class GetIndexedDocumentsOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [GetIndexedDocumentOptions] self
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
        yield self if block_given?
      end
    end
  end
end
