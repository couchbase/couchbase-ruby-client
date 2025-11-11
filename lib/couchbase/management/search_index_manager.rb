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

require "couchbase/errors"
require "couchbase/options"

module Couchbase
  module Management
    module Options
      module Search
        # Options for {SearchIndexManager#get_index}
        class GetIndex < ::Couchbase::Options::Base
          # Creates an instance of options for {SearchIndexManager#get_index}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [GetIndex] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end

          # @api private
          DEFAULT = new.freeze
        end

        # Options for {SearchIndexManager#get_all_indexes}
        class GetAllIndexes < ::Couchbase::Options::Base
          # Creates an instance of options for {SearchIndexManager#get_all_indexes}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [GetAllIndexes] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end

          # @api private
          DEFAULT = new.freeze
        end

        # Options for {SearchIndexManager#upsert_index}
        class UpsertIndex < ::Couchbase::Options::Base
          # Creates an instance of options for {SearchIndexManager#upsert_index}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [UpsertIndex] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end

          # @api private
          DEFAULT = new.freeze
        end

        # Options for {SearchIndexManager#drop_index}
        class DropIndex < ::Couchbase::Options::Base
          # Creates an instance of options for {SearchIndexManager#drop_index}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [DropIndex] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end

          # @api private
          DEFAULT = new.freeze
        end

        # Options for {SearchIndexManager#get_indexed_documents_count}
        class GetIndexedDocumentsCount < ::Couchbase::Options::Base
          # Creates an instance of options for {SearchIndexManager#get_indexed_documents_count}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [GetIndexedDocumentsCount] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end

          # @api private
          DEFAULT = new.freeze
        end

        # Options for {SearchIndexManager#get_index_stats}
        class GetIndexStats < ::Couchbase::Options::Base
          # Creates an instance of options for {SearchIndexManager#get_index_stats}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [GetIndexStats] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end

          # @api private
          DEFAULT = new.freeze
        end

        # Options for {SearchIndexManager#pause_ingest}
        class PauseIngest < ::Couchbase::Options::Base
          # Creates an instance of options for {SearchIndexManager#pause_ingest}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [PauseIngest] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end

          # @api private
          DEFAULT = new.freeze
        end

        # Options for {SearchIndexManager#resume_ingest}
        class ResumeIngest < ::Couchbase::Options::Base
          # Creates an instance of options for {SearchIndexManager#resume_ingest}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [ResumeIngest] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end

          # @api private
          DEFAULT = new.freeze
        end

        # Options for {SearchIndexManager#allow_querying}
        class AllowQuerying < ::Couchbase::Options::Base
          # Creates an instance of options for {SearchIndexManager#allow_querying}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [AllowQuerying] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end

          # @api private
          DEFAULT = new.freeze
        end

        # Options for {SearchIndexManager#disallow_querying}
        class DisallowQuerying < ::Couchbase::Options::Base
          # Creates an instance of options for {SearchIndexManager#disallow_querying}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [DisallowQuerying] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end

          # @api private
          DEFAULT = new.freeze
        end

        # Options for {SearchIndexManager#freeze_plan}
        class FreezePlan < ::Couchbase::Options::Base
          # Creates an instance of options for {SearchIndexManager#freeze_plan}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [FreezePlan] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end

          # @api private
          DEFAULT = new.freeze
        end

        # Options for {SearchIndexManager#unfreeze_plan}
        class UnfreezePlan < ::Couchbase::Options::Base
          # Creates an instance of options for {SearchIndexManager#unfreeze_plan}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [UnfreezePlan] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end

          # @api private
          DEFAULT = new.freeze
        end

        # Options for {SearchIndexManager#analyze_document}
        class AnalyzeDocument < ::Couchbase::Options::Base
          # Creates an instance of options for {SearchIndexManager#analyze_document}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [AnalyzeDocument] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end

          # @api private
          DEFAULT = new.freeze
        end
      end
    end

    class SearchIndexManager
      alias inspect to_s

      # @param [Couchbase::Backend] backend
      def initialize(backend)
        @backend = backend
      end

      # Fetches an index from the server if it exists
      #
      # @param [String] index_name name of the index
      # @param [Options::Search::GetIndex] options
      #
      # @return [SearchIndex]
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def get_index(index_name, options = Options::Search::GetIndex::DEFAULT)
        res = @backend.search_index_get(nil, nil, index_name, options.timeout)
        self.class.extract_search_index(res)
      end

      # Fetches all indexes from the server
      #
      # @param [Options::Search::GetAllIndexes] options
      #
      # @return [Array<SearchIndex>]
      def get_all_indexes(options = Options::Search::GetAllIndexes::DEFAULT)
        res = @backend.search_index_get_all(nil, nil, options.timeout)
        res[:indexes].map { |idx| self.class.extract_search_index(idx) }
      end

      # Creates or updates the index
      #
      # @param [SearchIndex] index_definition the index definition
      # @param [Options::Search::UpsertIndex] options
      #
      # @return void
      #
      # @raise [ArgumentError] if name, type or source_type is empty
      def upsert_index(index_definition, options = Options::Search::UpsertIndex::DEFAULT)
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
      # @param [Options::Search::DropIndex] options
      #
      # @return void
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def drop_index(index_name, options = Options::Search::DropIndex::DEFAULT)
        @backend.search_index_drop(nil, nil, index_name, options.timeout)
      end

      # Retrieves the number of documents that have been indexed for an index
      #
      # @param [String] index_name name of the index
      # @param [Options::Search::GetIndexedDocumentsCount] options
      #
      # @return [Integer]
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def get_indexed_documents_count(index_name, options = Options::Search::GetIndexedDocumentsCount::DEFAULT)
        res = @backend.search_index_get_documents_count(nil, nil, index_name, options.timeout)
        res[:count]
      end

      # Retrieves metrics, timings and counters for a given index
      #
      # @!macro uncommitted
      #
      # @param [String] index_name name of the index
      # @param [Options::Search::GetIndexStats] options
      #
      # @return [Integer]
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def get_index_stats(index_name, options = Options::Search::GetIndexStats::DEFAULT)
        res = @backend.search_index_get_stats(index_name, options.timeout)
        JSON.parse(res)
      end

      # Retrieves statistics on search service. Information is provided on documents, partition indexes, mutations,
      # compactions, queries, and more.
      #
      # @!macro uncommitted
      #
      # @param [Options::Search::GetIndexStats] options
      #
      # @return [Integer]
      #
      # @raise [ArgumentError]
      def get_stats(options = Options::Search::GetIndexStats::DEFAULT)
        res = @backend.search_get_stats(options.timeout)
        JSON.parse(res)
      end

      # Pauses updates and maintenance for the index
      #
      # @param [String] index_name name of the index
      # @param [Options::Search::PauseIngest] options
      #
      # @return void
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def pause_ingest(index_name, options = Options::Search::PauseIngest::DEFAULT)
        @backend.search_index_pause_ingest(nil, nil, index_name, options.timeout)
      end

      # Resumes updates and maintenance for an index
      #
      # @param [String] index_name name of the index
      # @param [Options::Search::ResumeIngest] options
      #
      # @return void
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def resume_ingest(index_name, options = Options::Search::ResumeIngest::DEFAULT)
        @backend.search_index_resume_ingest(nil, nil, index_name, options.timeout)
      end

      # Allows querying against the index
      #
      # @param [String] index_name name of the index
      # @param [Options::Search::AllowQuerying] options
      #
      # @return void
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def allow_querying(index_name, options = Options::Search::AllowQuerying::DEFAULT)
        @backend.search_index_allow_querying(nil, nil, index_name, options.timeout)
      end

      # Disallows querying against the index
      #
      # @param [String] index_name name of the index
      # @param [Options::Search::DisallowQuerying] options
      #
      # @return void
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def disallow_querying(index_name, options = Options::Search::DisallowQuerying::DEFAULT)
        @backend.search_index_disallow_querying(nil, nil, index_name, options.timeout)
      end

      # Freeze the assignment of index partitions to nodes
      #
      # @param [String] index_name name of the index
      # @param [Options::Search::FreezePlan] options
      #
      # @return void
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def freeze_plan(index_name, options = Options::Search::FreezePlan::DEFAULT)
        @backend.search_index_freeze_plan(nil, nil, index_name, options.timeout)
      end

      # Unfreeze the assignment of index partitions to nodes
      #
      # @param [String] index_name name of the index
      # @param [Options::Search::UnfreezePlan] options
      #
      # @return void
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def unfreeze_plan(index_name, options = Options::Search::UnfreezePlan::DEFAULT)
        @backend.search_index_unfreeze_plan(nil, nil, index_name, options.timeout)
      end

      # Allows to see how a document is analyzed against a specific index
      #
      # @param [String] index_name name of the index
      # @param [Hash] document the document to be analyzed
      # @param [Options::Search::AnalyzeDocument] options
      #
      # @return [Array<Hash>]
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def analyze_document(index_name, document, options = Options::Search::AnalyzeDocument::DEFAULT)
        res = @backend.search_index_analyze_document(nil, nil, index_name, JSON.generate(document), options.timeout)
        JSON.parse(res[:analysis])
      end

      # @api private
      # @deprecated use {Options::Search::GetIndex} instead
      GetIndexOptions = Options::Search::GetIndex

      # @api private
      # @deprecated use {Options::Search::GetAllIndexes} instead
      GetAllIndexesOptions = Options::Search::GetAllIndexes

      # @api private
      # @deprecated use {Options::Search::UpsertIndex} instead
      UpsertIndexOptions = Options::Search::UpsertIndex

      # @api private
      # @deprecated use {Options::Search::DropIndex} instead
      DropIndexOptions = Options::Search::DropIndex

      # @api private
      # @deprecated use {Options::Search::GetIndexedDocumentsCount} instead
      GetIndexedDocumentsCountOptions = Options::Search::GetIndexedDocumentsCount

      # @api private
      # @deprecated use {Options::Search::GetIndexStats} instead
      GetIndexStatsOptions = Options::Search::GetIndexStats

      # @api private
      # @deprecated use {Options::Search::PauseIngest} instead
      PauseIngestOptions = Options::Search::PauseIngest

      # @api private
      # @deprecated use {Options::Search::ResumeIngest} instead
      ResumeIngestOptions = Options::Search::ResumeIngest

      # @api private
      # @deprecated use {Options::Search::AllowQuerying} instead
      AllowQueryingOptions = Options::Search::AllowQuerying

      # @api private
      # @deprecated use {Options::Search::DisallowQuerying} instead
      DisallowQueryingOptions = Options::Search::DisallowQuerying

      # @api private
      # @deprecated use {Options::Search::FreezePlan} instead
      FreezePlanOptions = Options::Search::FreezePlan

      # @api private
      # @deprecated use {Options::Search::UnfreezePlan} instead
      UnfreezePlanOptions = Options::Search::UnfreezePlan

      # @api private
      # @deprecated use {Options::Search::AnalyzeDocument} instead
      AnalyzeDocumentOptions = Options::Search::AnalyzeDocument

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
