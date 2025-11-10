# frozen_string_literal: true

#  Copyright 2024. Couchbase, Inc.
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

require_relative "search_index_manager"

module Couchbase
  module Management
    class ScopeSearchIndexManager
      alias inspect to_s

      # @param [Couchbase::Backend] backend
      # @param [String] bucket_name
      # @param [String] scope_name
      def initialize(backend, bucket_name, scope_name)
        @backend = backend
        @bucket_name = bucket_name
        @scope_name = scope_name
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
        res = @backend.search_index_get(@bucket_name, @scope_name, index_name, options.timeout)
        SearchIndexManager.extract_search_index(res)
      end

      # Fetches all indexes from the server
      #
      # @param [Options::Search::GetAllIndexes] options
      #
      # @return [Array<SearchIndex>]
      def get_all_indexes(options = Options::Search::GetAllIndexes::DEFAULT)
        res = @backend.search_index_get_all(@bucket_name, @scope_name, options.timeout)
        res[:indexes].map { |idx| SearchIndexManager.extract_search_index(idx) }
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
        @backend.search_index_upsert(@bucket_name, @scope_name, index_definition.to_backend, options.timeout)
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
        @backend.search_index_drop(@bucket_name, @scope_name, index_name, options.timeout)
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
        res = @backend.search_index_get_documents_count(@bucket_name, @scope_name, index_name, options.timeout)
        res[:count]
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
        @backend.search_index_pause_ingest(@bucket_name, @scope_name, index_name, options.timeout)
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
        @backend.search_index_resume_ingest(@bucket_name, @scope_name, index_name, options.timeout)
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
        @backend.search_index_allow_querying(@bucket_name, @scope_name, index_name, options.timeout)
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
        @backend.search_index_disallow_querying(@bucket_name, @scope_name, index_name, options.timeout)
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
        @backend.search_index_freeze_plan(@bucket_name, @scope_name, index_name, options.timeout)
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
        @backend.search_index_unfreeze_plan(@bucket_name, @scope_name, index_name, options.timeout)
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
        res = @backend.search_index_analyze_document(@bucket_name, @scope_name, index_name, JSON.generate(document), options.timeout)
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
    end
  end
end
