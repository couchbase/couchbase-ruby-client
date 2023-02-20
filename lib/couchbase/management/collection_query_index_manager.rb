#  Copyright 2023 Couchbase, Inc.
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

require "couchbase/management/query_index_manager"
require "couchbase/utils/time"

module Couchbase
  module Management
    class CollectionQueryIndexManager
      alias inspect to_s

      # @param [Couchbase::Backend] backend
      # @param [String] bucket_name name of the bucket
      # @param [String] scope_name name of the scope
      # @param [String] collection_name name of the collection
      def initialize(backend, bucket_name, scope_name, collection_name)
        @backend = backend
        @bucket_name = bucket_name
        @scope_name = scope_name
        @collection_name = collection_name
      end

      # Fetches all indexes from the server
      #
      # @param [Options::Query::GetAllIndexes] options
      #
      # @return [Array<QueryIndex>]
      #
      # @raise [ArgumentError]
      def get_all_indexes(options = Options::Query::GetAllIndexes.new)
        res = @backend.collection_query_index_get_all(@bucket_name, @scope_name, @collection_name, options.to_backend)
        res[:indexes].map do |idx|
          QueryIndex.new do |index|
            index.name = idx[:name]
            index.is_primary = idx[:is_primary]
            index.type = idx[:type]
            index.state = idx[:state]
            index.bucket = idx[:bucket_name]
            index.scope = idx[:scope_name]
            index.collection = idx[:collection_name]
            index.index_key = idx[:index_key]
            index.condition = idx[:condition]
            index.partition = idx[:partition]
          end
        end
      end

      # Creates a new index
      #
      # @param [String] index_name name of the index
      # @param [Array<String>] fields the lists of fields to create th index over
      # @param [Options::Query::CreateIndex] options
      #
      # @return void
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexExists]
      def create_index(index_name, fields, options = Options::Query::CreateIndex.new)
        unless options.scope_name.nil?
          raise ArgumentError, "Scope name cannot be set in the options when using the Query Index manager at the collection level"
        end

        unless options.collection_name.nil?
          raise ArgumentError, "Collection name cannot be set in the options when using the Query Index manager at the collection level"
        end

        @backend.collection_query_index_create(@bucket_name, @scope_name, @collection_name, index_name, fields, options.to_backend)
      end

      # Creates new primary index
      #
      # @param [Options::Query::CreatePrimaryIndex] options
      #
      # @return void
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexExists]
      def create_primary_index(options = Options::Query::CreatePrimaryIndex.new)
        unless options.scope_name.nil?
          raise ArgumentError, "Scope name cannot be set in the options when using the Query Index manager at the collection level"
        end

        unless options.collection_name.nil?
          raise ArgumentError, "Collection name cannot be set in the options when using the Query Index manager at the collection level"
        end

        @backend.collection_query_index_create_primary(@bucket_name, @scope_name, @collection_name, options.to_backend)
      end

      # Drops the index
      #
      # @param [String] index_name name of the index
      # @param [Options::Query::DropIndex] options
      #
      # @return void
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def drop_index(index_name, options = Options::Query::DropIndex.new)
        unless options.scope_name.nil?
          raise ArgumentError, "Scope name cannot be set in the options when using the Query Index manager at the collection level"
        end

        unless options.collection_name.nil?
          raise ArgumentError, "Collection name cannot be set in the options when using the Query Index manager at the collection level"
        end

        @backend.collection_query_index_drop(@bucket_name, @scope_name, @collection_name, index_name, options.to_backend)
      end

      # Drops the primary index
      #
      # @param [Options::Query::DropPrimaryIndex] options
      #
      # @return void
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def drop_primary_index(options = Options::Query::DropPrimaryIndex.new)
        unless options.scope_name.nil?
          raise ArgumentError, "Scope name cannot be set in the options when using the Query Index manager at the collection level"
        end

        unless options.collection_name.nil?
          raise ArgumentError, "Collection name cannot be set in the options when using the Query Index manager at the collection level"
        end

        @backend.collection_query_index_drop_primary(@bucket_name, @scope_name, @collection_name, options.to_backend)
      end

      # Build all indexes which are currently in deferred state
      #
      # @param [Options::Query::BuildDeferredIndexes] options
      #
      # @return void
      #
      # @raise [ArgumentError]
      def build_deferred_indexes(options = Options::Query::BuildDeferredIndexes.new)
        @backend.collection_query_index_build_deferred(@bucket_name, @scope_name, @collection_name, options.to_backend)
      end

      # Polls indexes until they are online
      #
      # @param [Array<String>] index_names names of the indexes to watch
      # @param [Integer, #in_milliseconds] timeout the time in milliseconds allowed for the operation to complete
      # @param [Options::Query::WatchIndexes] options
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def watch_indexes(index_names, timeout, options = Options::Query::WatchIndexes.new)
        index_names.append("#primary") if options.watch_primary

        interval_millis = 50
        deadline = Time.now + (Utils::Time.extract_duration(timeout) * 0.001)
        while Time.now <= deadline
          get_all_opts = Options::Query::GetAllIndexes.new(timeout: ((deadline - Time.now) * 1000).round)
          indexes = get_all_indexes(get_all_opts).select { |idx| index_names.include? idx.name }
          indexes_not_found = index_names - indexes.map(&:name)
          raise Error::IndexNotFound, "Failed to find the indexes: #{indexes_not_found.join(', ')}" unless indexes_not_found.empty?

          all_online = indexes.all? { |idx| idx.state == :online }
          return if all_online

          sleep(interval_millis / 1000)
          interval_millis += 500
          interval_millis = 1000 if interval_millis > 1000
        end
        raise Error::UnambiguousTimeout, "Failed to find all indexes online within the allotted time"
      end
    end
  end
end
