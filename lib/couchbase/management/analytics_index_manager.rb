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
    class AnalyticsIndexManager
      alias_method :inspect, :to_s

      # @param [Couchbase::Backend] backend
      def initialize(backend)
        @backend = backend
      end

      # Creates a new dataverse
      #
      # @param [String] dataverse_name
      # @param [CreateDataverseOptions] options
      #
      # @return [void]
      #
      # @raise [ArgumentError]
      # @raise [Error::DataverseExists]
      def create_dataverse(dataverse_name, options = CreateDataverseOptions.new)
        @backend.analytics_dataverse_create(
            dataverse_name,
            options.ignore_if_exists,
            options.timeout
        )
      end

      # Drops a dataverse
      #
      # @param [String] dataverse_name name of the dataverse
      # @param [DropDataverseOptions] options
      #
      # @return [void]
      #
      # @raise [ArgumentError]
      # @raise [Error::DataverseNotFound]
      def drop_dataverse(dataverse_name, options = DropDataverseOptions.new)
        @backend.analytics_dataverse_drop(
            dataverse_name,
            options.ignore_if_does_not_exist,
            options.timeout
        )
      end

      # Creates a new dataset
      #
      # @param [String] dataset_name name of dataset
      # @param [String] bucket_name name of the bucket
      # @param [CreateDatasetOptions] options
      #
      # @return [void]
      #
      # @raise [ArgumentError]
      # @raise [Error::DatasetExists]
      # @raise [Error::LinkNotFound]
      def create_dataset(dataset_name, bucket_name, options = CreateDatasetOptions.new)
        @backend.analytics_dataset_create(
            dataset_name,
            bucket_name,
            options.condition,
            options.dataverse_name,
            options.ignore_if_exists,
            options.timeout
        )
      end

      # Drops a dataset
      #
      # @param [String] dataset_name name of the dataset
      # @param [DropDatasetOptions] options
      #
      # @return [void]
      #
      # @raise [ArgumentError]
      # @raise [Error::DatasetNotFound]
      def drop_dataset(dataset_name, options = DropDatasetOptions.new)
        @backend.analytics_dataset_drop(
            dataset_name,
            options.dataverse_name,
            options.ignore_if_does_not_exist,
            options.timeout
        )
      end

      # Gets all datasets
      #
      # @param [GetAllDatasetsOptions] options
      #
      # @return [Array<AnalyticsDataset>]
      def get_all_datasets(options = GetAllDatasetsOptions.new)
        resp = @backend.analytics_dataset_get_all(options.timeout)
        resp.map do |entry|
          AnalyticsDataset.new do |dataset|
            dataset.name = entry[:name]
            dataset.dataverse_name = entry[:dataverse_name]
            dataset.link_name = entry[:link_name]
            dataset.bucket_name = entry[:bucket_name]
          end
        end
      end

      # Creates a new index
      #
      # @param [String] index_name name of the index
      # @param [String] dataset_name name of the dataset
      # @param [Hash<String => String>] fields mapping of the field name to field type
      # @param [CreateIndexOptions] options
      #
      # @return [void]
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexExists]
      def create_index(index_name, dataset_name, fields, options = CreateIndexOptions.new)
        @backend.analytics_index_create(
            index_name,
            dataset_name,
            fields.entries,
            options.dataverse_name,
            options.ignore_if_exists,
            options.timeout
        )
      end

      # Drops an index
      #
      # @param [String] index_name name of the index
      # @param [String] dataset_name name of the dataset
      # @param [DropIndexOptions] options
      #
      # @return [void]
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def drop_index(index_name, dataset_name, options = DropIndexOptions.new)
        @backend.analytics_index_drop(
            index_name,
            dataset_name,
            options.dataverse_name,
            options.ignore_if_does_not_exist,
            options.timeout
        )
      end

      # Gets all indexes
      #
      # @param [GetAllIndexesOptions] options
      #
      # @return [Array<AnalyticsIndex>]
      def get_all_indexes(options = GetAllIndexesOptions.new)
        resp = @backend.analytics_index_get_all(options.timeout)
        resp.map do |entry|
          AnalyticsIndex.new do |dataset|
            dataset.name = entry[:name]
            dataset.dataverse_name = entry[:dataverse_name]
            dataset.dataset_name = entry[:dataset_name]
            dataset.is_primary = entry[:is_primary]
          end
        end
      end

      # Connects a link
      #
      # @param [ConnectLinkOptions] options
      #
      # @return [void]
      #
      # @raise [ArgumentError]
      # @raise [Error::LinkNotFound]
      def connect_link(options = ConnectLinkOptions.new)
        @backend.analytics_link_connect(
            options.link_name,
            options.force,
            options.dataverse_name,
            options.timeout
        )
      end

      # Disconnects a link,
      #
      # @param [DisconnectLinkOptions] options
      #
      # @return [void]
      #
      # @raise [ArgumentError]
      # @raise [Error::LinkNotFound]
      def disconnect_link(options = DisconnectLinkOptions.new)
        @backend.analytics_link_disconnect(
            options.link_name,
            options.dataverse_name,
            options.timeout
        )
      end

      # Gets the pending mutations for all datasets.
      #
      # @note If a link is disconnected then it will return no results. If all links are disconnected, then
      # an empty object is returned.
      #
      # @param [GetPendingMutationsOptions] options
      #
      # @return [Hash<String => Integer>] dictionary, where keys are dataset coordinates encoded as +"dataverse.dataset"+
      #   and values are number of mutations for given dataset.
      def get_pending_mutations(options = GetPendingMutationsOptions.new)
        @backend.analytics_get_pending_mutations(
            options.timeout
        )
      end

      class CreateDataverseOptions
        # @return [Boolean] ignore if the dataverse already exists
        attr_accessor :ignore_if_exists

        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [CreateDataverseOptions]
        def initialize
          @ignore_if_exists = false
          yield self if block_given?
        end
      end

      class DropDataverseOptions
        # @return [Boolean] ignore if the dataverse does not exists
        attr_accessor :ignore_if_does_not_exist

        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [DropDataverseOptions]
        def initialize
          @ignore_if_exists = false
          yield self if block_given?
        end
      end

      class CreateDatasetOptions
        # @return [Boolean] ignore if the dataset already exists
        attr_accessor :ignore_if_exists

        # @return [String] WHERE clause to use for creating dataset
        attr_accessor :condition

        # @return [String] The name of the dataverse to use (defaults to +nil+)
        attr_accessor :dataverse_name

        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [CreateDatasetOptions]
        def initialize
          @ignore_if_exists = false
          yield self if block_given?
        end
      end

      class DropDatasetOptions
        # @return [Boolean] ignore if the dataset does not exists
        attr_accessor :ignore_if_does_not_exist

        # @return [String] The name of the dataverse to use (defaults to +nil+)
        attr_accessor :dataverse_name

        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [DropDatasetOptions]
        def initialize
          @ignore_if_does_not_exist = false
          yield self if block_given?
        end
      end

      class GetAllDatasetsOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [GetAllDatasetsOptions]
        def initialize
          yield self if block_given?
        end
      end

      class CreateIndexOptions
        # @return [Boolean] ignore if the index already exists
        attr_accessor :ignore_if_exists

        # @return [String] The name of the dataverse to use (defaults to +nil+)
        attr_accessor :dataverse_name

        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [CreateIndexOptions]
        def initialize
          @ignore_if_exists = false
          yield self if block_given?
        end
      end

      class DropIndexOptions
        # @return [Boolean] ignore if the index does not exists
        attr_accessor :ignore_if_does_not_exist

        # @return [String] The name of the dataverse to use (defaults to +nil+)
        attr_accessor :dataverse_name

        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [DropIndexOptions]
        def initialize
          @ignore_if_does_not_exist = false
          yield self if block_given?
        end
      end

      class GetAllIndexesOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [GetAllIndexesOptions]
        def initialize
          yield self if block_given?
        end
      end

      class ConnectLinkOptions
        # @return [String] The name of the link (defaults to +"Local"+)
        attr_accessor :link_name

        # @return [Boolean] Whether to force link creation even if the bucket UUID changed, for example due to the
        #  bucket being deleted and recreated (defaults to +false+)
        attr_accessor :force

        # @return [String] The name of the dataverse to use (defaults to +nil+)
        attr_accessor :dataverse_name

        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [ConnectLinkOptions]
        def initialize
          @link_name = "Local"
          @force = false
          yield self if block_given?
        end
      end

      class DisconnectLinkOptions
        # @return [String] The name of the link (defaults to +"Local"+)
        attr_accessor :link_name

        # @return [String] The name of the dataverse to use (defaults to +nil+)
        attr_accessor :dataverse_name

        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [DisconnectLinkOptions]
        def initialize
          @link_name = "Local"
          yield self if block_given?
        end
      end

      class GetPendingMutationsOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [GetPendingMutationsOptions]
        def initialize
          yield self if block_given?
        end
      end
    end

    class AnalyticsDataset
      # @return [String]
      attr_accessor :name

      # @return [String]
      attr_accessor :dataverse_name

      # @return [String]
      attr_accessor :link_name

      # @return [String]
      attr_accessor :bucket_name

      # @yieldparam [AnalyticsDataset]
      def initialize
        yield self if block_given?
      end
    end

    class AnalyticsIndex
      # @return [String]
      attr_accessor :name

      # @return [String]
      attr_accessor :dataset_name

      # @return [String]
      attr_accessor :dataverse_name

      # @return [Boolean]
      attr_accessor :is_primary
      alias_method :primary?, :is_primary

      # @yieldparam [AnalyticsIndex]
      def initialize
        yield self if block_given?
      end
    end
  end
end
