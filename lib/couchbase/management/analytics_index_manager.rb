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
      # @raise [ArgumentError]
      # @raise [Error::DataverseExists]
      def create_dataverse(dataverse_name, options = CreateDataverseOptions.new) end

      # Drops a dataverse
      #
      # @param [String] dataverse_name name of the dataverse
      # @param [DropDataverseOptions] options
      #
      # @raise [ArgumentError]
      # @raise [Error::DataverseNotFound]
      def drop_dataverse(dataverse_name, options = DropDataverseOptions.new) end

      # Creates a new dataset
      #
      # @param [String] dataset_name name of dataset
      # @param [String] bucket_name name of the bucket
      # @param [CreateDatasetOptions] options
      #
      # @raise [ArgumentError]
      # @raise [Error::DatasetExists]
      def create_dataset(dataset_name, bucket_name, options = CreateDatasetOptions.new) end

      # Drops a dataset
      #
      # @param [String] dataset_name name of the dataset
      # @param [DropDataverseOptions] options
      # 
      # @raise [ArgumentError]
      # @raise [Error::DatasetNotFound]
      def drop_dataset(dataset_name, options = DropDatasetOptions.new) end

      # Gets all datasets
      #
      # @param [GetAllDatasetsOptions] options
      #
      # @return [Array<AnalyticsDataset>]
      def get_all_datasets(options = GetAllDatasetsOptions.new)
        # SELECT d.* FROM Metadata.`Dataset` d WHERE d.DataverseName <> "Metadata"
      end

      # Creates a new index
      #
      # @param [String] index_name name of the index
      # @param [String] dataset_name name of the dataset
      # @param [Hash<String, String>] fields mapping of the field name to field type
      # @param [CreateIndexOptions] options
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexExists]
      def create_index(index_name, dataset_name, fields, options = CreateIndexOptions.new) end

      # Drops an index
      #
      # @param [String] index_name name of the index
      # @param [String] dataset_name name of the dataset
      # @param [DropIndexOptions] options
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def drop_index(index_name, dataset_name, options = DropIndexOptions.new) end

      # Gets all indexes
      #
      # @param [GetAllIndexesOptions] options
      #
      # @return [Array<AnalyticsIndex>]
      def get_all_indexes(options = GetAllIndexesOptions.new)
        # SELECT d.* FROM Metadata.`Index` d WHERE d.DataverseName <> "Metadata"
      end

      # Connects a link
      #
      # @param [ConnectLinkOptions] options
      #
      # @raise [ArgumentError]
      # @raise [Error::LinkNotFound]
      def connect_link(options = ConnectLinkOptions.new) end

      # Disconnects a link,
      #
      # @param [DisconnectLinkOptions] options
      #
      # @raise [ArgumentError]
      # @raise [Error::LinkNotFound]
      def disconnect_link(options = DisconnectLinkOptions.new) end

      # Gets the pending mutations for all datasets.
      # 
      # @note If a link is disconnected then it will return no results. If all links are disconnected, then
      # an empty object is returned.
      #
      # @param [GetPendingMutationsOptions] options
      #
      # @return [Hash<String, Integer>] dictionary, where keys are dataset coordinates encoded as +"dataverse.dataset"+
      #   and values are number of mutations for given dataset.
      def get_pending_mutations(options = GetPendingMutationsOptions.new)
        # GET http://localhost:8095/analytics/node/agg/stats/remaining
      end

      class CreateDataverseOptions
        # @return [Boolean] ignore if the dataverse already exists
        attr_accessor :ignore_if_exists

        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        def initialize
          @ignore_if_exists = false
          yield self if block_given?
        end
      end

      class DropDataverseOptions
        # @return [Boolean] ignore if the dataverse does not exists
        attr_accessor :ignore_if_not_exists

        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

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

        def initialize
          @ignore_if_exists = false
          yield self if block_given?
        end
      end

      class DropDatasetOptions
        # @return [Boolean] ignore if the dataset does not exists
        attr_accessor :ignore_if_not_exists

        # @return [String] The name of the dataverse to use (defaults to +nil+)
        attr_accessor :dataverse_name

        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        def initialize
          @ignore_if_not_exists = false
          yield self if block_given?
        end
      end

      class GetAllDatasetsOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        def initialize
          @ignore_if_not_exists = false
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

        def initialize
          @ignore_if_exists = false
          yield self if block_given?
        end
      end

      class DropIndexOptions
        # @return [Boolean] ignore if the index does not exists
        attr_accessor :ignore_if_not_exists

        # @return [String] The name of the dataverse to use (defaults to +nil+)
        attr_accessor :dataverse_name

        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        def initialize
          @ignore_if_not_exists = false
          yield self if block_given?
        end
      end

      class GetAllIndexesOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        def initialize
          yield self if block_given?
        end
      end

      class ConnectLinkOptions
        # @return [String] The name of the link (defaults to +"Local"+)
        attr_accessor :link_name

        # @return [Boolean] Whether to force link creation even if the bucket UUID changed, for example due to the
        # bucket being deleted and recreated (defaults to +false+)
        attr_accessor :force

        # @return [String] The name of the dataverse to use (defaults to +nil+)
        attr_accessor :dataverse_name

        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

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

        def initialize
          @link_name = "Local"
          yield self if block_given?
        end
      end

      class GetPendingMutationsOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

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
    end
  end
end