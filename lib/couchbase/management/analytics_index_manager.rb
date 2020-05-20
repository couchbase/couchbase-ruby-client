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
      # @raise DataverseExists
      # @raise ArgumentError
      def create_dataverse(dataverse_name, options = CreateDataverseOptions.new) end

      # Drops a dataverse
      #
      # @param [String] dataverse_name name of the dataverse
      # @param [DropDataverseOptions] options
      #
      # @raise DataverseNotFound
      # @raise ArgumentError
      def drop_dataverse(dataverse_name, options = DropDataverseOptions.new) end

      # Creates a new dataset
      #
      # @param [String] dataset_name name of dataset
      # @param [String] bucket_name name of the bucket
      #
      # @raise DatasetAlreadyExistsError
      # @raise ArgumentError
      def create_dataset(dataset_name, bucket_name, options = CreateDatasetOptions.new) end

      def drop_dataset(dataset_name, options = DropDatasetOptions.new) end

      def get_all_datasets(options = GetAllDatasetOptions.new) end

      def create_index(index_name, dataset_name, fields, options = CreateIndexOptions.new) end

      def drop_index(index_name, dataset_name, options = DropIndexOptions.new) end

      def get_all_indexes(options = GetAllIndexesOptions.new) end

      def connect_link(options = ConnectLinkOptions.new) end

      def disconnect_link(options = DisconnectLinkOptions.new) end

      def get_pending_mutations(options = GetPendingMutations.new) end

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