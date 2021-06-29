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
      module Analytics
        # Options for {AnalyticsIndexManager#create_dataverse}
        class CreateDataverse < ::Couchbase::Options::Base
          attr_accessor :ignore_if_exists # @return [Boolean]

          # Creates an instance of options for {AnalyticsIndexManager#create_dataverse}
          #
          # @param [Boolean] ignore_if_exists if +true+, the exception {Error::DataverseExists} will not be raised if the
          #  dataverse with the specified name already exists.
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Hash, nil] client_context the client context data, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [CreateDataverse] self
          def initialize(ignore_if_exists: false,
                         timeout: nil,
                         retry_strategy: nil,
                         client_context: nil,
                         parent_span: nil)
            super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
            @ignore_if_exists = ignore_if_exists
            yield self if block_given?
          end

          # @api private
          def to_backend
            {
              timeout: @timeout.respond_to?(:in_milliseconds) ? @timeout.public_send(:in_milliseconds) : @timeout,
              ignore_if_exists: @ignore_if_exists,
            }
          end
        end

        # Options for {AnalyticsIndexManager#drop_dataverse}
        class DropDataverse < ::Couchbase::Options::Base
          attr_accessor :ignore_if_does_not_exist # @return [Boolean]

          # Creates an instance of options for {AnalyticsIndexManager#create_dataverse}
          #
          # @param [Boolean] ignore_if_does_not_exist if +true+, the exception {Error::DataverseNotFound} will not be raised
          #  if the dataverse with the specified name does not exist.
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Hash, nil] client_context the client context data, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [DropDataverse] self
          def initialize(ignore_if_does_not_exist: false,
                         timeout: nil,
                         retry_strategy: nil,
                         client_context: nil,
                         parent_span: nil)
            super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
            @ignore_if_does_not_exist = ignore_if_does_not_exist
            yield self if block_given?
          end

          # @api private
          def to_backend
            {
              timeout: @timeout.respond_to?(:in_milliseconds) ? @timeout.public_send(:in_milliseconds) : @timeout,
              ignore_if_does_not_exist: @ignore_if_does_not_exist,
            }
          end
        end

        # Options for {AnalyticsIndexManager#create_dataset}
        class CreateDataset < ::Couchbase::Options::Base
          attr_accessor :ignore_if_exists # @return [Boolean]
          attr_accessor :condition # @return [String]
          attr_accessor :dataverse_name # @return [String]

          # Creates an instance of options for {AnalyticsIndexManager#create_dataset}
          #
          # @param [Boolean] ignore_if_exists if +true+, the exception {Error::DatasetExists} will not be raised
          #  if the dataset with the specified name already exists.
          # @param [String] condition WHERE clause to use for creating dataset
          # @param [String] dataverse_name the name of the dataverse to use (defaults to +nil+)
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Hash, nil] client_context the client context data, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [CreateDataset] self
          def initialize(ignore_if_exists: false,
                         condition: nil,
                         dataverse_name: nil,
                         timeout: nil,
                         retry_strategy: nil,
                         client_context: nil,
                         parent_span: nil)
            super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
            @ignore_if_exists = ignore_if_exists
            @condition = condition
            @dataverse_name = dataverse_name
            yield self if block_given?
          end

          # @api private
          def to_backend
            {
              timeout: @timeout.respond_to?(:in_milliseconds) ? @timeout.public_send(:in_milliseconds) : @timeout,
              condition: @condition,
              dataverse_name: @dataverse_name,
              ignore_if_exists: @ignore_if_exists,
            }
          end
        end

        # Options for {AnalyticsIndexManager#drop_dataset}
        class DropDataset < ::Couchbase::Options::Base
          attr_accessor :ignore_if_does_not_exist # @return [Boolean]
          attr_accessor :dataverse_name # @return [String]

          # Creates an instance of options for {AnalyticsIndexManager#drop_dataset}
          #
          # @param [Boolean] ignore_if_does_not_exist if +true+, the exception {Error::DatasetNotFound} will not be raised
          #  if the dataset with the specified name does not exist.
          # @param [String] dataverse_name the name of the dataverse to use (defaults to +nil+)
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Hash, nil] client_context the client context data, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [DropDataset] self
          def initialize(ignore_if_does_not_exist: false,
                         dataverse_name: nil,
                         timeout: nil,
                         retry_strategy: nil,
                         client_context: nil,
                         parent_span: nil)
            super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
            @ignore_if_does_not_exist = ignore_if_does_not_exist
            @dataverse_name = dataverse_name
            yield self if block_given?
          end

          # @api private
          def to_backend
            {
              timeout: @timeout.respond_to?(:in_milliseconds) ? @timeout.public_send(:in_milliseconds) : @timeout,
              dataverse_name: @dataverse_name,
              ignore_if_does_not_exist: @ignore_if_does_not_exist,
            }
          end
        end

        # Options for {AnalyticsIndexManager#get_all_datasets}
        class GetAllDatasets < ::Couchbase::Options::Base
          # Creates an instance of options for {AnalyticsIndexManager#get_all_datasets}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Hash, nil] client_context the client context data, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [GetAllDatasets] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         client_context: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end
        end

        # Options for {AnalyticsIndexManager#create_index}
        class CreateIndex < ::Couchbase::Options::Base
          attr_accessor :ignore_if_exists # @return [Boolean]
          attr_accessor :dataverse_name # @return [String]

          # Creates an instance of options for {AnalyticsIndexManager#create_index}
          #
          # @param [Boolean] ignore_if_exists if +true+, the exception {Error::DatasetExists} will not be raised
          #  if the dataset with the specified name already exists.
          # @param [String] dataverse_name the name of the dataverse to use (defaults to +nil+)
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Hash, nil] client_context the client context data, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [CreateIndex] self
          def initialize(ignore_if_exists: false,
                         dataverse_name: nil,
                         timeout: nil,
                         retry_strategy: nil,
                         client_context: nil,
                         parent_span: nil)
            super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
            @ignore_if_exists = ignore_if_exists
            @dataverse_name = dataverse_name
            yield self if block_given?
          end

          # @api private
          def to_backend
            {
              timeout: @timeout.respond_to?(:in_milliseconds) ? @timeout.public_send(:in_milliseconds) : @timeout,
              dataverse_name: @dataverse_name,
              ignore_if_exists: @ignore_if_exists,
            }
          end
        end

        # Options for {AnalyticsIndexManager#drop_index}
        class DropIndex < ::Couchbase::Options::Base
          attr_accessor :ignore_if_does_not_exist # @return [Boolean]
          attr_accessor :dataverse_name # @return [String]

          # Creates an instance of options for {AnalyticsIndexManager#drop_index}
          #
          # @param [Boolean] ignore_if_does_not_exist if +true+, the exception {Error::DatasetNotFound} will not be raised
          #  if the dataset with the specified name does not exist.
          # @param [String] dataverse_name the name of the dataverse to use (defaults to +nil+)
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Hash, nil] client_context the client context data, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [DropIndex] self
          def initialize(ignore_if_does_not_exist: false,
                         dataverse_name: nil,
                         timeout: nil,
                         retry_strategy: nil,
                         client_context: nil,
                         parent_span: nil)
            super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
            @ignore_if_does_not_exist = ignore_if_does_not_exist
            @dataverse_name = dataverse_name
            yield self if block_given?
          end

          # @api private
          def to_backend
            {
              timeout: @timeout.respond_to?(:in_milliseconds) ? @timeout.public_send(:in_milliseconds) : @timeout,
              dataverse_name: @dataverse_name,
              ignore_if_does_not_exist: @ignore_if_does_not_exist,
            }
          end
        end

        # Options for {AnalyticsIndexManager#get_all_indexes}
        class GetAllIndexes < ::Couchbase::Options::Base
          # Creates an instance of options for {AnalyticsIndexManager#get_all_indexes}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Hash, nil] client_context the client context data, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [GetAllIndexes] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         client_context: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end
        end

        # Options for {AnalyticsIndexManager#connect_link}
        class ConnectLink < ::Couchbase::Options::Base
          attr_accessor :link_name # @return [String]
          attr_accessor :force # @return [Boolean]
          attr_accessor :dataverse_name # @return [String]

          # Creates an instance of options for {AnalyticsIndexManager#connect_link}
          #
          # @param [String] link_name the name of the link
          # @param [Boolean] force if +true+, link creation will be forced even if the bucket UUID changed, for example
          #   due to the bucket being deleted and recreated
          # @param [String] dataverse_name the name of the dataverse to use (defaults to +nil+)
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Hash, nil] client_context the client context data, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [ConnectLinkOptions] self
          def initialize(link_name: "Local",
                         force: false,
                         dataverse_name: nil,
                         timeout: nil,
                         retry_strategy: nil,
                         client_context: nil,
                         parent_span: nil)
            super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
            @link_name = link_name
            @force = force
            @dataverse_name = dataverse_name
            yield self if block_given?
          end

          # @api private
          def to_backend
            {
              timeout: @timeout.respond_to?(:in_milliseconds) ? @timeout.public_send(:in_milliseconds) : @timeout,
              link_name: @link_name,
              force: @force,
              dataverse_name: @dataverse_name,
            }
          end
        end

        # Options for {AnalyticsIndexManager#connect_link}
        class DisconnectLink < ::Couchbase::Options::Base
          attr_accessor :link_name # @return [String]
          attr_accessor :dataverse_name # @return [String]

          # Creates an instance of options for {AnalyticsIndexManager#disconnect_link}
          #
          # @param [String] link_name the name of the link
          # @param [String] dataverse_name the name of the dataverse to use (defaults to +nil+)
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Hash, nil] client_context the client context data, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [DisconnectLinkOptions] self
          def initialize(link_name: "Local",
                         dataverse_name: nil,
                         timeout: nil,
                         retry_strategy: nil,
                         client_context: nil,
                         parent_span: nil)
            super(timeout: timeout, retry_strategy: retry_strategy, client_context: client_context, parent_span: parent_span)
            @link_name = link_name
            @dataverse_name = dataverse_name
            yield self if block_given?
          end

          # @api private
          def to_backend
            {
              timeout: @timeout.respond_to?(:in_milliseconds) ? @timeout.public_send(:in_milliseconds) : @timeout,
              link_name: @link_name,
              dataverse_name: @dataverse_name,
            }
          end
        end

        # Options for {AnalyticsIndexManager#get_pending_mutations}
        class GetPendingMutations < ::Couchbase::Options::Base
          # Creates an instance of options for {AnalyticsIndexManager#get_pending_mutations}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Hash, nil] client_context the client context data, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [GetPendingMutations] self
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         client_context: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end
        end

        # rubocop:disable Naming/MethodName constructor shortcuts
        module_function

        # Construct {CreateDataverse} options for {AnalyticsIndexManager#create_dataverse}
        #
        # @return [CreateDataverse]
        def CreateDataverse(**args)
          CreateDataverse.new(**args)
        end

        # Construct {DropDataverse} options for {AnalyticsIndexManager#drop_dataverse}
        #
        # @return [DropDataverse]
        def DropDataverse(**args)
          DropDataverse.new(**args)
        end

        # Construct {CreateDataset} options for {AnalyticsIndexManager#create_dataset}
        #
        # @return [CreateDataset]
        def CreateDataset(**args)
          CreateDataset.new(**args)
        end

        # Construct {DropDataset} options for {AnalyticsIndexManager#drop_dataset}
        #
        # @return [DropDataset]
        def DropDataset(**args)
          DropDataset.new(**args)
        end

        # Construct {GetAllDatasets} options for {AnalyticsIndexManager#get_all_datasets}
        #
        # @return [GetAllDatasets]
        def GetAllDatasets(**args)
          GetAllDatasets.new(**args)
        end

        # Construct {CreateIndex} options for {AnalyticsIndexManager#create_index}
        #
        # @return [CreateIndex]
        def CreateIndex(**args)
          CreateIndex.new(**args)
        end

        # Construct {DropIndex} options for {AnalyticsIndexManager#drop_index}
        #
        # @return [DropIndex]
        def DropIndex(**args)
          DropIndex.new(**args)
        end

        # Construct {GetAllIndexes} options for {AnalyticsIndexManager#get_all_indexes}
        #
        # @return [GetAllIndexes]
        def GetAllIndexes(**args)
          GetAllIndexes.new(**args)
        end

        # Construct {ConnectLink} options for {AnalyticsIndexManager#connect_link}
        #
        # @return [ConnectLink]
        def ConnectLink(**args)
          ConnectLink.new(**args)
        end

        # Construct {DisconnectLink} options for {AnalyticsIndexManager#disconnect_link}
        #
        # @return [DisconnectLink]
        def DisconnectLink(**args)
          DisconnectLink.new(**args)
        end

        # Construct {GetPendingMutations} options for {AnalyticsIndexManager#get_pending_mutations}
        #
        # @return [GetPendingMutations]
        def GetPendingMutations(**args)
          GetPendingMutations.new(**args)
        end

        # rubocop:enable Naming/MethodName
      end
    end

    class AnalyticsIndexManager
      alias inspect to_s

      # @param [Couchbase::Backend] backend
      def initialize(backend)
        @backend = backend
      end

      # Creates a new dataverse
      #
      # @param [String] dataverse_name
      # @param [Options::Analytics::CreateDataverse] options
      #
      # @return [void]
      #
      # @raise [ArgumentError]
      # @raise [Error::DataverseExists]
      def create_dataverse(dataverse_name, options = Options::Analytics::CreateDataverse.new)
        @backend.analytics_dataverse_create(dataverse_name, options.to_backend)
      end

      # Drops a dataverse
      #
      # @param [String] dataverse_name name of the dataverse
      # @param [Options::Analytics::DropDataverse] options
      #
      # @return [void]
      #
      # @raise [ArgumentError]
      # @raise [Error::DataverseNotFound]
      def drop_dataverse(dataverse_name, options = Options::Analytics::DropDataverse.new)
        @backend.analytics_dataverse_drop(dataverse_name, options.to_backend)
      end

      # Creates a new dataset
      #
      # @param [String] dataset_name name of dataset
      # @param [String] bucket_name name of the bucket
      # @param [Options::Analytics::CreateDataset] options
      #
      # @return [void]
      #
      # @raise [ArgumentError]
      # @raise [Error::DatasetExists]
      # @raise [Error::LinkNotFound]
      def create_dataset(dataset_name, bucket_name, options = Options::Analytics::CreateDataset.new)
        @backend.analytics_dataset_create(dataset_name, bucket_name, options.to_backend)
      end

      # Drops a dataset
      #
      # @param [String] dataset_name name of the dataset
      # @param [Options::Analytics::DropDataset] options
      #
      # @return [void]
      #
      # @raise [ArgumentError]
      # @raise [Error::DatasetNotFound]
      def drop_dataset(dataset_name, options = Options::Analytics::DropDataset.new)
        @backend.analytics_dataset_drop(dataset_name, options.to_backend)
      end

      # Gets all datasets
      #
      # @param [Options::Analytics::GetAllDatasets] options
      #
      # @return [Array<AnalyticsDataset>]
      def get_all_datasets(options = Options::Analytics::GetAllDatasets.new)
        resp = @backend.analytics_dataset_get_all(options.to_backend)
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
      # @param [Options::Analytics::CreateIndex] options
      #
      # @return [void]
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexExists]
      def create_index(index_name, dataset_name, fields, options = Options::Analytics::CreateIndex.new)
        @backend.analytics_index_create(index_name, dataset_name, fields.entries, options.to_backend)
      end

      # Drops an index
      #
      # @param [String] index_name name of the index
      # @param [String] dataset_name name of the dataset
      # @param [Options::Analytics::DropIndex] options
      #
      # @return [void]
      #
      # @raise [ArgumentError]
      # @raise [Error::IndexNotFound]
      def drop_index(index_name, dataset_name, options = Options::Analytics::DropIndex.new)
        @backend.analytics_index_drop(index_name, dataset_name, options.to_backend)
      end

      # Gets all indexes
      #
      # @param [Options::Analytics::GetAllIndexes] options
      #
      # @return [Array<AnalyticsIndex>]
      def get_all_indexes(options = Options::Analytics::GetAllIndexes.new)
        resp = @backend.analytics_index_get_all(options.to_backend)
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
      # @param [Options::Analytics::ConnectLink] options
      #
      # @return [void]
      #
      # @raise [ArgumentError]
      # @raise [Error::LinkNotFound]
      def connect_link(options = Options::Analytics::ConnectLink.new)
        @backend.analytics_link_connect(options.to_backend)
      end

      # Disconnects a link,
      #
      # @param [Options::Analytics::DisconnectLink] options
      #
      # @return [void]
      #
      # @raise [ArgumentError]
      # @raise [Error::LinkNotFound]
      def disconnect_link(options = Options::Analytics::DisconnectLink.new)
        @backend.analytics_link_disconnect(options.to_backend)
      end

      # Gets the pending mutations for all datasets.
      #
      # @note If a link is disconnected then it will return no results. If all links are disconnected, then
      # an empty object is returned.
      #
      # @param [Options::Analytics::GetPendingMutations] options
      #
      # @return [Hash<String => Integer>] dictionary, where keys are dataset coordinates encoded as +"dataverse.dataset"+
      #   and values are number of mutations for given dataset.
      def get_pending_mutations(options = Options::Analytics::GetPendingMutations.new)
        @backend.analytics_get_pending_mutations(options.to_backend)
      end

      # @api private
      # TODO: deprecate after 3.2
      CreateDataverseOptions = ::Couchbase::Management::Options::Analytics::CreateDataverse

      # @api private
      # TODO: deprecate after 3.2
      DropDataverseOptions = ::Couchbase::Management::Options::Analytics::DropDataverse

      # @api private
      # TODO: deprecate after 3.2
      CreateDatasetOptions = ::Couchbase::Management::Options::Analytics::CreateDataset

      # @api private
      # TODO: deprecate after 3.2
      DropDatasetOptions = ::Couchbase::Management::Options::Analytics::DropDataset

      # @api private
      # TODO: deprecate after 3.2
      GetAllDatasetsOptions = ::Couchbase::Management::Options::Analytics::GetAllDatasets

      # @api private
      # TODO: deprecate after 3.2
      CreateIndexOptions = ::Couchbase::Management::Options::Analytics::CreateIndex

      # @api private
      # TODO: deprecate after 3.2
      DropIndexOptions = ::Couchbase::Management::Options::Analytics::DropIndex

      # @api private
      # TODO: deprecate after 3.2
      GetAllIndexesOptions = ::Couchbase::Management::Options::Analytics::GetAllIndexes

      # @api private
      # TODO: deprecate after 3.2
      ConnectLinkOptions = ::Couchbase::Management::Options::Analytics::ConnectLink

      # @api private
      # TODO: deprecate after 3.2
      GetPendingMutationsOptions = ::Couchbase::Management::Options::Analytics::GetPendingMutations
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
      alias primary? is_primary

      # @yieldparam [AnalyticsIndex]
      def initialize
        yield self if block_given?
      end
    end
  end
end
