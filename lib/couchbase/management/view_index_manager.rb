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
      module View
        # Options for {ViewIndexManager#get_design_document}
        class GetDesignDocument < Couchbase::Options::Base
          # Creates an instance of options for {ViewIndexManager#get_design_document}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [GetDesignDocument]
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end

          # @api private
          DEFAULT = new.freeze
        end

        # Options for {ViewIndexManager#get_all_design_documents}
        class GetAllDesignDocuments < Couchbase::Options::Base
          # Creates an instance of options for {ViewIndexManager#get_all_design_documents}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [GetAllDesignDocuments]
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end

          # @api private
          DEFAULT = new.freeze
        end

        # Options for {ViewIndexManager#upsert_design_document}
        class UpsertDesignDocument < Couchbase::Options::Base
          # Creates an instance of options for {ViewIndexManager#upsert_design_document}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [UpsertDesignDocument]
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end

          # @api private
          DEFAULT = new.freeze
        end

        # Options for {ViewIndexManager#drop_design_document}
        class DropDesignDocument < Couchbase::Options::Base
          # Creates an instance of options for {ViewIndexManager#drop_design_document}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [DropDesignDocument]
          def initialize(timeout: nil,
                         retry_strategy: nil,
                         parent_span: nil)
            super
            yield self if block_given?
          end

          # @api private
          DEFAULT = new.freeze
        end

        # Options for {ViewIndexManager#publish_design_document}
        class PublishDesignDocument < Couchbase::Options::Base
          # Creates an instance of options for {ViewIndexManager#publish_design_document}
          #
          # @param [Integer, #in_milliseconds, nil] timeout the time in milliseconds allowed for the operation to complete
          # @param [Proc, nil] retry_strategy the custom retry strategy, if set
          # @param [Span, nil] parent_span if set holds the parent span, that should be used for this request
          #
          # @yieldparam [PublishDesignDocument]
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

    # The View Index Manager interface contains the means for managing design documents used for views.
    #
    # A design document belongs to either the "development" or "production" namespace. A development document has a name
    # that starts with "dev_". This is an implementation detail we've chosen to hide from consumers of this API.
    # Document names presented to the user (returned from the "get" and "get all" methods, for example) always have the
    # "dev_" prefix stripped.
    #
    # Whenever the user passes a design document name to any method of this API, the user may refer to the document
    # using the "dev_" prefix regardless of whether the user is referring to a development document or production
    # document. The "dev_" prefix is always stripped from user input, and the actual document name passed to the server
    # is determined by the "namespace" argument.
    #
    # All methods (except publish) have a required "namespace" argument indicating whether the operation targets a
    # development document or a production document. The type of this argument is [Symbol] with allowed values
    # +:production+ and +:development+.
    #
    # @deprecated Views are deprecated in Couchbase Server 7.0+, and will be removed from a future server version.
    #   Views are not compatible with the Magma storage engine. Instead of views, use indexes and queries using the
    #   Index Service (GSI) and the Query Service (SQL++).
    class ViewIndexManager
      alias inspect to_s

      # @return [String] name of the bucket
      attr_accessor :bucket_name

      # @param [Couchbase::Backend] backend
      # @param [String] bucket_name
      # @param [Observability::Wrapper] observability wrapper
      def initialize(backend, bucket_name, observability)
        @backend = backend
        @bucket_name = bucket_name
        @observability = observability
      end

      # Fetches a design document from the server
      #
      # @param [String] name the name of the design document
      # @param [:production, :development] namespace the namespace
      # @param [Options::View::GetDesignDocument] options
      #
      # @return [DesignDocument]
      #
      # @raise [Error::DesignDocumentNotFound]
      def get_design_document(name, namespace, options = Options::View::GetDesignDocument::DEFAULT)
        @observability.record_operation(Observability::OP_VM_GET_DESIGN_DOCUMENT, options.parent_span, self, :views) do |_obs_handler|
          resp = @backend.view_index_get(@bucket_name, name, namespace, options.timeout)
          extract_design_document(resp)
        end
      end

      # Fetches all design documents from the server
      #
      # @param [:production, :development] namespace the namespace
      # @param [Options::View::GetAllDesignDocuments] options
      #
      # @return [Array<DesignDocument>]
      def get_all_design_documents(namespace, options = Options::View::GetAllDesignDocuments::DEFAULT)
        @observability.record_operation(Observability::OP_VM_GET_ALL_DESIGN_DOCUMENTS, options.parent_span, self, :views) do |_obs_handler|
          resp = @backend.view_index_get_all(@bucket_name, namespace, options.timeout)
          resp.map do |entry|
            extract_design_document(entry)
          end
        end
      end

      # Updates or inserts the design document
      #
      # @param [DesignDocument] document
      # @param [:production, :development] namespace the namespace
      # @param [Options::View::UpsertDesignDocument] options
      #
      # @return [void]
      def upsert_design_document(document, namespace, options = Options::View::UpsertDesignDocument::DEFAULT)
        @observability.record_operation(Observability::OP_VM_UPSERT_DESIGN_DOCUMENT, options.parent_span, self, :views) do |_obs_handler|
          @backend.view_index_upsert(@bucket_name, {
            name: document.name,
            views: document.views.map do |name, view|
              {
                name: name,
                map: view.map_function,
                reduce: view.reduce_function,
              }
            end,
          }, namespace, options.timeout)
        end
      end

      # Removes the design document
      #
      # @param [String] name design document name
      # @param [:production, :development] namespace the namespace
      # @param [Options::View::DropDesignDocument] options
      #
      # @return [void]
      #
      # @raise [Error::DesignDocumentNotFound]
      def drop_design_document(name, namespace, options = Options::View::DropDesignDocument::DEFAULT)
        @observability.record_operation(Observability::OP_VM_DROP_DESIGN_DOCUMENT, options.parent_span, self, :views) do |_obs_handler|
          @backend.view_index_drop(@bucket_name, name, namespace, options.timeout)
        end
      end

      # Publishes the design document.
      #
      # This method is equivalent to getting a document from the development namespace and upserting
      # it to the production namespace.
      #
      # @param [String] name design document name
      # @param [Options::View::PublishDesignDocument] options
      #
      # @return [void]
      #
      # @raise [ArgumentError]
      # @raise [Error::DesignDocumentNotFound]
      def publish_design_document(name, options = Options::View::PublishDesignDocument::DEFAULT)
        @observability.record_operation(Observability::OP_VM_PUBLISH_DESIGN_DOCUMENT, options.parent_span, self, :views) do |_obs_handler|
          document = get_design_document(name, :development, Options::View::GetDesignDocument.new(timeout: options.timeout))
          upsert_design_document(document, :production, Options::View::UpsertDesignDocument.new(timeout: options.timeout))
        end
      end

      # @api private
      # @deprecated use {Options::View::GetDesignDocument} instead
      GetDesignDocumentOptions = Options::View::GetDesignDocument

      # @api private
      # @deprecated use {Options::View::GetAllDesignDocuments} instead
      GetAllDesignDocumentsOptions = Options::View::GetAllDesignDocuments

      # @api private
      # @deprecated use {Options::View::UpsertDesignDocument} instead
      UpsertDesignDocumentOptions = Options::View::UpsertDesignDocument

      # @api private
      # @deprecated use {Options::View::DropDesignDocument} instead
      DropDesignDocumentOptions = Options::View::DropDesignDocument

      # @api private
      # @deprecated use {Options::View::PublishDesignDocument} instead
      PublishDesignDocumentOptions = Options::View::PublishDesignDocument

      private

      def extract_design_document(resp)
        DesignDocument.new do |design_document|
          design_document.name = resp[:name]
          design_document.namespace = resp[:namespace]
          resp[:views].each do |name, view_resp|
            design_document.views[name] = View.new(view_resp[:map], view_resp[:reduce])
          end
        end
      end
    end

    class View
      # @return [String] name of the view
      attr_accessor :view

      # @return [String] map function in javascript as String
      attr_accessor :map_function
      alias map map_function

      # @return [String] reduce function in javascript as String
      attr_accessor :reduce_function
      alias reduce reduce_function

      # @return [Boolean] true if map function is defined
      def has_map?
        !@map_function.nil?
      end

      # @return [Boolean] true if map function is defined
      def has_reduce?
        !@reduce_function.nil?
      end

      # @param [String] map
      # @param [String] reduce
      #
      # @yieldparam [View] self
      def initialize(map = nil, reduce = nil)
        @map_function = map
        @reduce_function = reduce
        yield self if block_given?
      end
    end

    class DesignDocument
      # @return [String] name
      attr_accessor :name

      # @return [Hash<String => View>]
      attr_accessor :views

      # @return [:production, :development]
      attr_accessor :namespace

      # @yieldparam [DesignDocument] self
      def initialize
        @views = {}
        yield self if block_given?
      end
    end
  end
end
