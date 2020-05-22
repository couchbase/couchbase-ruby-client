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
    class ViewIndexManager
      alias_method :inspect, :to_s

      # @param [Couchbase::Backend] backend
      # @param [String] bucket_name
      def initialize(backend, bucket_name)
        @backend = backend
        @bucket_name = bucket_name
      end

      # Fetches a design document from the server
      #
      # @param [String] name the name of the design document
      # @param [:production, :development] namespace the namespace
      # @param [GetDesignDocumentOptions] options
      #
      # @return [DesignDocument]
      #
      # @raise [Error::DesignDocumentNotFound]
      def get_design_document(name, namespace, options = GetDesignDocumentOptions.new)
        # GET /{bucket_name}/_design/{namespace}_{name}
      end

      # Fetches all design documents from the server
      #
      # @param [:production, :development] namespace the namespace
      # @param [GetAllDesignDocumentsOptions] options
      #
      # @return [Array<DesignDocument>]
      def get_all_design_documents(namespace, options = GetAllDesignDocumentsOptions.new)
        # GET /pools/default/buckets/{bucket_name}/ddocs
      end

      # Updates or inserts the design document
      #
      # @param [DesignDocument] document
      # @param [:production, :development] namespace the namespace
      # @param [UpsertDesignDocumentOptions] options
      def upsert_design_document(document, namespace, options = UpsertDesignDocumentOptions.new)
        # PUT /{bucket_name}/_design/{namespace}_{name}
      end

      # Removes the design document
      #
      # @param [String] name design document name
      # @param [:production, :development] namespace the namespace
      # @param [DropDesignDocumentOptions] options
      #
      # @raise [Error::DesignDocumentNotFound]
      def drop_design_document(name, namespace, options = DropDesignDocumentOptions.new)
        # DELETE /{bucket_name}/_design/{namespace}_{name}
      end

      # Publishes the design document.
      #
      # This method is equivalent to getting a document from the development namespace and upserting
      # it to the production namespace.
      #
      # @param [String] name design document name
      # @param [PublishDesignDocumentOptions] options
      #
      # @raise [ArgumentError]
      # @raise [Error::DesignDocumentNotFound]
      def publish_design_document(name, options = PublishDesignDocumentOptions.new)
      end

      class GetDesignDocumentOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [GetDesignDocumentOptions] self
        def initialize
          yield self if block_given?
        end
      end

      class GetAllDesignDocumentsOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [GetAllDesignDocumentsOptions] self
        def initialize
          yield self if block_given?
        end
      end

      class UpsertDesignDocumentOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [UpsertDesignDocumentOptions] self
        def initialize
          yield self if block_given?
        end
      end

      class DropDesignDocumentOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [DropDesignDocumentOptions] self
        def initialize
          yield self if block_given?
        end
      end

      class PublishDesignDocumentOptions
        # @return [Integer] the time in milliseconds allowed for the operation to complete
        attr_accessor :timeout

        # @yieldparam [PublishDesignDocumentOptions] self
        def initialize
          yield self if block_given?
        end
      end

    end

    class View
      # @return [String] map function in javascript
      attr_accessor :map

      # @return [String] reduce function in javascript
      attr_accessor :reduce

      # @yieldparam [View] self
      def initialize
        yield self if block_given?
      end
    end

    class DesignDocument
      # @return [String] name
      attr_accessor :name

      # @return [Hash<String, View>]
      attr_accessor :views

      # @yieldparam [DesignDocument] self
      def initialize
        yield self if block_given?
      end
    end
  end
end
