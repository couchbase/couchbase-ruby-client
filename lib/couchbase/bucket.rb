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

require "couchbase/scope"
require "couchbase/management/collection_manager"
require "couchbase/management/view_index_manager"
require "couchbase/options"
require "couchbase/view_options"
require "couchbase/diagnostics"

module Couchbase
  # Provides access to a Couchbase bucket APIs
  class Bucket
    # @return [String] name of the bucket
    attr_reader :name

    alias inspect to_s

    # @param [Couchbase::Backend] backend
    def initialize(backend, name)
      backend.open_bucket(name, true)
      @backend = backend
      @name = name
    end

    # Get default scope
    #
    # @return [Scope]
    def default_scope
      Scope.new(@backend, @name, "_default")
    end

    # Get a named scope
    #
    # @param [String] scope_name name of the scope
    #
    # @return [Scope]
    def scope(scope_name)
      Scope.new(@backend, @name, scope_name)
    end

    # Opens the named collection in the default scope of the bucket
    #
    # @param [String] collection_name name of the collection
    #
    # @return [Collection]
    def collection(collection_name)
      default_scope.collection(collection_name)
    end

    # Opens the default collection for this bucket
    #
    # @return [Collection]
    def default_collection
      Collection.new(@backend, @name, "_default", "_default")
    end

    # Performs query to view index.
    #
    # @param [String] design_document_name name of the design document
    # @param [String] view_name name of the view to query
    # @param [Options::View] options
    #
    # @example Make sure the view engine catch up with all mutations and return keys starting from +["random_brewery:"]+
    #   bucket.view_query("beer", "brewery_beers",
    #                     Options::View(
    #                       start_key: ["random_brewery:"],
    #                       scan_consistency: :request_plus
    #                     ))
    #
    # @return [ViewResult]
    def view_query(design_document_name, view_name, options = Options::View::DEFAULT)
      resp = @backend.document_view(@name, design_document_name, view_name, options.namespace, options.to_backend)
      ViewResult.new do |res|
        res.meta_data = ViewMetaData.new do |meta|
          meta.total_rows = resp[:meta][:total_rows]
          meta.debug_info = resp[:meta][:debug_info]
        end
        res.rows = resp[:rows].map do |entry|
          ViewRow.new do |row|
            row.id = entry[:id] if entry.key?(:id)
            row.key = JSON.parse(entry[:key])
            row.value = JSON.parse(entry[:value])
          end
        end
      end
    end

    # @return [Management::CollectionManager]
    def collections
      Management::CollectionManager.new(@backend, @name)
    end

    # @return [Management::ViewIndexManager]
    def view_indexes
      Management::ViewIndexManager.new(@backend, @name)
    end

    # Performs application-level ping requests against services in the couchbase cluster
    #
    # @param [Options::Ping] options
    #
    # @return [PingResult]
    def ping(options = Options::Ping::DEFAULT)
      resp = @backend.ping(@name, options.to_backend)
      PingResult.new do |res|
        res.version = resp[:version]
        res.id = resp[:id]
        res.sdk = resp[:sdk]
        resp[:services].each do |type, svcs|
          res.services[type] = svcs.map do |svc|
            PingResult::ServiceInfo.new do |info|
              info.id = svc[:id]
              info.state = svc[:state]
              info.latency = svc[:latency]
              info.remote = svc[:remote]
              info.local = svc[:local]
              info.error = svc[:error]
            end
          end
        end
      end
    end

    # @api private
    # TODO: deprecate in 3.1
    PingOptions = ::Couchbase::Options::Ping

    # @api private
    # TODO: deprecate in 3.1
    ViewOptions = ::Couchbase::Options::View
  end
end
