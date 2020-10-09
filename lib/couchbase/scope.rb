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

require "couchbase/collection"
require "couchbase/query_options"

module Couchbase
  class Scope
    attr_reader :bucket_name
    attr_reader :name

    alias inspect to_s

    # @param [Couchbase::Backend] backend
    # @param [String] bucket_name name of the bucket
    # @param [String, :_default] scope_name name of the scope
    def initialize(backend, bucket_name, scope_name)
      @backend = backend
      @bucket_name = bucket_name
      @name = scope_name
    end

    # Opens the default collection for this scope
    #
    # @param [String] collection_name name of the collection
    #
    # @return [Collection]
    def collection(collection_name)
      Collection.new(@backend, @bucket_name, @name, collection_name)
    end

    # Performs a query against the query (N1QL) services.
    #
    # The query will be implicitly scoped using current bucket and scope names.
    #
    # @see Options::Query#scope_qualifier
    # @see Cluster#query
    #
    # @param [String] statement the N1QL query statement
    # @param [Options::Query] options the custom options for this query
    #
    # @example Select first ten hotels from travel sample dataset
    #   cluster.query("SELECT * FROM `travel-sample` WHERE type = $type LIMIT 10",
    #                 Options::Query(named_parameters: {type: "hotel"}, metrics: true))
    #
    # @return [QueryResult]
    def query(statement, options = Options::Query.new)
      resp = @backend.document_query(statement, options.to_backend(scope_name: @name, bucket_name: @bucket_name))

      Cluster::QueryResult.new do |res|
        res.meta_data = Cluster::QueryMetaData.new do |meta|
          meta.status = resp[:meta][:status]
          meta.request_id = resp[:meta][:request_id]
          meta.client_context_id = resp[:meta][:client_context_id]
          meta.signature = JSON.parse(resp[:meta][:signature]) if resp[:meta][:signature]
          meta.profile = JSON.parse(resp[:meta][:profile]) if resp[:meta][:profile]
          meta.metrics = Cluster::QueryMetrics.new do |metrics|
            if resp[:meta][:metrics]
              metrics.elapsed_time = resp[:meta][:metrics][:elapsed_time]
              metrics.execution_time = resp[:meta][:metrics][:execution_time]
              metrics.sort_count = resp[:meta][:metrics][:sort_count]
              metrics.result_count = resp[:meta][:metrics][:result_count]
              metrics.result_size = resp[:meta][:metrics][:result_size]
              metrics.mutation_count = resp[:meta][:metrics][:mutation_count]
              metrics.error_count = resp[:meta][:metrics][:error_count]
              metrics.warning_count = resp[:meta][:metrics][:warning_count]
            end
          end
          res[:warnings] = resp[:warnings].map { |warn| QueryWarning.new(warn[:code], warn[:message]) } if resp[:warnings]
        end
        res.instance_variable_set("@rows", resp[:rows])
      end
    end
  end
end
