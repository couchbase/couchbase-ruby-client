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

require 'couchbase/authenticator'
require 'couchbase/bucket'

require 'couchbase/management/user_manager'
require 'couchbase/management/bucket_manager'
require 'couchbase/management/query_index_manager'
require 'couchbase/management/analytics_index_manager'
require 'couchbase/management/search_index_manager'

module Couchbase
  class Cluster
    alias_method :inspect, :to_s

    # Connect to the Couchbase cluster
    #
    # @param [String] connection_string connection string used to locate the Couchbase Cluster
    # @param [ClusterOptions] options custom options when creating the cluster connection
    #
    # @return [Cluster]
    def self.connect(connection_string, options)
      Cluster.new(connection_string, options)
    end

    def bucket(name)
      Bucket.new(@backend, name)
    end

    # Performs a query against the query (N1QL) services
    # 
    # @param [String] statement the N1QL query statement
    # @param [QueryOptions] options the custom options for this query
    #
    # @return [QueryResult]
    def query(statement, options = QueryOptions.new)
      resp = @backend.query(statement, {
          adhoc: options.adhoc,
          client_context_id: options.client_context_id,
          max_parallelism: options.max_parallelism,
          readonly: options.readonly,
          scan_cap: options.scan_cap,
          pipeline_batch: options.pipeline_batch,
          pipeline_cap: options.pipeline_cap,
          metrics: options.metrics,
          profile: options.profile,
          positional_parameters: options.instance_variable_get("@positional_parameters")&.map { |p| JSON.dump(p) },
          named_parameters: options.instance_variable_get("@named_parameters")&.each_with_object({}) { |(n, v), o| o[n.to_s] = JSON.dump(v) }
      })

      QueryResult.new do |res|
        res.meta_data = QueryMetaData.new do |meta|
          meta.status = resp[:meta][:status]
          meta.request_id = resp[:meta][:request_id]
          meta.client_context_id = resp[:meta][:client_context_id]
          meta.signature = JSON.parse(resp[:meta][:signature]) if resp[:meta][:signature]
          meta.profile = JSON.parse(resp[:meta][:profile]) if resp[:meta][:profile]
          meta.metrics = QueryMetrics.new do |metrics|
            if resp[:meta][:metrics]
              metrics.elapsed_time = resp[:meta][:metrics][:elapsed_time]
              metrics.execution_time = resp[:meta][:metrics][:execution_time]
              metrics.sort_count = resp[:meta][:metrics][:sort_count]
              metrics.result_count = resp[:meta][:metrics][:result_count]
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

    # Performs an analytics query
    # 
    # @param [String] statement the N1QL query statement
    # @param [AnalyticsOptions] options the custom options for this query
    #
    # @return [AnalyticsResult]
    def analytics_query(statement, options = AnalyticsOptions.new) end

    # Performs a Full Text Search (FTS) query
    #
    # @param [String] index_name the name of the search index
    # @param [SearchQuery] query the query tree
    # @param [SearchOptions] options the query tree
    #
    # @return [QueryResult]
    def search_query(index_name, query, options = SearchOptions.new) end

    def users
      UserManager.new(@backend)
    end

    def buckets
      BucketManager.new(@backend)
    end

    def query_indexes
      QueryIndexManager.new(@backend)
    end

    def analytics_indexes
      AnalyticsIndexManager.new(@backend)
    end

    def search_indexes
      SearchIndexManager.new(@backend)
    end

    def disconnect
      @backend.close
    end

    def diagnostics(options = DiagnosticsOptions.new) end

    class ClusterOptions
      attr_accessor :authenticator

      def initialize
        yield self if block_given?
      end

      def authenticate(username, password)
        @authenticator = PasswordAuthenticator.new(username, password)
        self
      end
    end

    class QueryOptions
      # @return [Boolean] Allows turning this request into a prepared statement query
      attr_accessor :adhoc

      # @return [String] Provides a custom client context ID for this query
      attr_accessor :client_context_id

      # @return [Integer] Allows overriding the default maximum parallelism for the query execution on the server side.
      attr_accessor :max_parallelism

      # @return [Boolean] Allows explicitly marking a query as being readonly and not mutating and documents on the server side.
      attr_accessor :readonly

      # @return [Integer] Supports customizing the maximum buffered channel size between the indexer and the query service
      attr_accessor :scan_cap

      # @return [Integer] Supports customizing the number of items execution operators can batch for fetch from the KV layer on the server.
      attr_accessor :pipeline_batch

      # @return [Integer] Allows customizing the maximum number of items each execution operator can buffer between various operators on the server.
      attr_accessor :pipeline_cap

      # @return [Boolean] Enables per-request metrics in the trailing section of the query
      attr_accessor :metrics

      # @return [:off, :phases, :timings] Customize server profile level for this query
      attr_accessor :profile

      def initialize
        @adhoc = true
        yield self if block_given?
        @positional_parameters = nil
        @named_parameters = nil
      end

      # Allows providing custom JSON key/value pairs for advanced usage
      #
      # @param [String] key the parameter name (key of the JSON property)
      # @param [Object] value the parameter value (value of the JSON property)
      def raw(key, value) end

      # Customizes the consistency guarantees for this query
      #
      # * +:not_bounded+ The indexer will return whatever state it has to the query engine at the time of query. This is the default (for single-statement requests).
      #
      # * +:request_plus+ The indexer will wait until all mutations have been processed at the time of request before returning to the query engine.
      #
      # @param [:not_bounded, :request_plus] level the index scan consistency to be used for this query
      def scan_consistency=(level)
        @scan_consistency = level
      end

      # Sets the mutation tokens this query should be consistent with
      #
      #
      # @param [MutationState] mutation_state the mutation state containing the mutation tokens
      def consistent_with(mutation_state)
        @mutation_state = mutation_state
      end

      # Sets positional parameters for the query
      #
      # @param [Array] positional the list of parameters that have to be substituted in the statement
      def positional_parameters(positional)
        @positional_parameters = positional
        @named_parameters = nil
      end

      # Sets named parameters for the query
      #
      # @param [Hash] named the key/value map of the parameters to substitute in the statement
      def named_parameters(named)
        @named_parameters = named
        @positional_parameters = nil
      end
    end

    class AnalyticsOptions
      def initialize
        yield self if block_given?
      end
    end

    class SearchOptions
      def initialize
        yield self if block_given?
      end
    end

    class DiagnosticsOptions
      # @return [String] Holds custom report id.
      attr_accessor :report_id

      def initialize
        yield self if block_given?
      end
    end

    private

    def initialize(connection_string, options)
      hostname = connection_string[%r{^couchbase://(.*)}, 1]
      raise ArgumentError, "missing hostname" unless hostname
      raise ArgumentError, "options must have authenticator configured" unless options.authenticator
      username = options.authenticator.username
      raise ArgumentError, "missing username" unless username
      password = options.authenticator.password
      raise ArgumentError, "missing password" unless password

      @backend = Backend.new
      @backend.open(hostname, username, password)
    end
  end
end