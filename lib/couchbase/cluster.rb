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

require "couchbase/authenticator"
require "couchbase/bucket"

require "couchbase/management/user_manager"
require "couchbase/management/bucket_manager"
require "couchbase/management/query_index_manager"
require "couchbase/management/analytics_index_manager"
require "couchbase/management/search_index_manager"

require "couchbase/search_options"

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
      resp = @backend.document_query(statement, {
          timeout: options.timeout,
          adhoc: options.adhoc,
          client_context_id: options.client_context_id,
          max_parallelism: options.max_parallelism,
          readonly: options.readonly,
          scan_wait: options.scan_wait,
          scan_cap: options.scan_cap,
          pipeline_batch: options.pipeline_batch,
          pipeline_cap: options.pipeline_cap,
          metrics: options.metrics,
          profile: options.profile,
          positional_parameters: options.instance_variable_get("@positional_parameters")&.map { |p| JSON.dump(p) },
          named_parameters: options.instance_variable_get("@named_parameters")&.each_with_object({}) { |(n, v), o| o[n.to_s] = JSON.dump(v) },
          raw_parameters: options.instance_variable_get("@raw_parameters"),
          scan_consistency: options.instance_variable_get("@scan_consistency"),
          mutation_state: options.instance_variable_get("@mutation_state")&.tokens&.map { |t|
            {
                bucket_name: t.bucket_name,
                partition_id: t.partition_id,
                partition_uuid: t.partition_uuid,
                sequence_number: t.sequence_number,
            }
          },
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
    # @return [SearchResult]
    def search_query(index_name, query, options = SearchOptions.new)
      resp = @backend.document_search(index_name, JSON.generate(query), {
          timeout: options.timeout,
          limit: options.limit,
          skip: options.skip,
          explain: options.explain,
          highlight_style: options.highlight_style,
          highlight_fields: options.highlight_fields,
          fields: options.fields,
          sort: options.sort&.map { |v| JSON.generate(v) },
          facets: options.facets&.map { |(k, v)| [k, JSON.generate(v)] },
          scan_consistency: options.instance_variable_get("@scan_consistency"),
          mutation_state: options.instance_variable_get("@mutation_state")&.tokens&.map { |t|
            {
                bucket_name: t.bucket_name,
                partition_id: t.partition_id,
                partition_uuid: t.partition_uuid,
                sequence_number: t.sequence_number,
            }
          },
      })

      SearchResult.new do |res|
        res.meta_data = SearchMetaData.new do |meta|
          meta.metrics.max_score = resp[:meta_data][:metrics][:max_score]
          meta.metrics.error_partition_count = resp[:meta_data][:metrics][:error_partition_count]
          meta.metrics.success_partition_count = resp[:meta_data][:metrics][:success_partition_count]
          meta.metrics.took = resp[:meta_data][:metrics][:took]
          meta.metrics.total_rows = resp[:meta_data][:metrics][:total_rows]
        end
        res.rows = resp[:rows].map do |r|
          SearchRow.new do |row|
            row.transcoder = options.transcoder
            row.index = r[:index]
            row.id = r[:id]
            row.score = r[:score]
            row.fragments = r[:fragments]
            row.locations = SearchRowLocations.new(
                r[:locations].map do |loc|
                  SearchRowLocation.new do |location|
                    location.field = loc[:field]
                    location.term = loc[:term]
                    location.position = loc[:position]
                    location.start_offset = loc[:start_offset]
                    location.end_offset = loc[:end_offset]
                    location.array_positions = loc[:array_positions]
                  end
                end
            )
            row.instance_variable_set("@fields", r[:fields])
            row.explanation = JSON.parse(r[:explanation]) if r[:explanation]
          end
        end
        res.facets = resp[:facets]&.each_with_object({}) do |(k, v), o|
          facet = case options.facets[k]
                  when SearchFacet::SearchFacetTerm
                    SearchFacetResult::TermFacetResult.new do |f|
                      f.terms = v[:terms]&.map do |t|
                        SearchFacetResult::TermFacetResult::TermFacet.new(t[:term], t[:count])
                      end || []
                    end
                  when SearchFacet::SearchFacetDateRange
                    SearchFacetResult::DateRangeFacetResult.new do |f|
                      f.date_ranges = v[:date_ranges]&.map do |r|
                        SearchFacetResult::DateRangeFacetResult::DateRangeFacet.new(r[:name], r[:count], r[:start_time], r[:end_time])
                      end || []
                    end
                  when SearchFacet::SearchFacetNumericRange
                    SearchFacetResult::NumericRangeFacetResult.new do |f|
                      f.numeric_ranges = v[:numeric_ranges]&.map do |r|
                        SearchFacetResult::NumericRangeFacetResult::NumericRangeFacet.new(r[:name], r[:count], r[:min], r[:max])
                      end || []
                    end
                  else
                    next # ignore unknown facet result
                  end
          facet.name = v[:name]
          facet.field = v[:field]
          facet.total = v[:total]
          facet.missing = v[:missing]
          facet.other = v[:other]
          o[k] = facet
        end
      end
    end

    # @return [Management::UserManager]
    def users
      Management::UserManager.new(@backend)
    end

    # @return [Management::BucketManager]
    def buckets
      Management::BucketManager.new(@backend)
    end

    # @return [Management::QueryIndexManager]
    def query_indexes
      Management::QueryIndexManager.new(@backend)
    end

    # @return [Management::AnalyticsIndexManager]
    def analytics_indexes
      Management::AnalyticsIndexManager.new(@backend)
    end

    # @return [Management::SearchIndexManager]
    def search_indexes
      Management::SearchIndexManager.new(@backend)
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
      # @return [Integer] Timeout in milliseconds
      attr_accessor :timeout

      # @return [Boolean] Allows turning this request into a prepared statement query
      attr_accessor :adhoc

      # @return [String] Provides a custom client context ID for this query
      attr_accessor :client_context_id

      # @return [Integer] Allows overriding the default maximum parallelism for the query execution on the server side.
      attr_accessor :max_parallelism

      # @return [Boolean] Allows explicitly marking a query as being readonly and not mutating and documents on the server side.
      attr_accessor :readonly

      # Allows customizing how long (in milliseconds) the query engine is willing to wait until the index catches up to whatever scan consistency is asked for in this query.
      #
      # @note that if +:not_bounded+ consistency level is used, this method doesn't do anything
      # at all. If no value is provided to this method, the server default is used.
      #
      # @return [Integer] The maximum duration (in milliseconds) the query engine is willing to wait before failing.
      attr_accessor :scan_wait

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
        @timeout = 75_000 # ms
        @adhoc = true
        @raw_parameters = {}
        @positional_parameters = nil
        @named_parameters = nil
        @scan_consistency = nil
        @mutation_state = nil
        yield self if block_given?
      end

      # Allows providing custom JSON key/value pairs for advanced usage
      #
      # @param [String] key the parameter name (key of the JSON property)
      # @param [Object] value the parameter value (value of the JSON property)
      def raw(key, value)
        @raw_parameters[key] = JSON.generate(value)
      end

      # Customizes the consistency guarantees for this query
      #
      # @note overrides consistency level set by {#consistent_with}
      #
      # [+:not_bounded+] The indexer will return whatever state it has to the query engine at the time of query. This is the default (for single-statement requests).
      #
      # [+:request_plus+] The indexer will wait until all mutations have been processed at the time of request before returning to the query engine.
      #
      # @param [:not_bounded, :request_plus] level the index scan consistency to be used for this query
      def scan_consistency=(level)
        @mutation_state = nil if @mutation_state
        @scan_consistency = level
      end

      # Sets the mutation tokens this query should be consistent with
      #
      # @note overrides consistency level set by {#scan_consistency=}
      #
      # @param [MutationState] mutation_state the mutation state containing the mutation tokens
      def consistent_with(mutation_state)
        @scan_consistency = nil if @scan_consistency
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

    class QueryResult
      # @return [QueryMetaData] returns object representing additional metadata associated with this query
      attr_accessor :meta_data

      attr_accessor :transcoder

      # Returns all rows converted using a transcoder
      #
      # @return [Array]
      def rows(transcoder = self.transcoder)
        @rows.lazy.map do |row|
          if transcoder == :json
            JSON.parse(row)
          else
            transcoder.call(row)
          end
        end
      end

      # @yieldparam [QueryResult] self
      def initialize
        yield self if block_given?
        @transcoder = :json
      end
    end

    class QueryMetaData
      # @return [String] returns the request identifier string of the query request
      attr_accessor :request_id

      # @return [String] returns the client context identifier string set of the query request
      attr_accessor :client_context_id

      # @return [Symbol] returns raw query execution status as returned by the query engine
      attr_accessor :status

      # @return [Hash] returns the signature as returned by the query engine which is then decoded as JSON object
      attr_accessor :signature

      # @return [Hash] returns the profiling information returned by the query engine which is then decoded as JSON object
      attr_accessor :profile

      # @return [QueryMetrics] metrics as returned by the query engine, if enabled
      attr_accessor :metrics

      # @return [List<QueryWarning>] list of warnings returned by the query engine
      attr_accessor :warnings

      # @yieldparam [QueryMetaData] self
      def initialize
        yield self if block_given?
      end
    end

    class QueryMetrics
      # @return [Integer] The total time taken for the request, that is the time from when the request was received until the results were returned
      attr_accessor :elapsed_time

      # @return [Integer] The time taken for the execution of the request, that is the time from when query execution started until the results were returned
      attr_accessor :execution_time

      # @return [Integer] the total number of results selected by the engine before restriction through LIMIT clause.
      attr_accessor :sort_count

      # @return [Integer] The total number of objects in the results.
      attr_accessor :result_count

      # @return [Integer] The total number of bytes in the results.
      attr_accessor :result_size

      # @return [Integer] The number of mutations that were made during the request.
      attr_accessor :mutation_count

      # @return [Integer] The number of errors that occurred during the request.
      attr_accessor :error_count

      # @return [Integer] The number of warnings that occurred during the request.
      attr_accessor :warning_count

      # @yieldparam [QueryMetrics] self
      def initialize
        yield self if block_given?
      end
    end

    # Represents a single warning returned from the query engine.
    class QueryWarning
      # @return [Integer]
      attr_accessor :code

      # @return [String]
      attr_accessor :message

      def initialize(code, message)
        @code = code
        @message = message
      end
    end

    class AnalyticsOptions
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
