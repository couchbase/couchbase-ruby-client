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

require 'json'

module Couchbase
  class Cluster
    class QueryOptions
      # @return [Integer] Timeout in milliseconds
      attr_accessor :timeout

      # @return [Boolean] Allows turning this request into a prepared statement query
      attr_accessor :adhoc

      # @return [String] Provides a custom client context ID for this query
      attr_accessor :client_context_id

      # @return [Integer] Allows overriding the default maximum parallelism for the query execution on the server side.
      attr_accessor :max_parallelism

      # @return [Boolean] Allows explicitly marking a query as being readonly and not mutating any documents on the server side.
      attr_accessor :readonly

      # Allows customizing how long (in milliseconds) the query engine is willing to wait until the index catches up to whatever scan
      # consistency is asked for in this query.
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

      # @return [Integer] Allows customizing the maximum number of items each execution operator can buffer between various operators on the
      #   server.
      attr_accessor :pipeline_cap

      # @return [Boolean] Enables per-request metrics in the trailing section of the query
      attr_accessor :metrics

      # @return [:off, :phases, :timings] Customize server profile level for this query
      attr_accessor :profile

      # Associate scope qualifier (also known as +query_context+) with the query.
      #
      # The qualifier must be in form +{bucket_name}.{scope_name}+ or +default:{bucket_name}.{scope_name}+.
      #
      # @api uncommitted
      # @return [String]
      attr_accessor :scope_qualifier

      # @return [:not_bounded, :request_plus]
      attr_reader :scan_consistency

      # @api private
      # @return [MutationState]
      attr_reader :mutation_state

      # @api private
      # @return [Hash<String => #to_json>]
      attr_reader :raw_parameters

      # @yieldparam [QueryOptions] self
      def initialize
        @timeout = 75_000 # ms
        @adhoc = true
        @raw_parameters = {}
        @positional_parameters = nil
        @named_parameters = nil
        @scan_consistency = nil
        @mutation_state = nil
        @scope_qualifier = nil
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
      # [+:not_bounded+] The indexer will return whatever state it has to the query engine at the time of query. This is the default (for
      #   single-statement requests).
      #
      # [+:request_plus+] The indexer will wait until all mutations have been processed at the time of request before returning to the query
      #   engine.
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

      # @api private
      # @return [Array<String>, nil]
      def export_positional_parameters
        @positional_parameters&.map { |p| JSON.dump(p) }
      end

      # Sets named parameters for the query
      #
      # @param [Hash] named the key/value map of the parameters to substitute in the statement
      def named_parameters(named)
        @named_parameters = named
        @positional_parameters = nil
      end

      # @api private
      # @return [Hash<String => String>, nil]
      def export_named_parameters
        @named_parameters&.each_with_object({}) { |(n, v), o| o[n.to_s] = JSON.dump(v) }
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

      # @return [Array<QueryWarning>] list of warnings returned by the query engine
      attr_accessor :warnings

      # @yieldparam [QueryMetaData] self
      def initialize
        yield self if block_given?
      end
    end

    class QueryMetrics
      # @return [Integer] The total time taken for the request, that is the time from when the request was received until the results were
      #   returned.
      attr_accessor :elapsed_time

      # @return [Integer] The time taken for the execution of the request, that is the time from when query execution started until the
      #   results were returned
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
  end
end
