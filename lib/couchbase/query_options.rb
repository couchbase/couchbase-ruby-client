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

require "json"

module Couchbase
  class Cluster
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
      # @return [Integer] The total time taken for the request (in nanoseconds), that is the time from when the request
      #   was received until the results were returned.
      attr_accessor :elapsed_time

      # @return [Integer] The time taken for the execution of the request (in nanoseconds), that is the time from when
      #   query execution started until the results were returned
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
