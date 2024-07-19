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

require "couchbase/json_transcoder"

module Couchbase
  class Cluster
    class AnalyticsWarning
      # @return [Integer]
      attr_accessor :code

      # @return [String]
      attr_accessor :message

      # @param [Integer] code
      # @param [String] message
      def initialize(code, message)
        @code = code
        @message = message
      end
    end

    class AnalyticsMetrics
      # @return [Integer] duration in milliseconds
      attr_accessor :elapsed_time

      # @return [Integer] duration in milliseconds
      attr_accessor :execution_time

      # @return [Integer]
      attr_accessor :result_count

      # @return [Integer]
      attr_accessor :result_size

      # @return [Integer]
      attr_accessor :error_count

      # @return [Integer]
      attr_accessor :processed_objects

      # @return [Integer]
      attr_accessor :warning_count

      # @yieldparam [AnalyticsMetrics] self
      def initialize
        yield self if block_given?
      end
    end

    class AnalyticsMetaData
      # @return [String]
      attr_accessor :request_id

      # @return [String]
      attr_accessor :client_context_id

      # @return [:running, :success, :errors, :completed, :stopped, :timeout, :closed, :fatal, :aborted, :unknown]
      attr_accessor :status

      # @return [Hash] returns the signature as returned by the query engine which is then decoded as JSON object
      attr_accessor :signature

      # @return [Array<AnalyticsWarning>]
      attr_accessor :warnings

      # @return [AnalyticsMetrics]
      attr_accessor :metrics

      # @yieldparam [AnalyticsMetaData] self
      def initialize
        yield self if block_given?
      end
    end

    class AnalyticsResult
      # @return [AnalyticsMetaData]
      attr_accessor :meta_data

      attr_accessor :transcoder

      # Returns all rows converted using a transcoder
      #
      # @return [Array]
      def rows(transcoder = self.transcoder)
        @rows.lazy.map { |row| transcoder.decode(row, 0) }
      end

      # @yieldparam [AnalyticsResult] self
      def initialize
        @transcoder = JsonTranscoder.new
        yield self if block_given?
      end
    end
  end
end
