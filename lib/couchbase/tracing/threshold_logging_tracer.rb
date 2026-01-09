# frozen_string_literal: true

#  Copyright 2025-Present Couchbase, Inc.
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

require 'couchbase/tracing/request_tracer'
require 'couchbase/tracing/threshold_logging_span'
require 'couchbase/utils/observability_constants'
require 'couchbase/logger'
require 'couchbase/utils/stdlib_logger_adapter'

require 'json'

module Couchbase
  module Tracing
    class ThresholdLoggingTracer < RequestTracer
      # @api private
      DEFAULT_KV_THRESHOLD = 500 # milliseconds

      # @api private
      DEFAULT_HTTP_THRESHOLD = 1_000 # milliseconds

      # @api private
      DEFAULT_SAMPLE_SIZE = 10

      # @api private
      DEFAULT_EMIT_INTERVAL = 10_000 # milliseconds

      def initialize(
        emit_interval: nil,
        kv_threshold: nil,
        query_threshold: nil,
        views_threshold: nil,
        search_threshold: nil,
        analytics_threshold: nil,
        management_threshold: nil,
        sample_size: nil
      )
        super()

        emit_interval = DEFAULT_EMIT_INTERVAL if emit_interval.nil?
        kv_threshold = DEFAULT_KV_THRESHOLD if kv_threshold.nil?
        query_threshold = DEFAULT_HTTP_THRESHOLD if query_threshold.nil?
        views_threshold = DEFAULT_HTTP_THRESHOLD if views_threshold.nil?
        search_threshold = DEFAULT_HTTP_THRESHOLD if search_threshold.nil?
        analytics_threshold = DEFAULT_HTTP_THRESHOLD if analytics_threshold.nil?
        management_threshold = DEFAULT_HTTP_THRESHOLD if management_threshold.nil?
        sample_size = DEFAULT_SAMPLE_SIZE if sample_size.nil?

        raise ArgumentError, "The sample size for ThresholdLoggingTracer must be positive" unless sample_size.positive?

        @emit_interval = emit_interval
        @sample_size = sample_size
        @groups = {
          Observability::ATTR_VALUE_SERVICE_KV => Group.new(floor_us: 1000 * kv_threshold, capacity: @sample_size),
          Observability::ATTR_VALUE_SERVICE_QUERY => Group.new(floor_us: 1000 * query_threshold, capacity: @sample_size),
          Observability::ATTR_VALUE_SERVICE_VIEWS => Group.new(floor_us: 1000 * views_threshold, capacity: @sample_size),
          Observability::ATTR_VALUE_SERVICE_SEARCH => Group.new(floor_us: 1000 * search_threshold, capacity: @sample_size),
          Observability::ATTR_VALUE_SERVICE_ANALYTICS => Group.new(floor_us: 1000 * analytics_threshold, capacity: @sample_size),
          Observability::ATTR_VALUE_SERVICE_MANAGEMENT => Group.new(floor_us: 1000 * management_threshold, capacity: @sample_size),
        }

        # TODO(DC): Find better solution for logging
        @logger = Couchbase.logger || Logger.new($stdout, Utils::StdlibLoggerAdapter.map_spdlog_level(Couchbase.log_level))
        start_reporting_thread
      end

      def request_span(name, parent: nil, start_timestamp: nil)
        ThresholdLoggingSpan.new(
          name,
          start_timestamp: start_timestamp,
          parent: parent,
          tracer: self,
        )
      end

      def record_operation(service, item)
        group = @groups[service]
        return if group.nil?

        group.record_operation(item)
      end

      def close
        @thread.exit
      end

      def create_report
        data = {}
        @groups.each do |service, group|
          group_data = group.steal_data
          next if group_data.nil?

          data[service] = group_data
        end
        data
      end

      def start_reporting_thread
        @thread = Thread.new do # rubocop:disable ThreadSafety/NewThread
          loop do
            sleep(@emit_interval / 1_000.0)
            report = create_report

            next if report.empty?

            begin
              @logger.info("Threshold Logging Report: #{report.to_json}")
            rescue StandardError => e
              @logger.debug("Failed to log threshold logging report: #{e.message}")
            end
          end
        end
      end

      class Item
        attr_accessor :total_duration_us
        attr_accessor :encode_duration_us
        attr_accessor :last_dispatch_duration_us
        attr_accessor :total_dispatch_duration_us
        attr_accessor :last_server_duration_us
        attr_accessor :total_server_duration_us
        attr_accessor :operation_name
        attr_accessor :last_local_id
        attr_accessor :operation_id
        attr_accessor :last_remote_socket

        def initialize(
          total_duration_us:,
          encode_duration_us:,
          last_dispatch_duration_us:,
          total_dispatch_duration_us:,
          last_server_duration_us:,
          total_server_duration_us:,
          operation_name:,
          last_local_id:,
          operation_id:,
          last_remote_socket:
        )
          @total_duration_us = total_duration_us
          @encode_duration_us = encode_duration_us
          @last_dispatch_duration_us = last_dispatch_duration_us
          @total_dispatch_duration_us = total_dispatch_duration_us
          @last_server_duration_us = last_server_duration_us
          @total_server_duration_us = total_server_duration_us
          @operation_name = operation_name
          @last_local_id = last_local_id
          @operation_id = operation_id
          @last_remote_socket = last_remote_socket
        end

        def to_h
          {
            total_duration_us: @total_duration_us,
            encode_duration_us: @encode_duration_us,
            last_dispatch_duration_us: @last_dispatch_duration_us,
            total_dispatch_duration_us: @total_dispatch_duration_us,
            last_server_duration_us: @last_server_duration_us,
            total_server_duration_us: @total_server_duration_us,
            operation_name: @operation_name,
            last_local_id: @last_local_id,
            operation_id: @operation_id,
            last_remote_socket: @last_remote_socket,
          }.compact
        end
      end

      class Group
        def initialize(capacity:, floor_us:)
          @capacity = capacity
          @floor_us = floor_us

          @total_count = 0
          @top_requests = []
          @mutex = Mutex.new
        end

        def record_operation(item)
          return if item.total_duration_us < @floor_us

          @mutex.synchronize do
            @total_count += 1

            return if @top_requests.size >= @capacity && item.total_duration_us < @top_requests[-1].total_duration_us

            idx = @top_requests.bsearch_index do |x|
              item.total_duration_us >= x.total_duration_us
            end

            # The item is smaller than all existing items. We will insert it at the end.
            idx = @top_requests.size if idx.nil?

            if @top_requests.size >= @capacity
              # We are at capacity, remove the smallest (last) item
              @top_requests.pop
            end
            @top_requests.insert(idx, item)
          end
        end

        def steal_data
          top_requests, total_count = @mutex.synchronize do
            top_requests_tmp = @top_requests
            total_count_tmp = @total_count
            @top_requests = []
            @total_count = 0
            [top_requests_tmp, total_count_tmp]
          end

          return nil if total_count.zero?

          {
            total_count: total_count,
            top_requests: top_requests.map(&:to_h),
          }
        end
      end
    end
  end
end
