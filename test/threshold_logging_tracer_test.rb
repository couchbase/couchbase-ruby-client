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

require "test_helper"

require "couchbase/utils/observability_constants"
require "couchbase/tracing/threshold_logging_tracer"

module Couchbase
  class ThresholdLoggingTracerTest < Minitest::Test
    include TestUtilities

    def setup
      @tracer = Tracing::ThresholdLoggingTracer.new(sample_size: 3, emit_interval: 100_000)
    end

    def teardown
      @tracer.close
    end

    def test_add_operations_above_threshold
      items = [
        Tracing::ThresholdLoggingTracer::Item.new(
          total_duration_us: 600_000,
          encode_duration_us: 50_000,
          last_dispatch_duration_us: 200_000,
          total_dispatch_duration_us: 200_000,
          last_server_duration_us: 300_000,
          total_server_duration_us: 300_000,
          last_local_id: "local1",
          operation_name: "replace",
          operation_id: "op1",
          last_remote_socket: "remote-socket-1",
        ),
        Tracing::ThresholdLoggingTracer::Item.new(
          total_duration_us: 700_000,
          encode_duration_us: 50_000,
          last_dispatch_duration_us: 200_000,
          total_dispatch_duration_us: 200_000,
          last_server_duration_us: 300_000,
          total_server_duration_us: 300_000,
          last_local_id: "local1",
          operation_name: "insert",
          operation_id: "op2",
          last_remote_socket: "remote-socket-1",
        ),
        Tracing::ThresholdLoggingTracer::Item.new(
          total_duration_us: 510_000,
          encode_duration_us: 50_000,
          last_dispatch_duration_us: 200_000,
          total_dispatch_duration_us: 200_000,
          last_server_duration_us: 300_000,
          total_server_duration_us: 300_000,
          last_local_id: "local1",
          operation_name: "upsert",
          operation_id: "op3",
          last_remote_socket: "remote-socket-1",
        ),
      ]

      items.each { |item| @tracer.record_operation("kv", item) }
      report = @tracer.create_report

      assert_equal 1, report.size # Only KV operations in report
      assert report.key?("kv")

      kv_report = report["kv"]

      assert_equal 3, kv_report[:total_count]
      assert_equal 3, kv_report[:top_requests].size
      assert_equal(
        [
          {
            total_duration_us: 700_000,
            operation_name: "insert",
            operation_id: "op2",
          },
          {
            total_duration_us: 600_000,
            operation_name: "replace",
            operation_id: "op1",
          },
          {
            total_duration_us: 510_000,
            operation_name: "upsert",
            operation_id: "op3",
          },
        ],
        kv_report[:top_requests].map do |item|
          {
            total_duration_us: item[:total_duration_us],
            operation_name: item[:operation_name],
            operation_id: item[:operation_id],
          }
        end,
      )

      # When a report is generated the internal state of the tracer is reset
      assert_empty @tracer.create_report
    end

    def test_add_operations_below_threshold
      items = [
        Tracing::ThresholdLoggingTracer::Item.new(
          total_duration_us: 400_000,
          encode_duration_us: 50_000,
          last_dispatch_duration_us: 200_000,
          total_dispatch_duration_us: 200_000,
          last_server_duration_us: 300_000,
          total_server_duration_us: 300_000,
          last_local_id: "local1",
          operation_name: "replace",
          operation_id: "op1",
          last_remote_socket: "remote-socket-1",
        ),
        Tracing::ThresholdLoggingTracer::Item.new(
          total_duration_us: 420_000,
          encode_duration_us: 50_000,
          last_dispatch_duration_us: 200_000,
          total_dispatch_duration_us: 200_000,
          last_server_duration_us: 300_000,
          total_server_duration_us: 300_000,
          last_local_id: "local1",
          operation_name: "insert",
          operation_id: "op2",
          last_remote_socket: "remote-socket-1",
        ),
        Tracing::ThresholdLoggingTracer::Item.new(
          total_duration_us: 390_000,
          encode_duration_us: 50_000,
          last_dispatch_duration_us: 200_000,
          total_dispatch_duration_us: 200_000,
          last_server_duration_us: 300_000,
          total_server_duration_us: 300_000,
          last_local_id: "local1",
          operation_name: "upsert",
          operation_id: "op3",
          last_remote_socket: "remote-socket-1",
        ),
      ]

      items.each { |item| @tracer.record_operation("kv", item) }

      assert_empty @tracer.create_report
    end

    def test_exceeding_sample_size
      items = [
        Tracing::ThresholdLoggingTracer::Item.new(
          total_duration_us: 600_000,
          encode_duration_us: 50_000,
          last_dispatch_duration_us: 200_000,
          total_dispatch_duration_us: 200_000,
          last_server_duration_us: 300_000,
          total_server_duration_us: 300_000,
          last_local_id: "local1",
          operation_name: "replace",
          operation_id: "op1",
          last_remote_socket: "remote-socket-1",
        ),
        Tracing::ThresholdLoggingTracer::Item.new(
          total_duration_us: 700_000,
          encode_duration_us: 50_000,
          last_dispatch_duration_us: 200_000,
          total_dispatch_duration_us: 200_000,
          last_server_duration_us: 300_000,
          total_server_duration_us: 300_000,
          last_local_id: "local1",
          operation_name: "insert",
          operation_id: "op2",
          last_remote_socket: "remote-socket-1",
        ),
        Tracing::ThresholdLoggingTracer::Item.new(
          total_duration_us: 510_000,
          encode_duration_us: 50_000,
          last_dispatch_duration_us: 200_000,
          total_dispatch_duration_us: 200_000,
          last_server_duration_us: 300_000,
          total_server_duration_us: 300_000,
          last_local_id: "local1",
          operation_name: "upsert",
          operation_id: "op3",
          last_remote_socket: "remote-socket-1",
        ),
        Tracing::ThresholdLoggingTracer::Item.new(
          total_duration_us: 800_000,
          encode_duration_us: 50_000,
          last_dispatch_duration_us: 200_000,
          total_dispatch_duration_us: 200_000,
          last_server_duration_us: 300_000,
          total_server_duration_us: 300_000,
          last_local_id: "local1",
          operation_name: "insert",
          operation_id: "op4",
          last_remote_socket: "remote-socket-1",
        ),
        Tracing::ThresholdLoggingTracer::Item.new(
          total_duration_us: 530_000,
          encode_duration_us: 50_000,
          last_dispatch_duration_us: 200_000,
          total_dispatch_duration_us: 200_000,
          last_server_duration_us: 300_000,
          total_server_duration_us: 300_000,
          last_local_id: "local1",
          operation_name: "upsert",
          operation_id: "op5",
          last_remote_socket: "remote-socket-1",
        ),
      ]

      items.each { |item| @tracer.record_operation("kv", item) }
      report = @tracer.create_report

      assert_equal 1, report.size # Only KV operations in report
      assert report.key?("kv")

      kv_report = report["kv"]

      assert_equal 5, kv_report[:total_count]
      assert_equal 3, kv_report[:top_requests].size
      assert_equal(
        [
          {
            total_duration_us: 800_000,
            operation_name: "insert",
            operation_id: "op4",
          },
          {
            total_duration_us: 700_000,
            operation_name: "insert",
            operation_id: "op2",
          },
          {
            total_duration_us: 600_000,
            operation_name: "replace",
            operation_id: "op1",
          },
        ],
        kv_report[:top_requests].map do |item|
          {
            total_duration_us: item[:total_duration_us],
            operation_name: item[:operation_name],
            operation_id: item[:operation_id],
          }
        end,
      )

      # When a report is generated the internal state of the tracer is reset
      assert_empty @tracer.create_report
    end

    def test_add_operations_above_threshold_multiple_services
      kv_items = [
        Tracing::ThresholdLoggingTracer::Item.new(
          total_duration_us: 600_000,
          encode_duration_us: 50_000,
          last_dispatch_duration_us: 200_000,
          total_dispatch_duration_us: 200_000,
          last_server_duration_us: 300_000,
          total_server_duration_us: 300_000,
          last_local_id: "local1",
          operation_name: "replace",
          operation_id: "op1",
          last_remote_socket: "remote-socket-1",
        ),
        Tracing::ThresholdLoggingTracer::Item.new(
          total_duration_us: 700_000,
          encode_duration_us: 50_000,
          last_dispatch_duration_us: 200_000,
          total_dispatch_duration_us: 200_000,
          last_server_duration_us: 300_000,
          total_server_duration_us: 300_000,
          last_local_id: "local1",
          operation_name: "insert",
          operation_id: "op2",
          last_remote_socket: "remote-socket-1",
        ),

      ]
      query_items = [
        Tracing::ThresholdLoggingTracer::Item.new(
          total_duration_us: 510_000,
          encode_duration_us: nil,
          last_dispatch_duration_us: 200_000,
          total_dispatch_duration_us: 200_000,
          last_server_duration_us: nil,
          total_server_duration_us: nil,
          last_local_id: nil,
          operation_name: "query",
          operation_id: "op11",
          last_remote_socket: "remote-socket-1",
        ),
        Tracing::ThresholdLoggingTracer::Item.new(
          total_duration_us: 1_100_000,
          encode_duration_us: nil,
          last_dispatch_duration_us: 600_000,
          total_dispatch_duration_us: 600_000,
          last_server_duration_us: nil,
          total_server_duration_us: nil,
          last_local_id: "local1",
          operation_name: "query",
          operation_id: "op12",
          last_remote_socket: "remote-socket-1",
        ),
      ]

      kv_items.each { |item| @tracer.record_operation("kv", item) }
      query_items.each { |item| @tracer.record_operation("query", item) }
      report = @tracer.create_report

      assert_equal 2, report.size
      assert report.key?("kv")
      assert report.key?("query")

      kv_report = report["kv"]

      assert_equal 2, kv_report[:total_count]
      assert_equal 2, kv_report[:top_requests].size
      assert_equal(
        [
          {
            total_duration_us: 700_000,
            operation_name: "insert",
            operation_id: "op2",
          },
          {
            total_duration_us: 600_000,
            operation_name: "replace",
            operation_id: "op1",
          },
        ],
        kv_report[:top_requests].map do |item|
          {
            total_duration_us: item[:total_duration_us],
            operation_name: item[:operation_name],
            operation_id: item[:operation_id],
          }
        end,
      )

      query_report = report["query"]

      assert_equal 1, query_report[:total_count]
      assert_equal 1, query_report[:top_requests].size
      assert_equal(
        [
          {
            total_duration_us: 1_100_000,
            operation_name: "query",
            operation_id: "op12",
          },
        ],
        query_report[:top_requests].map do |item|
          {
            total_duration_us: item[:total_duration_us],
            operation_name: item[:operation_name],
            operation_id: item[:operation_id],
          }
        end,
      )

      # When a report is generated the internal state of the tracer is reset
      assert_empty @tracer.create_report
    end

    def test_span_converted_to_threshold_logger_item
      op_span_start_time = Time.now
      op_span = @tracer.request_span("replace", start_timestamp: op_span_start_time)
      op_span.set_attribute(Couchbase::Observability::ATTR_SERVICE, "kv")
      op_span.set_attribute(Couchbase::Observability::ATTR_OPERATION_NAME, "get")

      request_encoding_start_time = op_span_start_time + 0.1
      encoding_span = @tracer.request_span(Couchbase::Observability::STEP_REQUEST_ENCODING, parent: op_span,
                                                                                            start_timestamp: request_encoding_start_time)
      encoding_span.finish(end_timestamp: request_encoding_start_time + 0.1) # 0.1s duration

      dispatch_start_time = request_encoding_start_time + 0.2
      dispatch_span = @tracer.request_span(Couchbase::Observability::STEP_DISPATCH_TO_SERVER, parent: op_span,
                                                                                              start_timestamp: dispatch_start_time)
      dispatch_span.set_attribute(Couchbase::Observability::ATTR_LOCAL_ID, "local1")
      dispatch_span.set_attribute(Couchbase::Observability::ATTR_OPERATION_ID, "op1")
      dispatch_span.set_attribute(Couchbase::Observability::ATTR_PEER_ADDRESS, "1.2.3.4")
      dispatch_span.set_attribute(Couchbase::Observability::ATTR_PEER_PORT, 11210)
      dispatch_span.set_attribute(Couchbase::Observability::ATTR_SERVER_DURATION, 200_000) # In microseconds
      dispatch_span.finish(end_timestamp: dispatch_start_time + 0.4) # 0.4s duration

      op_span.finish(end_timestamp: op_span_start_time + 1) # 1s duration

      assert_equal(
        {
          "kv" => {
            total_count: 1,
            top_requests: [
              {
                total_duration_us: 1_000_000,
                encode_duration_us: 100_000,
                last_dispatch_duration_us: 400_000,
                total_dispatch_duration_us: 400_000,
                last_server_duration_us: 200_000,
                total_server_duration_us: 200_000,
                last_local_id: "local1",
                operation_name: "replace",
                operation_id: "op1",
                last_remote_socket: "1.2.3.4:11210",
              },
            ],
          },
        },
        @tracer.create_report,
      )
    end
  end
end
