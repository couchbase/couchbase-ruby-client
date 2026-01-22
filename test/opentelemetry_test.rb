# frozen_string_literal: true

#  Copyright 2026-Present Couchbase, Inc.
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

require_relative "test_helper"

require "couchbase/tracing/open_telemetry_request_tracer"
require "couchbase/metrics/open_telemetry_meter"

require "opentelemetry-sdk"
require "opentelemetry-metrics-sdk"

module Couchbase
  class OpenTelemetryTest < Minitest::Test
    include TestUtilities

    def setup
      @span_exporter = ::OpenTelemetry::SDK::Trace::Export::InMemorySpanExporter.new
      @tracer_provider =
        begin
          tracer_provider = ::OpenTelemetry::SDK::Trace::TracerProvider.new
          tracer_provider.add_span_processor(
            ::OpenTelemetry::SDK::Trace::Export::SimpleSpanProcessor.new(@span_exporter),
          )
          tracer_provider
        end
      @tracer = Couchbase::Tracing::OpenTelemetryRequestTracer.new(@tracer_provider)

      @metric_exporter = ::OpenTelemetry::SDK::Metrics::Export::InMemoryMetricPullExporter.new
      @metric_reader = ::OpenTelemetry::SDK::Metrics::Export::PeriodicMetricReader.new(exporter: @metric_exporter)
      @meter_provider =
        begin
          meter_provider = ::OpenTelemetry::SDK::Metrics::MeterProvider.new
          meter_provider.add_metric_reader(@metric_reader)
          meter_provider
        end
      @meter = Couchbase::Metrics::OpenTelemetryMeter.new(@meter_provider)

      connect(Options::Cluster.new(tracer: @tracer, meter: @meter))
      @bucket = @cluster.bucket(env.bucket)
      @collection = @bucket.default_collection
      @parent_span = @tracer.request_span("parent_span")
    end

    def teardown
      disconnect
      @tracer_provider.shutdown
      @meter_provider.shutdown
    end

    def assert_otel_span(
      span_data,
      name,
      attributes: {},
      parent_span_id: nil,
      status_code: OpenTelemetry::Trace::Status::UNSET
    )
      assert_equal name, span_data.name
      assert_equal :client, span_data.kind
      assert_equal status_code, span_data.status.code

      if parent_span_id.nil?
        assert_predicate span_data.parent_span_id.hex, :zero?
      else
        assert_equal parent_span_id, span_data.parent_span_id
      end

      attributes.each do |key, value|
        if value.nil?
          assert span_data.attributes.key?(key), "Expected attribute #{key} to be present"
        else
          assert_equal value, span_data.attributes[key], "Expected attribute #{key} to have value #{value}"
        end
      end
    end

    def test_opentelemetry_tracer
      res = @collection.upsert(uniq_id(:otel_test), {foo: "bar"}, Options::Upsert.new(parent_span: @parent_span))

      assert_predicate res.cas, :positive?

      @parent_span.finish
      spans = @span_exporter.finished_spans.sort_by(&:start_timestamp)

      assert_otel_span(
        spans[0],
        "parent_span",
        attributes: {},
        parent_span_id: nil,
      )

      assert_otel_span(
        spans[1],
        "upsert",
        attributes: {
          "db.system.name" => "couchbase",
          "couchbase.cluster.name" => env.cluster_name,
          "couchbase.cluster.uuid" => env.cluster_uuid,
          "db.operation.name" => "upsert",
          "db.namespace" => @bucket.name,
          "couchbase.scope.name" => "_default",
          "couchbase.collection.name" => "_default",
          "couchbase.retries" => nil,
        },
        parent_span_id: spans[0].span_id,
        status_code: OpenTelemetry::Trace::Status::OK,
      )

      assert_otel_span(
        spans[2],
        "request_encoding",
        attributes: {
          "db.system.name" => "couchbase",
          "couchbase.cluster.name" => env.cluster_name,
          "couchbase.cluster.uuid" => env.cluster_uuid,
        },
        parent_span_id: spans[1].span_id,
      )

      assert_otel_span(
        spans[3],
        "dispatch_to_server",
        attributes: {
          "db.system.name" => "couchbase",
          "couchbase.cluster.name" => env.cluster_name,
          "couchbase.cluster.uuid" => env.cluster_uuid,
          "network.peer.address" => nil,
          "network.peer.port" => nil,
          "network.transport" => "tcp",
          "server.address" => nil,
          "server.port" => nil,
          "couchbase.local_id" => nil,
        },
        parent_span_id: spans[1].span_id,
      )
    end

    def test_opentelemetry_meter
      assert_raises(Error::DocumentNotFound) do
        @collection.get(uniq_id(:does_not_exist))
      end

      @collection.insert(uniq_id(:otel_test), {foo: "bar"})

      @metric_reader.force_flush
      snapshots = @metric_exporter.metric_snapshots

      assert_equal 1, snapshots.size

      snapshot = snapshots[0]

      assert_equal "db.client.operation.duration", snapshot.name
      assert_equal "s", snapshot.unit
      assert_equal :histogram, snapshot.instrument_kind
      assert_equal 2, snapshot.data_points.size

      snapshot.data_points.each_with_index do |p, idx|
        assert_equal "couchbase", p.attributes["db.system.name"]
        assert_equal env.cluster_name, p.attributes["couchbase.cluster.name"]
        assert_equal env.cluster_uuid, p.attributes["couchbase.cluster.uuid"]
        assert_equal env.bucket, p.attributes["db.namespace"]
        assert_equal "_default", p.attributes["couchbase.scope.name"]
        assert_equal "_default", p.attributes["couchbase.collection.name"]
        assert_equal "kv", p.attributes["couchbase.service"]

        case idx
        when 0
          assert_equal "get", p.attributes["db.operation.name"]
          assert_equal "DocumentNotFound", p.attributes["error.type"]
        when 1
          assert_equal "insert", p.attributes["db.operation.name"]
          assert_nil p.attributes["error.type"]
        end
      end
    end
  end
end
