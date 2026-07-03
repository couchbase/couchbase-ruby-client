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

require "opentelemetry-exporter-otlp"
require "opentelemetry-exporter-otlp-metrics"
require "opentelemetry-metrics-sdk"
require "opentelemetry-sdk"

module FIT
  module Performer
    module Observability
      module Otel
        SAMPLING_PERCENTAGE_EPSILON = 0.00001
        TRACING_PATH = "/v1/traces"
        METRICS_PATH = "/v1/metrics"

        def self.create_resource(proto_resources)
          OpenTelemetry::SDK::Resources::Resource.create(
            proto_resources.map do |k, v| # rubocop:disable Style/MapToHash -- Google::Protobuf::Map does not implement the full Hash API
              [k, v.public_send(v.value)]
            end.to_h,
          )
        end

        def self.create_tracer_provider(config)
          exporter = OpenTelemetry::Exporter::OTLP::Exporter.new(
            endpoint: URI.join(config.endpoint_hostname, TRACING_PATH).to_s,
            ssl_verify_mode: OpenSSL::SSL::VERIFY_NONE,
          )

          span_processor =
            if config.batching
              OpenTelemetry::SDK::Trace::Export::BatchSpanProcessor.new(
                exporter,
                schedule_delay: config.export_every_millis,
              )
            else
              OpenTelemetry::SDK::Trace::Export::SimpleSpanProcessor.new(exporter)
            end

          sampler =
            if config.sampling_percentage < SAMPLING_PERCENTAGE_EPSILON
              OpenTelemetry::SDK::Trace::Samplers::ALWAYS_OFF
            elsif config.sampling_percentage > 1.0 - SAMPLING_PERCENTAGE_EPSILON
              OpenTelemetry::SDK::Trace::Samplers::ALWAYS_ON
            else
              OpenTelemetry::SDK::Trace::Samplers::TraceIdRatioBased.new(config.sampling_percentage)
            end

          provider = OpenTelemetry::SDK::Trace::TracerProvider.new(
            sampler: sampler,
            resource: create_resource(config.resources),
          )
          provider.add_span_processor(span_processor)
          provider
        end

        def self.create_meter_provider(config)
          exporter = OpenTelemetry::Exporter::OTLP::Metrics::MetricsExporter.new(
            endpoint: URI.join(config.endpoint_hostname, METRICS_PATH).to_s,
            ssl_verify_mode: OpenSSL::SSL::VERIFY_NONE,
          )

          reader = OpenTelemetry::SDK::Metrics::Export::PeriodicMetricReader.new(
            export_interval_millis: config.export_every_millis,
            exporter: exporter,
          )

          provider = OpenTelemetry::SDK::Metrics::MeterProvider.new(resource: create_resource(config.resources))
          provider.add_metric_reader(reader)
          provider
        end
      end
    end
  end
end
