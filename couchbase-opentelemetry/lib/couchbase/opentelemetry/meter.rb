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

require "couchbase/metrics/meter"
require "couchbase/errors"
require_relative "value_recorder"

require "opentelemetry-metrics-api"

module Couchbase
  module OpenTelemetry
    # @couchbase.stability
    #  **Uncommitted:** This API may change in the future, as Metrics in OpenTelemetry Ruby are currently in development.
    #
    #  See the {https://opentelemetry.io/docs/languages/ruby/#status-and-releases OpenTelemetry Ruby documentation} for more information.
    class Meter < ::Couchbase::Metrics::Meter
      # Initializes a Couchbase OpenTelemetry Meter
      #
      # @param [::OpenTelemetry::Metrics::MeterProvider] meter_provider The OpenTelemetry meter provider
      #
      # @raise [Couchbase::Error::MeterError] if the meter cannot be created for any reason
      #
      # @example Initializing a Couchbase OpenTelemetry Meter with an OTLP Exporter
      #   require "opentelemetry-metrics-sdk"
      #
      #   # Initialize a meter provider
      #   meter_provider = ::OpenTelemetry::SDK::Metrics::MeterProvider.new
      #   meter_provider.add_metric_reader(
      #       ::OpenTelemetry::SDK::Metrics::Export::PeriodicMetricReader.new(
      #           exporter: ::OpenTelemetry::Exporter::OTLP::Metrics::MetricsExporter.new(
      #               endpoint: "https://<hostname>:<port>/v1/metrics"
      #          )
      #       )
      #   )
      #   # Initialize the Couchbase OpenTelemetry Meter
      #   meter = Couchbase::OpenTelemetry::Meter.new(meter_provider)
      #
      #   # Set the meter in the cluster options
      #   options = Couchbase::Options::Cluster.new(
      #      authenticator: Couchbase::PasswordAuthenticator.new("Administrator", "password")
      #      meter: meter
      #   )
      #
      # @see https://www.rubydoc.info/gems/opentelemetry-metrics-sdk/OpenTelemetry/SDK/Metrics/MeterProvider
      #  <tt>opentelemetry-metrics-sdk</tt> API Reference
      def initialize(meter_provider)
        super()
        @histogram_cache = Concurrent::Map.new
        begin
          @wrapped = meter_provider.meter("com.couchbase.client/ruby")
        rescue StandardError => e
          raise Error::MeterError.new("Failed to create OpenTelemetry Meter: #{e.message}", nil, e)
        end
      end

      def value_recorder(name, tags)
        unit = tags.delete("__unit")

        otel_histogram = @histogram_cache.compute_if_absent(name) do
          @wrapped.create_histogram(name, unit: unit)
        end

        ValueRecorder.new(otel_histogram, tags, unit: unit)
      rescue StandardError => e
        raise Error::MeterError.new("Failed to create OpenTelemetry Histogram: #{e.message}", nil, e)
      end
    end
  end
end
