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
require_relative "open_telemetry_value_recorder"

require "opentelemetry-metrics-api"

module Couchbase
  module Metrics
    class OpenTelemetryMeter < Meter
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

        OpenTelemetryValueRecorder.new(otel_histogram, tags, unit: unit)
      rescue StandardError => e
        raise Error::MeterError.new("Failed to create OpenTelemetry Histogram: #{e.message}", nil, e)
      end
    end
  end
end
