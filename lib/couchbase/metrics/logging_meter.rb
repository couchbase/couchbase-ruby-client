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

require_relative "logging_value_recorder"
require_relative "meter"
require_relative "noop_meter"
require "couchbase/utils/observability_constants"
require "couchbase/utils/stdlib_logger_adapter"
require "couchbase/logger"

require "concurrent/map"
require "concurrent/timer_task"

module Couchbase
  module Metrics
    class LoggingMeter < Meter
      # @api private
      DEFAULT_EMIT_INTERVAL = 600_000 # milliseconds

      def initialize(emit_interval: nil)
        super()
        @emit_interval = emit_interval || DEFAULT_EMIT_INTERVAL
        @value_recorders = {
          Observability::ATTR_VALUE_SERVICE_KV => Concurrent::Map.new,
          Observability::ATTR_VALUE_SERVICE_QUERY => Concurrent::Map.new,
          Observability::ATTR_VALUE_SERVICE_VIEWS => Concurrent::Map.new,
          Observability::ATTR_VALUE_SERVICE_SEARCH => Concurrent::Map.new,
          Observability::ATTR_VALUE_SERVICE_ANALYTICS => Concurrent::Map.new,
          Observability::ATTR_VALUE_SERVICE_MANAGEMENT => Concurrent::Map.new,
        }

        # TODO(DC): Find better solution for logging
        @logger = Couchbase.logger || Logger.new($stdout, Utils::StdlibLoggerAdapter.map_spdlog_level(Couchbase.log_level))
        @task = Concurrent::TimerTask.new(execution_interval: @emit_interval / 1_000.0) do
          report = create_report
          return if report.empty?

          @logger.info("Metrics: #{report.to_json}")
        rescue StandardError => e
          @logger.debug("Failed to log metrics: #{e.message}")
        end
      end

      def value_recorder(name, tags)
        return NoopMeter::VALUE_RECORDER_INSTANCE unless name == Observability::METER_NAME_OPERATION_DURATION

        operation_name = tags[Observability::ATTR_OPERATION_NAME]
        service = tags[Observability::ATTR_SERVICE]

        return NoopMeter::VALUE_RECORDER_INSTANCE if operation_name.nil? || service.nil?

        @value_recorders[service].put_if_absent(
          operation_name,
          LoggingValueRecorder.new(
            operation_name: operation_name,
            service: service,
          ),
        )

        @value_recorders[service][operation_name]
      end

      def create_report
        operations = {}
        @value_recorders.each do |service, recorders|
          recorders.each_key do |operation_name|
            recorder = recorders[operation_name]
            operation_report = recorder.report_and_reset
            if operation_report
              operations[service] ||= {}
              operations[service][operation_name] = operation_report
            end
          end
        end
        if operations.empty?
          {}
        else
          {
            meta: {
              emit_interval_ms: @emit_interval,
            },
            operations: operations,
          }
        end
      end

      def close
        @task.shutdown
        @value_recorders.each_value do |operation_map|
          operation_map.each_value(&:close)
        end
      end
    end
  end
end
