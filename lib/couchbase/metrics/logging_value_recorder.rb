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

require_relative "value_recorder"
require "couchbase/utils/hdr_histogram"

module Couchbase
  module Metrics
    class LoggingValueRecorder < ValueRecorder
      attr_reader :operation_name
      attr_reader :service

      def initialize(operation_name:, service:)
        super()
        @operation_name = operation_name
        @service = service
        @histogram = Utils::HdrHistogram.new(
          lowest_discernible_value: 1, # 1 microsecond
          highest_trackable_value: 30_000_000, # 30 seconds
          significant_figures: 3,
        )
      end

      def record_value(value)
        @histogram.record_value(value)
      end

      def report_and_reset
        @histogram.report_and_reset
      end

      def close
        @histogram.close
      end
    end
  end
end
