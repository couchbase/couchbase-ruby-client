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

require "couchbase/metrics/value_recorder"

module Couchbase
  module Metrics
    class OpenTelemetryValueRecorder < ValueRecorder
      def initialize(recorder, tags, unit: nil)
        super()
        @wrapped = recorder
        @tags = tags
        @unit = unit
      end

      def record_value(value)
        value =
          case @unit
          when "s"
            value / 1_000_000.0
          else
            value
          end

        @wrapped.record(value, attributes: @tags)
      end
    end
  end
end
