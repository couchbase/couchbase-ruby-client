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

module Couchbase
  module Utils
    class HdrHistogram
      def initialize(
        lowest_discernible_value:,
        highest_trackable_value:,
        significant_figures:,
        percentiles: nil
      )
        @histogram_backend = HdrHistogramC.new(lowest_discernible_value, highest_trackable_value, significant_figures)
        @percentiles = percentiles || [50.0, 90.0, 99.0, 99.9, 100.0]
      end

      def record_value(value)
        @histogram_backend.record_value(value)
      end

      def close
        @histogram_backend.close
      end

      def report_and_reset
        backend_report = @histogram_backend.get_percentiles_and_reset(@percentiles)
        total_count = backend_report[:total_count]

        return nil if total_count.zero?

        report = {
          total_count: total_count,
          percentiles_us: {},
        }
        @percentiles.zip(backend_report[:percentiles]).each do |percentile, percentile_value|
          report[:percentiles_us][percentile.to_s] = percentile_value
        end
        report
      end
    end
  end
end
