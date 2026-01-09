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

require "couchbase/metrics/logging_meter"

module Couchbase
  class LoggingMeterTest < Minitest::Test
    include TestUtilities

    def setup
      @meter = Metrics::LoggingMeter.new
    end

    def teardown
      @meter.close
    end

    def test_record_values
      get_recorder = @meter.value_recorder("db.client.operation.duration", {
        "couchbase.service" => "kv",
        "db.operation.name" => "get",
      })

      assert_instance_of Metrics::LoggingValueRecorder, get_recorder

      get_recorder.record_value(50)
      get_recorder.record_value(75)
      get_recorder.record_value(100)

      replace_recorder = @meter.value_recorder("db.client.operation.duration", {
        "couchbase.service" => "kv",
        "db.operation.name" => "replace",
      })

      assert_instance_of Metrics::LoggingValueRecorder, replace_recorder

      replace_recorder.record_value(150)

      query_recorder = @meter.value_recorder("db.client.operation.duration", {
        "couchbase.service" => "query",
        "db.operation.name" => "query",
      })

      assert_instance_of Metrics::LoggingValueRecorder, query_recorder

      query_recorder.record_value(400)
      query_recorder.record_value(500)
      query_recorder.record_value(600)

      report = @meter.create_report

      assert_equal 600000, report[:meta][:emit_interval_ms]
      assert_equal 2, report[:operations].size

      get_report = report[:operations]["kv"]["get"]

      assert_equal 3, get_report[:total_count]
      assert_equal(
        {
          "50.0" => 75,
          "90.0" => 100,
          "99.0" => 100,
          "99.9" => 100,
          "100.0" => 100,
        },
        get_report[:percentiles_us],
      )

      replace_report = report[:operations]["kv"]["replace"]

      assert_equal 1, replace_report[:total_count]
      assert_equal(
        {
          "50.0" => 150,
          "90.0" => 150,
          "99.0" => 150,
          "99.9" => 150,
          "100.0" => 150,
        },
        replace_report[:percentiles_us],
      )

      query_report = report[:operations]["query"]["query"]

      assert_equal 3, query_report[:total_count]
      assert_equal(
        {
          "50.0" => 500,
          "90.0" => 600,
          "99.0" => 600,
          "99.9" => 600,
          "100.0" => 600,
        },
        query_report[:percentiles_us],
      )

      # Check that after reporting, the metrics are reset
      assert_empty @meter.create_report
    end

    def test_unrecognized_meters_are_ignored
      rec = @meter.value_recorder("unknown.meter", {
        "couchbase.service" => "kv",
        "db.operation.name" => "get",
      })

      assert_instance_of Metrics::NoopValueRecorder, rec

      assert_empty @meter.create_report
    end

    def test_metrics_with_missing_service_are_ignored
      rec = @meter.value_recorder("db.client.operation.duration", {
        "db.operation.name" => "get",
      })

      assert_instance_of Metrics::NoopValueRecorder, rec

      assert_empty @meter.create_report
    end

    def test_metrics_with_missing_operation_name_are_ignored
      rec = @meter.value_recorder("db.client.operation.duration", {
        "couchbase.service" => "kv",
      })

      assert_instance_of Metrics::NoopValueRecorder, rec

      assert_empty @meter.create_report
    end
  end
end
