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

require_relative "test_helper"

require "couchbase/utils/hdr_histogram"

module Couchbase
  # Regression tests for RCBC-569. A histogram left locked aborts the process or blocks every Ruby
  # thread, so each case runs in a forked child under a watchdog.
  class HdrHistogramTest < Minitest::Test
    CHILD_TIMEOUT = 10

    def test_integer_percentile_reports_same_as_float
      in_child do
        with_integer = new_histogram(percentiles: [50, 99.0])
        with_float = new_histogram(percentiles: [50.0, 99.0])
        (1..100).each do |value|
          with_integer.record_value(value)
          with_float.record_value(value)
        end

        assert_equal with_float.report_and_reset[:percentiles_us].values, with_integer.report_and_reset[:percentiles_us].values

        with_integer.record_value(5)

        assert_equal 1, with_integer.report_and_reset[:total_count]
        with_integer.close
      end
    end

    def test_non_numeric_percentile_leaves_histogram_usable_on_same_thread
      in_child do
        histogram = histogram_after_type_error

        histogram.record_value(5)

        assert_equal 2, histogram.get_percentiles_and_reset([50.0])[:total_count]
        histogram.close
      end
    end

    def test_non_numeric_percentile_leaves_histogram_usable_from_another_thread
      in_child do
        histogram = histogram_after_type_error
        total_count = Thread.new do # rubocop:disable ThreadSafety/NewThread
          histogram.record_value(5)
          count = histogram.get_percentiles_and_reset([50.0])[:total_count]
          histogram.close
          count
        end.value

        assert_equal 2, total_count
      end
    end

    def test_default_percentiles
      in_child do
        histogram = new_histogram
        (1..100).each { |value| histogram.record_value(value) }

        assert_equal({total_count: 100, percentiles_us: {"50.0" => 50, "90.0" => 90, "99.0" => 99, "99.9" => 100, "100.0" => 100}},
                     histogram.report_and_reset)
        histogram.close
      end
    end

    private

    def new_histogram(percentiles: nil)
      Utils::HdrHistogram.new(lowest_discernible_value: 1, highest_trackable_value: 30_000_000, significant_figures: 3,
                              percentiles: percentiles)
    end

    # Returns a backend histogram holding one value, after a call with a String percentile.
    def histogram_after_type_error
      histogram = Utils::HdrHistogramC.new(1, 30_000_000, 3)
      histogram.record_value(5)
      assert_raises(TypeError) { histogram.get_percentiles_and_reset([50.0, "99.0"]) }
      histogram
    end

    def in_child
      skip("Forking not supported on Windows") if Gem.win_platform?
      skip("Forking not supported") unless Process.respond_to?(:fork)

      pid = Process.fork do
        yield
        exit!(0)
      rescue Exception => e # rubocop:disable Lint/RescueException
        warn(e.full_message)
        exit!(1)
      end
      status = wait_for_child_or_kill(pid, timeout: CHILD_TIMEOUT)

      assert_predicate status, :success?, "Child process failed: #{status.inspect}"
    end

    # Polls for +pid+ to exit, killing it and failing the test if it doesn't within +timeout+ seconds.
    def wait_for_child_or_kill(pid, timeout:)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout

      loop do
        _, status = Process.waitpid2(pid, Process::WNOHANG)
        return status if status

        if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
          Process.kill("KILL", pid)
          Process.waitpid2(pid)

          flunk "Child process (pid #{pid}) did not exit within #{timeout}s."
        end

        sleep 0.1
      end
    end
  end
end
