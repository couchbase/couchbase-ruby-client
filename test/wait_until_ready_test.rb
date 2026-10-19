# frozen_string_literal: true

#  Copyright 2026. Couchbase, Inc.
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

module Couchbase
  class WaitUntilReadyTest < Minitest::Test
    include Couchbase::TestUtilities

    TIMEOUT = 10_000
    # Services a wait can name, other than management, which every cluster runs.
    WAITABLE_SERVICES = [:kv, :query, :analytics, :search, :views, :eventing].freeze

    def setup
      skip("#{name}: The #{Couchbase::Protostellar::NAME} protocol does not support wait_until_ready") if env.protostellar?

      connect
    end

    def teardown
      disconnect
    end

    def test_cluster_wait_until_ready
      assert_nil @cluster.wait_until_ready(TIMEOUT)
    end

    def test_bucket_wait_until_ready
      assert_nil @cluster.bucket(env.bucket).wait_until_ready(TIMEOUT)
    end

    def test_bucket_wait_until_ready_for_kv
      assert_nil @cluster.bucket(env.bucket).wait_until_ready(TIMEOUT, Options::WaitUntilReady.new(service_types: [:kv]))
    end

    def test_cluster_wait_until_degraded
      assert_nil @cluster.wait_until_ready(TIMEOUT, Options::WaitUntilReady.new(desired_state: :degraded))
    end

    def test_zero_timeout
      assert_raises(Error::UnambiguousTimeout) { @cluster.wait_until_ready(0) }
    end

    def test_closed_cluster
      cluster = @cluster
      bucket = cluster.bucket(env.bucket)
      disconnect

      assert_raises(Error::ClusterClosed) { cluster.wait_until_ready(TIMEOUT) }
      assert_raises(Error::ClusterClosed) { bucket.wait_until_ready(TIMEOUT) }
    end

    def test_invalid_desired_state
      assert_raises(Error::InvalidArgument) do
        @cluster.wait_until_ready(TIMEOUT, Options::WaitUntilReady.new(desired_state: :offline))
      end
    end

    def test_invalid_service_type
      assert_raises(Error::InvalidArgument) do
        @cluster.wait_until_ready(TIMEOUT, Options::WaitUntilReady.new(service_types: [:kv, :unknown]))
      end
    end

    # A negative timeout, or one that overflows the deadline, ended the wait at once with UnambiguousTimeout.
    def test_out_of_range_timeout
      [-1, -(2**64), 2**62, 2**63, 2**64, 2**128].each do |timeout|
        error = assert_raises(Error::InvalidArgument) { @cluster.wait_until_ready(timeout) }
        assert_match(/\Atimeout must be between 0 and \d+ milliseconds, but given #{timeout}\z/, error.message)
      end
    end

    # The largest accepted timeout waits and completes; one above it is rejected. Timeout.timeout cannot interrupt the
    # native wait, so a thread bounds it.
    def test_timeout_at_upper_bound
      error = assert_raises(Error::InvalidArgument) { @cluster.wait_until_ready(-1) }
      max_timeout = Integer(error.message[/between 0 and (\d+) milliseconds/, 1])

      assert_equal 365 * 24 * 60 * 60 * 1000, max_timeout
      assert_raises(Error::InvalidArgument) { @cluster.wait_until_ready(max_timeout + 1) }

      options = Options::WaitUntilReady.new(service_types: [:kv])
      waiter = Thread.new do # rubocop:disable ThreadSafety/NewThread
        Thread.current.report_on_exception = false
        @cluster.wait_until_ready(max_timeout, options)
      end

      assert waiter.join(TIMEOUT / 1000.0), "a wait with a #{max_timeout} ms timeout did not complete"
    ensure
      if waiter&.alive?
        disconnect
        begin
          waiter.join
        rescue Error::ClusterClosed
          nil
        end
      end
    end

    def test_nil_timeout
      error = assert_raises(ArgumentError) { @cluster.wait_until_ready(nil) }
      assert_equal "timeout must be an Integer, but given nil", error.message
    end

    def test_service_type_whose_inspect_raises
      entry = Object.new
      def entry.inspect = raise(ArgumentError, "inspect failed")

      error = assert_raises(ArgumentError) do
        @cluster.wait_until_ready(TIMEOUT, Options::WaitUntilReady.new(service_types: [:kv, entry]))
      end
      assert_equal "inspect failed", error.message
      @cluster.wait_until_ready(TIMEOUT)
    end

    def test_positive_timeout_expires
      options = Options::WaitUntilReady.new(service_types: [missing_service])
      [@cluster, @cluster.bucket(env.bucket)].each do |target|
        started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        assert_raises(Error::UnambiguousTimeout) { target.wait_until_ready(500, options) }
        assert_operator Process.clock_gettime(Process::CLOCK_MONOTONIC) - started, :>=, 0.4
      end
    end

    def test_disconnect_ends_a_pending_cluster_wait
      assert_disconnect_ends_wait(@cluster)
    end

    def test_disconnect_ends_a_pending_bucket_wait
      assert_disconnect_ends_wait(@cluster.bucket(env.bucket))
    end

    private

    # A service the cluster does not run, so a wait for it can end only by timeout or disconnect.
    def missing_service
      running = @cluster.ping(Options::Ping.new(service_types: WAITABLE_SERVICES)).services.keys
      WAITABLE_SERVICES.find { |service| !running.include?(service) } || skip("#{name}: the cluster runs every service")
    end

    # A wait already in flight fails in the core with "unable to wait until ready". A call made after
    # disconnect fails in the binding with "Cluster has been closed already", so the message shows the
    # disconnect landed during the wait.
    def assert_disconnect_ends_wait(target)
      options = Options::WaitUntilReady.new(service_types: [missing_service])
      waiter = Thread.new do # rubocop:disable ThreadSafety/NewThread
        Thread.current.report_on_exception = false
        target.wait_until_ready(TIMEOUT, options)
      end
      # The waiter can sleep only inside the native wait, which blocks with the GVL released.
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 5
      sleep(0.01) until waiter.status == "sleep" || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

      assert_equal "sleep", waiter.status, "the native wait did not start"
      disconnect

      error = assert_raises(Error::ClusterClosed) { waiter.value }
      assert_match(/unable to wait until ready/, error.message)
    end
  end

  class WaitUntilReadyCouchbase2Test < Minitest::Test
    include Couchbase::TestUtilities

    def setup
      skip("#{name}: requires the #{Couchbase::Protostellar::NAME} protocol") unless env.protostellar?

      connect
    end

    def teardown
      disconnect
    end

    def test_not_supported
      assert_raises(Error::FeatureNotAvailable) { @cluster.wait_until_ready(WaitUntilReadyTest::TIMEOUT) }
      assert_raises(Error::FeatureNotAvailable) { @cluster.bucket(env.bucket).wait_until_ready(WaitUntilReadyTest::TIMEOUT) }
    end
  end
end
