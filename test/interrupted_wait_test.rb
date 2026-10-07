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

require_relative "test_helper"

require "etc"
require "timeout"
require "couchbase/raw_binary_transcoder"

module Couchbase
  # RCBC-559: an interrupt delivered at the blocking wait must not leave the response
  # unreachable. Each such response holds one document body, so RSS over many
  # interrupted gets of a large document shows the leak.
  #
  # Thread.handle_interrupt(Object => :on_blocking) defers a pending interrupt to the
  # next blocking point, which for a get is the wait. The before-wait test instead
  # raises its interrupt at the check that precedes the wait.
  class InterruptedWaitTest < Minitest::Test
    include TestUtilities

    class Interrupted < StandardError; end

    DOCUMENT_SIZE = 4 * 1024 * 1024
    LANDED_TARGET = 100
    MAX_ITERATIONS = 1000
    WARMUP_ITERATIONS = 10

    def setup
      skip("#{name}: #{Couchbase::Protostellar::NAME} does not use cb_wait_for_future") if env.protostellar?
      skip("#{name}: RSS is read from /proc/self/statm") unless File.readable?("/proc/self/statm")
      connect
      @collection = @cluster.bucket(env.bucket).default_collection
      @transcoder = RawBinaryTranscoder.new
      @doc_id = uniq_id(:interrupted_wait)
      @collection.upsert(@doc_id, "x" * DOCUMENT_SIZE, Options::Upsert.new(transcoder: @transcoder))
      @get_options = Options::Get.new(transcoder: @transcoder)
    end

    def teardown
      @collection&.remove(@doc_id)
      disconnect
    end

    def test_get_interrupted_by_timeout_does_not_leak_response
      assert_no_leak do
        completed = false
        Timeout.timeout(0.001) do
          Thread.handle_interrupt(Object => :on_blocking) do
            Thread.pass until Thread.pending_interrupt?
            @collection.get(@doc_id, @get_options)
            completed = true
          end
        end
        !completed
      rescue Timeout::Error
        !completed
      end
    end

    def test_get_interrupted_by_thread_kill_does_not_leak_response
      assert_no_leak do
        completed = false
        started = Queue.new
        thread = Thread.new do # rubocop:disable ThreadSafety/NewThread
          Thread.handle_interrupt(Object => :on_blocking) do
            started << true
            Thread.pass until Thread.pending_interrupt?
            @collection.get(@doc_id, @get_options)
            completed = true
          end
        end
        started.pop
        thread.kill
        thread.join
        !completed
      end
    end

    # An interrupt already pending when the wait begins leaves the operation in flight;
    # its response must still be destroyed. Only a direct backend call reaches the wait
    # with the interrupt still pending.
    def test_get_interrupted_before_wait_does_not_leak_response
      backend = @cluster.instance_variable_get(:@backend)
      observability = @collection.instance_variable_get(:@observability)
      args = [env.bucket, "_default", "_default", @doc_id, @get_options.to_backend]
      assert_no_leak do
        completed = false
        Thread.handle_interrupt(Object => :never) do
          current = Thread.current
          Thread.new { current.raise(Interrupted) }.join # rubocop:disable ThreadSafety/NewThread
          observability.record_operation(Observability::OP_GET, nil, @collection, :kv) do |handler|
            # :immediate would raise on entering the block, before the backend call.
            Thread.handle_interrupt(Object => :on_blocking) do
              backend.document_get(*args, handler)
              completed = true
            end
          end
        end
        !completed
      rescue Interrupted
        !completed
      end
    end

    private

    # Runs the block, which returns whether its interrupt landed, until LANDED_TARGET
    # interrupts have landed, and bounds RSS growth by a quarter of a document per landing.
    def assert_no_leak(&)
      WARMUP_ITERATIONS.times(&)
      GC.start
      before = rss_bytes
      landed = 0
      iterations = 0
      while landed < LANDED_TARGET && iterations < MAX_ITERATIONS
        landed += 1 if yield
        iterations += 1
      end
      skip("#{name}: #{landed} of #{iterations} interrupts landed in the wait") if landed < LANDED_TARGET
      # Responses on one connection arrive in order, so this returns after every earlier get of
      # the document, including one whose wait was abandoned, has received its response.
      @collection.get(@doc_id, @get_options)
      GC.start
      growth = rss_bytes - before

      assert_operator growth, :<, landed * DOCUMENT_SIZE / 4, "RSS grew by #{growth} bytes over #{landed} interrupted gets"
    end

    def rss_bytes
      File.read("/proc/self/statm").split[1].to_i * Etc.sysconf(Etc::SC_PAGESIZE)
    end
  end
end
