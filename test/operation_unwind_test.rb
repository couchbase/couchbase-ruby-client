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
require "couchbase/tracing/noop_tracer"

module Couchbase
  # RCBC-567: an operation's C++ frame must unwind, destroying its response and cluster
  # reference, when the operation fails and when a Ruby exception or interrupt is raised while
  # the backend records spans or builds the error. Each leaked response holds one large document
  # body or query row, so RSS over many such operations shows the leak.
  class OperationUnwindTest < Minitest::Test
    include TestUtilities

    class TracerError < StandardError; end

    # Calls the hook when the observability handler records a span reported by the backend,
    # the only caller that passes start_timestamp.
    class HookTracer < Tracing::NoopTracer
      attr_accessor :hook

      def request_span(name, parent: nil, start_timestamp: nil)
        @hook&.call unless start_timestamp.nil?
        super
      end
    end

    # Calls the current thread's hook with the exception from CouchbaseError#initialize, which the
    # backend runs while it builds the exception for a failed operation.
    module InitializeHook
      def initialize(*)
        Thread.current[:operation_unwind_initialize_hook]&.call(self)
        super
      end
    end
    Error::CouchbaseError.prepend(InitializeHook)

    DOCUMENT_SIZE = 4 * 1024 * 1024
    ITERATIONS = 100
    WARMUP_ITERATIONS = 10

    def setup
      skip("#{name}: #{Couchbase::Protostellar::NAME} does not record backend spans") if env.protostellar?
      @tracer = HookTracer.new
      connect(Options::Cluster.new(tracer: @tracer))
      @collection = @cluster.bucket(env.bucket).default_collection
      @transcoder = RawBinaryTranscoder.new
      @doc_id = uniq_id(:operation_unwind)
      @content = "x" * DOCUMENT_SIZE
      @cas = @collection.upsert(@doc_id, @content, Options::Upsert.new(transcoder: @transcoder)).cas
      @get_options = Options::Get.new(transcoder: @transcoder)
      @entered = 0
      # The timeout library bundled with Ruby 3.2 starts its watcher thread on first use, and the
      # thread inherits the caller's interrupt mask (ruby/timeout#41). Started under
      # Object => :never, the watcher cannot be killed, and the process never exits.
      Timeout.timeout(1) { nil }
    end

    def teardown
      Thread.current[:operation_unwind_initialize_hook] = nil
      @tracer&.hook = nil
      @collection&.remove(@doc_id)
      disconnect
    end

    def test_get_with_tracer_raising_does_not_leak_response
      @tracer.hook = method(:raise_tracer_error)
      assert_no_leak do
        error = assert_raises(TracerError) { @collection.get(@doc_id, @get_options) }

        assert_equal "tracer failed", error.message
      end
    end

    def test_get_interrupted_by_timeout_during_span_recording_does_not_leak_response
      @tracer.hook = method(:block_until_interrupted)
      assert_no_leak do
        assert_raises(Timeout::Error) do
          Thread.handle_interrupt(Object => :never) do
            Timeout.timeout(0.001) { @collection.get(@doc_id, @get_options) }
          end
        end
      end

      assert_equal WARMUP_ITERATIONS + ITERATIONS, @entered
    end

    def test_get_interrupted_by_thread_kill_during_span_recording_does_not_leak_response
      @tracer.hook = method(:block_until_interrupted)
      assert_no_leak do
        completed = false
        started = Queue.new
        thread = Thread.new do # rubocop:disable ThreadSafety/NewThread
          Thread.handle_interrupt(Object => :never) do
            started << true
            @collection.get(@doc_id, @get_options)
            completed = true
          end
        end
        started.pop
        thread.kill
        thread.join

        refute completed
      end

      assert_equal WARMUP_ITERATIONS + ITERATIONS, @entered
    end

    def test_query_with_tracer_raising_does_not_leak_response
      skip("#{name}: CAVES does not support query service") if use_caves?
      statement = "SELECT RAW REPEAT('x', #{DOCUMENT_SIZE})"
      @tracer.hook = method(:raise_tracer_error)
      assert_no_leak do
        error = assert_raises(TracerError) { @cluster.query(statement) }

        assert_equal "tracer failed", error.message
      end
    end

    # The query returns one row of DOCUMENT_SIZE bytes and then fails in ABORT() with code 5011,
    # so the response that is live while the error is built holds the row. A server that rejects
    # the statement before the row fails with another class.
    def test_query_interrupted_while_building_its_error_does_not_leak_response
      skip("#{name}: CAVES does not support query service") if use_caves?
      statement = "SELECT RAW CASE WHEN a = 2 THEN ABORT('stop') ELSE REPEAT('x', #{DOCUMENT_SIZE}) END FROM [1, 2] AS a"
      error_classes = []
      Thread.current[:operation_unwind_initialize_hook] = lambda do |error|
        error_classes << error.class
        block_until_interrupted
      end
      assert_no_leak do
        assert_raises(Timeout::Error) do
          Thread.handle_interrupt(Object => :never) do
            Timeout.timeout(0.001) { @cluster.query(statement) }
          end
        end
      end

      assert_equal WARMUP_ITERATIONS + ITERATIONS, @entered
      assert_equal [Error::InternalServerFailure], error_classes.uniq
    end

    # A failed operation raises from cb_method instead of from its catch handler; the error must
    # reach the caller with the same class, message and backtrace origin.
    def test_get_of_missing_document_raises_error
      missing_id = uniq_id(:missing)
      100.times do
        error = assert_raises(Error::DocumentNotFound) { @collection.get(missing_id) }

        assert_match(/\Aunable to fetch document: document_not_found \(101\)/, error.message)
        assert_equal "document_get", error.backtrace_locations.first.base_label
      end
    end

    def test_get_with_tracer_not_raising_returns_document
      @tracer.hook = lambda { @entered += 1 }
      res = @collection.get(@doc_id, @get_options)

      assert_operator @entered, :>, 0
      assert_equal @content, res.content
      assert_equal @cas, res.cas
    end

    private

    def raise_tracer_error
      raise TracerError, "tracer failed"
    end

    def block_until_interrupted
      @entered += 1
      Thread.handle_interrupt(Object => :immediate) { sleep }
    end

    # Runs the block ITERATIONS times and bounds RSS growth by a quarter of a document per
    # iteration.
    def assert_no_leak(&)
      skip("#{name}: 4 MiB round trips are slow on CAVES; the code under test is server-independent") if use_caves?
      skip("#{name}: RSS is read from /proc/self/statm") unless File.readable?("/proc/self/statm")
      WARMUP_ITERATIONS.times(&)
      GC.start
      before = rss_bytes
      ITERATIONS.times(&)
      GC.start
      growth = rss_bytes - before

      assert_operator growth, :<, ITERATIONS * DOCUMENT_SIZE / 4, "RSS grew by #{growth} bytes over #{ITERATIONS} operations"
    end

    def rss_bytes
      File.read("/proc/self/statm").split[1].to_i * Etc.sysconf(Etc::SC_PAGESIZE)
    end
  end
end
