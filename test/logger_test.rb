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

require "English"
require "timeout"

# Regression tests for RCBC-568: a logger callback that raises an exception which is not a StandardError
# must not leave the logger sink locked, and the messages queued behind it must still be delivered.
class LoggerTest < Minitest::Test
  include Couchbase::TestUtilities

  class CallbackAdapter
    def initialize(callback, **)
      @callback = callback
    end

    def log(_level, _thread_id, seconds, nanoseconds, *)
      @callback.call(seconds + (nanoseconds / 1e9))
    end
  end

  Child = Struct.new(:cluster, :collection, :writer, :hook) do
    def arm
      hook[:armed] = true
    end

    def mark(label)
      writer.puts("mark #{label}")
    end
  end

  def setup
    skip("Forking not supported on Windows") if Gem.win_platform?
    skip("Forking not supported") unless Process.respond_to?(:fork)
    skip("Cannot use gRPC before and after fork (unless GRPC_ENABLE_FORK_SUPPORT is set)") if env.protostellar?
  end

  def test_exception_requeues_messages_for_the_next_flush
    failure = lambda { raise Exception, "logger failure" } # rubocop:disable Lint/RaiseException
    events = run_with_failing_logger(failure) do |child|
      child.arm
      begin
        child.cluster.disconnect
      rescue Exception # rubocop:disable Lint/RescueException
        child.mark(:raised)
      end
      Couchbase::Cluster.connect(env.connection_string, env.username, env.password).disconnect
      child.mark(:next)
    end

    assert_stopped_at_failure(events, :raised)
    assert_requeued_delivered(events, :raised, :next)
  end

  def test_interrupt_requeues_messages_for_exit
    events = run_with_failing_logger(lambda { raise Interrupt }) do |child|
      child.arm
      begin
        child.cluster.disconnect
      rescue Interrupt
        child.mark(:raised)
      end
    end

    assert_stopped_at_failure(events, :raised)
    assert_requeued_delivered(events, :raised)
  end

  def test_timeout_requeues_messages_for_set_logger
    events = run_with_failing_logger(lambda { sleep }) do |child|
      child.arm
      begin
        Timeout.timeout(5) { child.cluster.disconnect }
      rescue Timeout::Error
        child.mark(:raised)
      end
      Couchbase.set_logger(lambda { |_time| }, adapter_class: CallbackAdapter, level: :debug)
      child.mark(:replaced)
    end

    assert_stopped_at_failure(events, :raised)
    assert_requeued_delivered(events, :raised, :replaced)
  end

  def test_thread_kill_terminates_thread_and_requeues_messages
    entered = Queue.new
    failure = lambda {
      entered << true
      sleep
    }
    events = run_with_failing_logger(failure) do |child|
      child.arm
      thread = Thread.new { child.cluster.disconnect } # rubocop:disable ThreadSafety/NewThread
      raise "logger callback was not entered" unless entered.pop(timeout: 10)

      thread.kill
      child.mark(:killed) if thread.join(10) && thread.status == false
      Couchbase::Cluster.connect(env.connection_string, env.username, env.password).disconnect
      child.mark(:next)
    end

    assert_stopped_at_failure(events, :killed)
    assert_requeued_delivered(events, :killed, :next)
  end

  def test_exception_in_operation_flush_is_raised_and_next_operation_succeeds
    failure = lambda { raise Exception, "logger failure" } # rubocop:disable Lint/RaiseException
    events = run_with_failing_logger(failure, level: :trace) do |child|
      child.arm
      begin
        child.collection.upsert(uniq_id(:logger), {})
      rescue Exception # rubocop:disable Lint/RescueException
        child.mark(:raised)
      end
      child.collection.upsert(uniq_id(:logger), {})
      child.mark(:next)
      child.cluster.disconnect
    end

    assert_stopped_at_failure(events, :raised)
    assert_requeued_delivered(events, :raised, :next)
  end

  def test_exception_in_fork_prepare_flush_keeps_cluster_working
    failure = lambda { raise Exception, "logger failure" } # rubocop:disable Lint/RaiseException
    events = run_with_failing_logger(failure, level: :trace) do |child|
      child.arm
      begin
        child.collection.upsert(uniq_id(:logger), {})
      rescue Exception # rubocop:disable Lint/RescueException
        child.mark(:raised)
      end
      child.arm
      begin
        Process.wait(Process.fork { exit!(0) })
      rescue Exception # rubocop:disable Lint/RescueException
        child.mark(:fork_raised)
      end
      child.collection.upsert(uniq_id(:logger), {})
      child.mark(:next)
      child.cluster.disconnect
    end

    assert_includes events, [:mark, "fork_raised"]
    assert_includes events, [:mark, "next"]
  end

  def test_exception_during_set_logger_installs_the_new_logger
    failure = lambda { raise Exception, "logger failure" } # rubocop:disable Lint/RaiseException
    events = run_with_failing_logger(failure) do |child|
      child.arm
      begin
        child.cluster.disconnect
      rescue Exception # rubocop:disable Lint/RescueException
        child.mark(:raised)
      end
      new_logger = lambda { |time| child.writer.puts("new #{time}") }
      child.arm
      begin
        Couchbase.set_logger(new_logger, adapter_class: CallbackAdapter, level: :debug)
      rescue Exception # rubocop:disable Lint/RescueException
        child.mark(:set_logger_raised)
      end
      Couchbase::Cluster.connect(env.connection_string, env.username, env.password).disconnect
      child.mark(:next)
    end

    assert_includes events, [:mark, "set_logger_raised"]
    assert_requeued_delivered(events, :set_logger_raised, :next, kind: :new)
    fail_time = events.reverse.find { |kind, _| kind == :fail }[1]

    assert_operator events.count { |kind, time| kind == :new && time > fail_time }, :>, 0,
                    "messages logged after set_logger did not reach the new logger"
  end

  def test_exit_flush_retry_keeps_the_exit_error_info
    failure = lambda { raise Exception, "logger failure" } # rubocop:disable Lint/RaiseException
    events = run_with_failing_logger(failure) do |child|
      child.arm
      begin
        child.cluster.disconnect
      rescue Exception # rubocop:disable Lint/RescueException
        child.mark(:raised)
      end
      child.hook[:each] = lambda { child.mark("errinfo #{$ERROR_INFO.inspect}") }
      child.arm
    end

    assert_operator events.count([:mark, "errinfo nil"]), :>, 1
    assert_equal [[:mark, "errinfo nil"]], events.select { |kind, label| kind == :mark && label.start_with?("errinfo") }.uniq
  end

  def test_signal_during_exit_flush_terminates_the_process
    failure = lambda { raise Exception, "logger failure" } # rubocop:disable Lint/RaiseException
    events = run_with_failing_logger(failure, signal: "TERM") do |child|
      child.arm
      begin
        child.cluster.disconnect
      rescue Exception # rubocop:disable Lint/RescueException
        child.mark(:raised)
      end
      child.hook[:each] = lambda {
        child.mark(:exit_flush)
        sleep
      }
    end

    assert_equal 1, events.count([:mark, "exit_flush"])
  end

  def test_failed_fork_keeps_cluster_working
    events = run_with_failing_logger(lambda {}) do |child|
      Process.singleton_class.send(:define_method, :_fork) { raise Errno::EAGAIN }
      begin
        Process.fork { exit!(0) }
      rescue Errno::EAGAIN
        child.mark(:fork_raised)
      end
      child.collection.upsert(uniq_id(:logger), {})
      child.mark(:next)
      child.cluster.disconnect
    end

    assert_includes events, [:mark, "fork_raised"]
    assert_includes events, [:mark, "next"]
  end

  def test_killed_fork_keeps_cluster_working
    entered = Queue.new
    events = run_with_failing_logger(lambda {}) do |child|
      Process.singleton_class.send(:define_method, :_fork) do
        entered << true
        sleep
      end
      thread = Thread.new { Process.fork { exit!(0) } } # rubocop:disable ThreadSafety/NewThread
      raise "Process._fork was not entered" unless entered.pop(timeout: 10)

      thread.kill
      child.mark(:killed) if thread.join(10) && thread.status == false
      child.collection.upsert(uniq_id(:logger), {})
      child.mark(:next)
      child.cluster.disconnect
    end

    assert_includes events, [:mark, "killed"]
    assert_includes events, [:mark, "next"]
  end

  def test_standard_error_is_rescued_and_messages_are_written_in_the_same_flush
    events = run_with_failing_logger(lambda { raise StandardError, "logger failure" }) do |child|
      child.arm
      child.cluster.disconnect
      child.mark(:returned)
    end

    fail_index = events.index { |kind, _| kind == :fail }

    refute_nil fail_index
    assert_operator events[fail_index...events.index([:mark, "returned"])].count { |kind, _| kind == :log }, :>, 0
  end

  private

  # No log call runs between the failing callback and the point where the caller sees its result.
  def assert_stopped_at_failure(events, outcome)
    fail_index = events.index { |kind, _| kind == :fail }
    outcome_index = events.index([:mark, outcome.to_s])

    refute_nil fail_index, "the logger callback did not fail"
    refute_nil outcome_index, "the failure did not reach the caller as #{outcome}"
    assert_equal 0, events[fail_index...outcome_index].count { |kind, _| kind == :log }, "the logger kept running after the failure"
  end

  # Messages produced before the failure reach a logger (+kind+) after it, before +until_mark+ or by process exit.
  def assert_requeued_delivered(events, outcome, until_mark = nil, kind: :log)
    from = events.index([:mark, outcome.to_s])
    fail_time = events[0...from].reverse.find { |event, _| event == :fail }[1]
    to = until_mark ? events.index([:mark, until_mark.to_s]) : events.size

    refute_nil to, "#{until_mark} was not reached"
    assert_operator events[from...to].count { |event, time| event == kind && time < fail_time }, :>, 0,
                    "no message produced before the failure arrived after it: the queued messages were lost, " \
                    "or the failing message was the last of its batch"
  end

  # Runs the block in a forked child with a logger that writes every call to a pipe, and fails the first call after
  # +arm+ with +failure+. A sink left locked hangs the child with the GVL held, so the parent kills it. With +signal+,
  # the parent sends it once the child marks +exit_flush+, and expects the child to die by it.
  def run_with_failing_logger(failure, level: :debug, signal: nil)
    reader, writer = IO.pipe
    pid = Process.fork do
      reader.close
      writer.sync = true
      hook = {armed: false}
      Couchbase.set_logger(lambda { |time|
        hook[:each]&.call
        # A callback that rescues internally clears $!, which must not affect re-raising an earlier failure.
        begin
          raise "rescued inside the logger"
        rescue StandardError
          nil
        end
        if hook[:armed]
          hook[:armed] = false
          writer.puts("fail #{Process.clock_gettime(Process::CLOCK_REALTIME)}")
          failure.call
        end
        writer.puts("log #{time}")
      }, adapter_class: CallbackAdapter, level: level)
      cluster = Couchbase::Cluster.connect(env.connection_string, env.username, env.password)
      collection = cluster.bucket(env.bucket).default_collection
      collection.upsert(uniq_id(:logger), {})
      yield Child.new(cluster, collection, writer, hook)
    end
    writer.close
    output = +""
    if signal
      begin
        Timeout.timeout(FORK_CHILD_TIMEOUT) do
          while (line = reader.gets)
            output << line
            break Process.kill(signal, pid) if line == "mark exit_flush\n"
          end
        end
      rescue Timeout::Error
        nil
      end
    end
    status = wait_for_child_or_kill(pid)
    output << reader.read
    reader.close

    if signal
      assert_equal Signal.list[signal], status.termsig, "Child process was not terminated by SIG#{signal}: #{status}"
    else
      assert_predicate status, :success?, "Child process failed with #{status}"
    end
    output.lines.map do |line|
      kind, value = line.split(" ", 2)
      kind == "mark" ? [:mark, value.strip] : [kind.to_sym, Float(value)]
    end
  end
end
