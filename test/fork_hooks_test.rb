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

module Couchbase
  class ForkHooksTest < Minitest::Test
    def setup
      skip("Forking not supported on Windows") if Gem.win_platform?
      skip("Forking not supported") unless Process.respond_to?(:fork)
    end

    # Catches a child notified as the parent, which restarts the I/O it inherited and reads from the
    # parent's connections.
    def test_fork_notifies_the_child_as_the_child
      events = []
      child_events = in_fork(events) { Process._fork }

      assert_equal "prepare,child", child_events
      assert_equal [:prepare, :parent], events
    end

    # Catches an interrupt during :prepare that leaves the instances stopped, or that still forks a
    # child the caller cannot wait on.
    def test_interrupt_during_prepare_restarts_the_instances_without_forking
      events = []
      notify = lambda do |event|
        events << event
        Thread.current.raise(Interrupt) if event == :prepare
      end
      reader, writer = IO.pipe

      with_stubs(notify, nil) do
        assert_raises(Interrupt) do
          Process.fork do
            writer.write("forked")
            exit!(0)
          end
        end
      end
      writer.close
      forked = reader.read
      reader.close

      assert_empty forked
      assert_equal [:prepare, :parent], events
    end

    # Catches an interrupt raised after the fork that notifies the child as the parent.
    def test_interrupt_after_fork_still_notifies_the_child
      events = []
      child_events = in_fork(events) do |original_fork, child_pid|
        Process.singleton_class.send(:define_method, :_fork) do
          child_pid << original_fork.bind_call(Process)
          Thread.current.raise(Interrupt)
        end
        assert_raises(Interrupt) { Process._fork }
      end

      assert_equal "prepare,child", child_events
      assert_equal [:prepare, :parent], events
    end

    private

    # Runs the block, which forks, with Backend.notify_fork recording into +events+. Returns the events
    # the child recorded. The child always exits here, whatever the block raised in it.
    def in_fork(events)
      parent_pid = Process.pid
      child_pid = []
      reader, writer = IO.pipe
      with_stubs(lambda { |event| events << event }, nil) do |original_fork|
        pid = yield(original_fork, child_pid)
        child_pid << pid if pid.is_a?(Integer)
      ensure
        unless Process.pid == parent_pid
          writer.puts(events.join(","))
          exit!(0)
        end
      end
      writer.close
      child_events = reader.read.strip
      reader.close
      Process.wait(child_pid.first)
      child_events
    end

    def with_stubs(notify, fork_impl)
      original_notify = Backend.method(:notify_fork)
      original_fork = Process.singleton_class.instance_method(:_fork).super_method
      Backend.define_singleton_method(:notify_fork, &notify)
      Process.singleton_class.send(:define_method, :_fork, &fork_impl) if fork_impl
      yield original_fork
    ensure
      Process.singleton_class.send(:define_method, :_fork, original_fork) if original_fork
      Backend.define_singleton_method(:notify_fork, original_notify) if original_notify
    end
  end
end
