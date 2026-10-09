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

    # A child told it is the parent restarts the I/O it inherited, and reads from the parent's
    # connections.
    def test_fork_notifies_the_child_as_the_child
      events = []
      original = Backend.method(:notify_fork)
      Backend.define_singleton_method(:notify_fork) { |event| events << event }
      reader, writer = IO.pipe
      begin
        pid = Process.fork do
          reader.close
          writer.puts(events.join(","))
          exit!(0)
        end
      ensure
        Backend.define_singleton_method(:notify_fork, original)
      end
      writer.close
      child_events = reader.read.strip
      reader.close
      Process.wait(pid)

      assert_equal "prepare,child", child_events
      assert_equal [:prepare, :parent], events
    end
  end
end
