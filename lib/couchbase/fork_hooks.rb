# frozen_string_literal: true

#  Copyright 2020-2025 Couchbase, Inc.
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
  module ForkHooks
    def _fork
      # :prepare stops the instances and only :parent or :child restarts them, so an asynchronous
      # exception is deferred everywhere except inside the fork itself.
      Thread.handle_interrupt(Object => :never) do
        parent_pid = Process.pid
        Couchbase::Backend.notify_fork(:prepare)
        begin
          Thread.handle_interrupt(Object => :immediate) do
            # A check point: an interrupt deferred during :prepare lands here, before the fork.
            Thread.pass
            super
          end
        ensure
          # Decided by pid rather than by the return value, which an exception raised after the fork
          # never delivers.
          Couchbase::Backend.notify_fork(Process.pid == parent_pid ? :parent : :child)
        end
      end
    end
  end
end

Process.singleton_class.prepend(Couchbase::ForkHooks)
