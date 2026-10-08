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
      Couchbase::Backend.notify_fork(:prepare)
      forked = false
      begin
        pid = super
        forked = true
      ensure
        # :prepare stopped the instances. When the fork raises or its thread is killed, no fork
        # happened to restart them.
        Couchbase::Backend.notify_fork(:parent) unless forked
      end
      # 0 in the child. Ruby treats 0 as true, so the test has to be explicit.
      if pid.zero?
        Couchbase::Backend.notify_fork(:child)
      else
        Couchbase::Backend.notify_fork(:parent)
      end
      pid
    end
  end
end

Process.singleton_class.prepend(Couchbase::ForkHooks)
