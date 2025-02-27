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
      pid = super
      if pid
        Couchbase::Backend.notify_fork(:parent)
      else
        Couchbase::Backend.notify_fork(:child)
      end
      pid
    end
  end
end

Process.singleton_class.prepend(Couchbase::ForkHooks)
