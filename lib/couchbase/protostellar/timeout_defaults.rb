# frozen_string_literal: true

#  Copyright 2023. Couchbase, Inc.
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
  module Protostellar
    module TimeoutDefaults
      KEY_VALUE = 2_500
      KEY_VALUE_DURABLE = 10_000
      VIEW = 75_000
      QUERY = 75_000
      ANALYTICS = 75_000
      SEARCH = 75_000
      MANAGEMENT = 75_000
    end
  end
end
