# frozen_string_literal: true

#  Copyright 2025-Present Couchbase, Inc.
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

require_relative 'meter'
require_relative 'noop_value_recorder'

module Couchbase
  module Metrics
    class NoopMeter < Meter
      def value_recorder(_name, _tags)
        VALUE_RECORDER_INSTANCE
      end

      VALUE_RECORDER_INSTANCE = NoopValueRecorder.new.freeze
    end
  end
end
