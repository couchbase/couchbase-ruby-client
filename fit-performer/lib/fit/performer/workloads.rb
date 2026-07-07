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

require 'fit/performer/workloads/sdk_workload'
require 'fit/performer/performer_error'

module FIT
  module Performer
    module Workloads
      def self.build_workload(raw_workload, connection, run_id, stream_owner, span_owner)
        workload_type = raw_workload.workload
        case workload_type
        when :sdk
          SdkWorkload.new(raw_workload.sdk, connection, run_id, stream_owner, span_owner)
        else
          raise PerformerError, "Workload type `#{workload_type}` not supported"
        end
      end
    end
  end
end
