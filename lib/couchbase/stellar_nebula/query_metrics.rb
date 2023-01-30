# frozen_string_literal: true

#  Copyright 2022-Present. Couchbase, Inc.
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
  module StellarNebula
    class QueryMetrics
      attr_accessor :elapsed_time
      attr_accessor :execution_time
      attr_accessor :sort_count
      attr_accessor :result_count
      attr_accessor :result_size
      attr_accessor :mutation_count
      attr_accessor :error_count
      attr_accessor :warning_count

      def initialize(resp)
        @elapsed_time = resp.elapsed_time.nanos + (resp.elapsed_time.seconds * (10**9))
        @execution_time = resp.execution_time.nanos + (resp.execution_time.seconds * (10**9))
        @result_count = resp.result_count
        @result_size = resp.result_size
        @mutation_count = resp.mutation_count
        @sort_count = resp.sort_count
        @error_count = resp.error_count
        @warning_count = resp.warning_count

        yield self if block_given?
      end
    end
  end
end
