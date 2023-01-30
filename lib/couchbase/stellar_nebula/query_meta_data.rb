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

require_relative "json_transcoder"
require_relative "query_metrics"
require_relative "query_warning"

module Couchbase
  module StellarNebula
    class QueryMetaData
      attr_accessor :request_id
      attr_accessor :client_context_id
      attr_accessor :status
      attr_accessor :signature
      attr_accessor :profile
      attr_accessor :metrics
      attr_accessor :warnings

      def initialize(resp)
        @request_id = resp.request_id
        @client_context_id = resp.client_context_id
        @status = resp.status.downcase

        transcoder = JsonTranscoder.new
        @profile = transcoder.decode(resp.profile) unless resp.profile.nil?
        @signature = transcoder.decode(resp.signature)

        @metrics = QueryMetrics.new(resp.metrics) unless resp.metrics.nil?
        @warnings = resp.warnings.map { |w| QueryWarning.new(w.code, w.message) } unless resp.warnings.empty?

        yield self if block_given?
      end
    end
  end
end
