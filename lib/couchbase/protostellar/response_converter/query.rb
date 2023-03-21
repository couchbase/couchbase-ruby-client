# frozen_string_literal: true

#  Copyright 2023-Present. Couchbase, Inc.
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

require "json"

require "couchbase/query_options"

module Couchbase
  module Protostellar
    module ResponseConverter
      class Query
        STATUS_MAP = {
          :STATUS_RUNNING => :running,
          :STATUS_SUCCESS => :success,
          :STATUS_ERRORS => :errors,
          :STATUS_COMPLETED => :completed,
          :STATUS_STOPPED => :stopped,
          :STATUS_TIMEOUT => :timeout,
          :STATUS_CLOSED => :closed,
          :STATUS_FATAL => :fatal,
          :STATUS_ABORTED => :aborted,
          :STATUS_UNKNOWN => :unknown,
        }.freeze

        def self.from_query_responses(resps)
          Couchbase::Cluster::QueryResult.new do |res|
            rows = []
            resps.each do |resp|
              rows.push(*resp.rows)
              res.meta_data = convert_query_metadata(resp.meta_data) unless resp.meta_data.nil?
            end
            res.instance_variable_set(:@rows, rows)
          end
        end

        def self.convert_query_metadata(proto_metadata)
          Couchbase::Cluster::QueryMetaData.new do |meta|
            meta.status = STATUS_MAP[proto_metadata.status]
            meta.request_id = proto_metadata.request_id
            meta.client_context_id = proto_metadata.client_context_id
            meta.signature = JSON.parse(proto_metadata.signature) unless proto_metadata.signature.nil? || proto_metadata.signature.empty?
            meta.profile = JSON.parse(proto_metadata.profile) unless proto_metadata.profile.nil? || proto_metadata.profile.empty?
            meta.metrics = convert_query_metrics(proto_metadata.metrics) unless proto_metadata.metrics.nil?
            meta.warnings = resp.warnings.map { |w| convert_query_warning(w) } unless proto_metadata.warnings.empty?
          end
        end

        def self.convert_query_warning(proto_warning)
          Couchbase::Cluster::QueryWarning.new(proto_warning.code, proto_warning.message)
        end

        def self.convert_query_metrics(proto_metrics)
          Couchbase::Cluster::QueryMetrics.new do |metrics|
            metrics.elapsed_time = proto_metrics.elapsed_time.nanos + (proto_metrics.elapsed_time.seconds * (10**9))
            metrics.execution_time = proto_metrics.execution_time.nanos + (proto_metrics.execution_time.seconds * (10**9))
            metrics.result_count = proto_metrics.result_count
            metrics.result_size = proto_metrics.result_size
            metrics.mutation_count = proto_metrics.mutation_count
            metrics.sort_count = proto_metrics.sort_count
            metrics.error_count = proto_metrics.error_count
            metrics.warning_count = proto_metrics.warning_count
          end
        end
      end
    end
  end
end
