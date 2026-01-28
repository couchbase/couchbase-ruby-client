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

require 'couchbase/tracing/request_span'
require 'couchbase/utils/observability_constants'

module Couchbase
  module Tracing
    class ThresholdLoggingSpan < RequestSpan
      attr_accessor :name
      attr_accessor :should_report
      attr_accessor :service
      attr_accessor :encode_duration_us
      attr_accessor :last_dispatch_duration_us
      attr_accessor :total_dispatch_duration_us
      attr_accessor :last_server_duration_us
      attr_accessor :total_server_duration_us
      attr_accessor :last_local_id
      attr_accessor :operation_id
      attr_accessor :last_peer_address
      attr_accessor :last_peer_port

      def initialize(name, start_timestamp: nil, parent: nil, tracer: nil)
        super()
        @name = name
        @parent = parent
        @tracer = tracer

        @start_timestamp = if start_timestamp.nil?
                             Time.now
                           else
                             start_timestamp
                           end
      end

      def set_attribute(key, value)
        case key
        when Observability::ATTR_OPERATION_ID
          @operation_id = value
        when Observability::ATTR_LOCAL_ID
          @last_local_id = value
        when Observability::ATTR_PEER_ADDRESS
          @last_peer_address = value
        when Observability::ATTR_PEER_PORT
          @last_peer_port = value
        when Observability::ATTR_SERVICE
          @service = value
        when Observability::ATTR_SERVER_DURATION
          @last_server_duration_us = value
        end
      end

      def status=(*); end

      def finish(end_timestamp: nil)
        duration_us = (((end_timestamp || Time.now) - @start_timestamp) * 1_000_000).round
        case name
        when Observability::STEP_REQUEST_ENCODING
          return if @parent.nil?

          @parent.should_report = true
          @parent.encode_duration_us = duration_us
        when Observability::STEP_DISPATCH_TO_SERVER
          return if @parent.nil?

          @parent.should_report = true
          @parent.last_dispatch_duration_us = duration_us
          @parent.total_dispatch_duration_us = 0 if @parent.total_dispatch_duration_us.nil?
          @parent.total_dispatch_duration_us += duration_us
          unless @last_server_duration_us.nil?
            @parent.last_server_duration_us = @last_server_duration_us
            @parent.total_server_duration_us = 0 if @parent.total_server_duration_us.nil?
            @parent.total_server_duration_us += @last_server_duration_us
          end
          @parent.last_local_id = @last_local_id
          @parent.operation_id = @operation_id
          @parent.last_peer_address = @last_peer_address
          @parent.last_peer_port = @last_peer_port
        else
          @should_report ||= @parent.nil?
          return unless @should_report && !@service.nil?

          @tracer.record_operation(@service, ThresholdLoggingTracer::Item.new(
                                               total_duration_us: duration_us,
                                               encode_duration_us: @encode_duration_us,
                                               last_dispatch_duration_us: @last_dispatch_duration_us,
                                               total_dispatch_duration_us: @total_dispatch_duration_us,
                                               last_server_duration_us: @last_server_duration_us,
                                               total_server_duration_us: @total_server_duration_us,
                                               operation_name: @name,
                                               last_local_id: @last_local_id,
                                               operation_id: @operation_id,
                                               last_remote_socket: "#{@last_peer_address}:#{@last_peer_port}",
                                             ))
        end
      end
    end
  end
end
