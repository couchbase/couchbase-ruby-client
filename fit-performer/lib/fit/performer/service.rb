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

require 'logger'
require 'securerandom'

require 'grpc'

require 'couchbase'

require 'fit/performer/connections'
require 'fit/performer/performer_error'
require 'fit/performer/multi_thread_executor'
require 'fit/performer/request_executor'
require 'fit/performer/streaming'
require 'fit/performer/observability/span_owner'
require 'fit/performer/counters'

require 'fit/protocol/performer_services_pb'
require 'fit/protocol/performer.caps_pb'
require 'fit/protocol/shared.echo_pb'

module FIT
  module Performer
    class Service < FIT::Protocol::PerformerService::Service
      # rubocop:disable Naming/VariableNumber
      PERFORMER_CAPS = [
        :KV_SUPPORT_1,
        :CLUSTER_CONFIG_CERT,
        :CLUSTER_CONFIG_INSECURE,
        :CUSTOM_SERIALIZATION_SUPPORT_FOR_SEARCH,
        :OBSERVABILITY_1,
      ].freeze

      SDK_CAPS = [
        :SDK_QUERY_INDEX_MANAGEMENT,
        :SDK_COLLECTION_QUERY_INDEX_MANAGEMENT,
        :SDK_SEARCH_INDEX_MANAGEMENT,
        :SDK_QUERY,
        :SDK_LOOKUP_IN,
        :SDK_BUCKET_MANAGEMENT,
        :SDK_KV,
        :SDK_SEARCH,
        :SUPPORTS_AUTHENTICATOR,
        :SDK_QUERY_READ_FROM_REPLICA,
        :SDK_KV_RANGE_SCAN,
        :SDK_LOOKUP_IN_REPLICAS,
        :SDK_COLLECTION_MANAGEMENT,
        :SDK_MANAGEMENT_HISTORY_RETENTION,
        :SDK_DOCUMENT_NOT_LOCKED,
        :SDK_VECTOR_SEARCH,
        :SDK_SCOPE_SEARCH_INDEX_MANAGEMENT,
        :SDK_SCOPE_SEARCH,
        :SDK_INDEX_MANAGEMENT_RFC_REVISION_25,
        :SDK_SEARCH_RFC_REVISION_11,
        :SDK_VECTOR_SEARCH_BASE64,
        :SDK_ZONE_AWARE_READ_FROM_REPLICA,
        :SDK_BUCKET_SETTINGS_NUM_VBUCKETS,
        :SDK_PREFILTER_VECTOR_SEARCH,
        :SDK_APP_TELEMETRY,
        :SDK_SET_AUTHENTICATOR,
        :SDK_JWT,
        :SDK_OBSERVABILITY_CLUSTER_LABELS,
        :SDK_OBSERVABILITY_RFC_REV_24,
        :SDK_STABLE_OTEL_SEMANTIC_CONVENTIONS,
        :SDK_STABLE_OTEL_SEMANTIC_CONVENTIONS_EMITTED_BY_DEFAULT,
      ].freeze

      # We don't currently support transactions. However, the driver calls transactions_factory_create during the
      # non-transactional observability tests if we don't declare EXT_SDK_INTEGRATION. So, let's declare it here until
      # that issue is fixed in the driver.
      TRANSACTION_CAPS = [
        :EXT_SDK_INTEGRATION,
      ].freeze

      # rubocop:enable Naming/VariableNumber

      SDK_VERSION = Couchbase::VERSION[:sdk]

      def initialize
        super
        @logger = Logger.new($stdout)
        @connections = Connections.new
        @stream_owner = Streaming::StreamOwner.new
        @span_owner = Observability::SpanOwner.new
        @global_counters = Counters.new
        @logger.info("Using Ruby SDK version #{SDK_VERSION}")
      end

      def performer_caps_fetch(_request, _call)
        @logger.info("performer_caps_fetch called")
        resp = FIT::Protocol::Performer::PerformerCapsFetchResponse.new(
          transaction_implementations_caps: TRANSACTION_CAPS,
          sdk_implementation_caps: SDK_CAPS,
          performer_caps: PERFORMER_CAPS,
          performer_user_agent: "ruby",
          library_version: SDK_VERSION,
          supported_apis: [:DEFAULT],
        )
        @logger.info("Reporting caps #{resp}")
        resp
      end

      def cluster_connection_create(request, _call)
        @logger.info("cluster_connection_create called")
        @connections.create_connection(request)
      end

      def cluster_connection_close(request, _call)
        @logger.info("cluster_connection_close called")
        @connections.close_connection(request)
      end

      def disconnect_connections(request, _call)
        @logger.info("disconnect_connections called")
        @connections.disconnect_connections(request)
      end

      def echo(request, _call)
        @logger.info("====== #{request.testName}: #{request.message} ======")
        FIT::Protocol::Shared::EchoResponse.new
      end

      def run(request, _call)
        @logger.info("run called")

        # Generate a random run ID - used for identifying streams started from this run
        run_id = SecureRandom.uuid

        request_type = request.request
        case request_type
        when :workloads
          begin
            connection = @connections[request.workloads.cluster_connection_id]
            workload_executor = MultiThreadExecutor.build_executor(
              request, connection, run_id, @stream_owner, @span_owner, @global_counters
            )
            @logger.info("Created the workload executor")
            req_executor = RequestExecutor.build_request(workload_executor, request)
            @logger.info("Created the request executor")
            req_executor.execute_request
            @logger.info("Executing the request")
            req_executor.results
          rescue PerformerError => e
            raise GRPC::Unimplemented, e.message
          rescue StandardError => e
            @logger.error(e.message)
            @logger.error(e.backtrace.join("\n"))
            raise e
          end
        else
          raise GRPC::Unimplemented, "Run request type #{request_type} not supported"
        end
      end

      def stream_cancel(request, _call)
        @stream_owner.cancel(request)
        FIT::Protocol::Streams::CancelResponse.new
      end

      def stream_request_items(request, _call)
        @stream_owner.request_items(request)
        FIT::Protocol::Streams::RequestItemsResponse.new
      end

      def span_create(request, _call)
        tracer = @connections[request.cluster_connection_id].tracer
        @span_owner.create_span(tracer, request)
        FIT::Protocol::Observability::SpanCreateResponse.new
      rescue PerformerError => e
        raise GRPC::FailedPrecondition, e.message
      end

      def span_finish(request, _call)
        @span_owner.finish_span(request)
        FIT::Protocol::Observability::SpanFinishResponse.new
      rescue PerformerError => e
        raise GRPC::FailedPrecondition, e.message
      end

      def set_counter(request, _call)
        @global_counters.set_counter_value(request.counter_id, request.global.count)
        FIT::Protocol::Shared::SetCounterResponse.new
      end

      def clear_all_counters(_request, _call)
        @global_counters.clear
        FIT::Protocol::Shared::ClearAllCountersResponse.new
      end
    end
  end
end
