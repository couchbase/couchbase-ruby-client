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

# frozen_string_literal: true

require_relative "error_handling"

require_relative "generated/routing/v1/routing_services_pb"
require_relative "generated/kv/v1/kv_services_pb"
require_relative "generated/query/v1/query_services_pb"
require_relative "generated/search/v1/search_services_pb"
require_relative "generated/analytics/v1/analytics_services_pb"
require_relative "generated/view/v1/view_services_pb"
require_relative "generated/admin/collection/v1/collection_services_pb"
require_relative "generated/admin/bucket/v1/bucket_services_pb"
require_relative "generated/admin/query/v1/query_services_pb"
require_relative "generated/admin/search/v1/search_services_pb"

module Couchbase
  module Protostellar
    # @api private
    class Client
      def initialize(host:, credentials:, channel_args:, call_metadata:, timeouts:)
        @channel = GRPC::Core::Channel.new(host, channel_args, credentials)
        @call_metadata = call_metadata
        @timeouts = timeouts

        @stubs = {
          routing: Generated::Routing::V1::RoutingService::Stub.new(host, credentials, channel_override: @channel),
          kv: Generated::KV::V1::KvService::Stub.new(host, credentials, channel_override: @channel),
          query: Generated::Query::V1::QueryService::Stub.new(host, credentials, channel_override: @channel),
          search: Generated::Search::V1::SearchService::Stub.new(host, credentials, channel_override: @channel),
          analytics: Generated::Analytics::V1::AnalyticsService::Stub.new(host, credentials, channel_override: @channel),
          view: Generated::View::V1::ViewService::Stub.new(host, credentials, channel_override: @channel),
          bucket_admin: Generated::Admin::Bucket::V1::BucketAdminService::Stub.new(host, credentials, channel_override: @channel),
          collection_admin: Generated::Admin::Collection::V1::CollectionAdminService::Stub.new(host, credentials,
                                                                                               channel_override: @channel),
          query_admin: Generated::Admin::Query::V1::QueryAdminService::Stub.new(host, credentials, channel_override: @channel),
          search_admin: Generated::Admin::Search::V1::SearchAdminService::Stub.new(host, credentials, channel_override: @channel),
        }
      end

      def close
        @channel.close
      end

      def send_request(request)
        request.set_timeout_from_defaults(@timeouts)
        loop do
          resp = @stubs[request.service].public_send(request.rpc, request.proto_request, deadline: request.deadline,
                                                                                         metadata: @call_metadata)
          return resp unless resp.respond_to?(:next)

          # Streaming RPC - wrap it in an enumerator that handles any mid-stream errors
          return Enumerator.new do |y|
            loop do
              y << resp.next
            rescue GRPC::BadStatus => e
              request_behaviour = ErrorHandling.handle_grpc_error(e, request)
              raise request_behaviour.error unless request_behaviour.error.nil?

              unless request_behaviour.retry_duration.nil?
                raise Couchbase::Error::RequestCanceled.new("Error encountered mid-stream",
                                                            request.error_context)
              end

              next
            end
          end

        # Simple RPC - just return it
        rescue GRPC::BadStatus => e
          request_behaviour = ErrorHandling.handle_grpc_error(e, request)

          unless request_behaviour.error.nil? ^ request_behaviour.retry_duration.nil?
            raise Couchbase::Error::CouchbaseError, "Either the error or the retry duration can be set"
          end
          raise request_behaviour.error unless request_behaviour.error.nil?

          unless request_behaviour.retry_duration.nil?
            sleep(0.001 * request_behaviour.retry_duration)
            next
          end
        end
      end
    end
  end
end
