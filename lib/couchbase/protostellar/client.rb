# frozen_string_literal: true

#  Copyright 2022-Present Couchbase, Inc.
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

require_relative "error_handling"
require_relative "error"

require_relative "generated/routing.v1_services_pb"
require_relative "generated/kv.v1_services_pb"
require_relative "generated/query.v1_services_pb"
require_relative "generated/search.v1_services_pb"
require_relative "generated/analytics.v1_services_pb"
require_relative "generated/view.v1_services_pb"
require_relative "generated/admin/collection.v1_services_pb"
require_relative "generated/admin/bucket.v1_services_pb"

module Couchbase
  module Protostellar
    class Client
      def initialize(host, credentials, channel_args)
        @channel = GRPC::Core::Channel.new(host, channel_args, credentials)
        @routing_stub = Generated::Routing::V1::Routing::Stub.new(host, credentials, channel_override: @channel)
        @kv_stub = Generated::KV::V1::Kv::Stub.new(host, credentials, channel_override: @channel)
        @query_stub = Generated::Query::V1::Query::Stub.new(host, credentials, channel_override: @channel)
        @search_stub = Generated::Search::V1::Search::Stub.new(host, credentials, channel_override: @channel)
        @analytics_stub = Generated::Analytics::V1::Analytics::Stub.new(host, credentials, channel_override: @channel)
        @view_stub = Generated::View::V1::View::Stub.new(host, credentials, channel_override: @channel)
        @bucket_admin_stub = Generated::Admin::Bucket::V1::BucketAdmin::Stub.new(host, credentials, channel_override: @channel)
        @collection_admin_stub = Generated::Admin::Collection::V1::CollectionAdmin::Stub.new(host, credentials, channel_override: @channel)
      end

      def stub(service)
        case service
        when :analytics
          @analytics_stub
        when :kv
          @kv_stub
        when :query
          @query_stub
        when :routing
          @routing_stub
        when :search
          @search_stub
        when :view
          @view_stub
        when :bucket_admin
          @bucket_admin_stub
        when :collection_admin
          @collection_admin_stub
        else
          raise Protostellar::Error::UnexpectedServiceType "service `#{service}' not recognised"
        end
      end

      def close
        @channel.close
      end

      def send_request(request)
        loop do
          return stub(request.service).public_send(request.rpc, request.proto_request, deadline: request.deadline)
        rescue GRPC::BadStatus => e
          request_behaviour = ErrorHandling.handle_grpc_error(e, request)

          unless request_behaviour.error.nil? || request_behaviour.retry_duration.nil?
            raise Protostellar::Error::InvalidRetryBehaviour "The error and the retry duration cannot both be set"
          end

          raise request_behaviour.error unless request_behaviour.error.nil?

          unless request_behaviour.retry_duration.nil?
            sleep(0.001 * request_behaviour.retry_duration)
            next
          end

          raise Protostellar::Error::InvalidRetryBehaviour "Either the error or the retry duration should have been set"
        end
      end
    end
  end
end
