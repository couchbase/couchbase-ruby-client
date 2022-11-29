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

require_relative "generated/routing.v1_services_pb"
require_relative "generated/kv.v1_services_pb"
require_relative "generated/query.v1_services_pb"
require_relative "generated/search.v1_services_pb"
require_relative "generated/analytics.v1_services_pb"
require_relative "generated/view.v1_services_pb"

module Couchbase
  module StellarNebula
    class Client
      def initialize(host, credentials, channel_args)
        @channel = GRPC::Core::Channel.new(host, channel_args, credentials)
        @routing_stub = Generated::Routing::V1::Routing::Stub.new(host, credentials, channel_override: @channel)
        @kv_stub = Generated::KV::V1::Kv::Stub.new(host, credentials, channel_override: @channel)
        @query_stub = Generated::Query::V1::Query::Stub.new(host, credentials, channel_override: @channel)
        @search_stub = Generated::Search::V1::Search::Stub.new(host, credentials, channel_override: @channel)
        @analytics_stub = Generated::Analytics::V1::Analytics::Stub.new(host, credentials, channel_override: @channel)
        @view_stub = Generated::View::V1::View::Stub.new(host, credentials, channel_override: @channel)
      end

      def kv
        @kv_stub
      end

      def close
        @channel.close
      end
    end
  end
end
