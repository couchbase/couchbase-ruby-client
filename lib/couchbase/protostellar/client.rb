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
      end

      def kv
        @kv_stub
      end

      def close
        @channel.close
      end

      def get(request, timeout: nil)
        @kv_stub.get(request, deadline: get_deadline(timeout))
      end

      def get_and_touch(request, timeout: nil)
        @kv_stub.get_and_touch(request, deadline: get_deadline(timeout))
      end

      def get_and_lock(request, timeout: nil)
        @kv_stub.get_and_lock(request, deadline: get_deadline(timeout))
      end

      def unlock(request, timeout: nil)
        @kv_stub.unlock(request, deadline: get_deadline(timeout))
      end

      def get_replica(request, timeout: nil)
        @kv_stub.get_replica(request, deadline: get_deadline(timeout))
      end

      def touch(request, timeout: nil)
        @kv_stub.touch(request, deadline: get_deadline(timeout))
      end

      def exists(request, timeout: nil)
        @kv_stub.exists(request, deadline: get_deadline(timeout))
      end

      def insert(request, timeout: nil)
        @kv_stub.insert(request, deadline: get_deadline(timeout))
      end

      def upsert(request, timeout: nil)
        @kv_stub.upsert(request, deadline: get_deadline(timeout))
      end

      def replace(request, timeout: nil)
        @kv_stub.replace(request, deadline: get_deadline(timeout))
      end

      def remove(request, timeout: nil)
        @kv_stub.remove(request, deadline: get_deadline(timeout))
      end

      def increment(request, timeout: nil)
        @kv_stub.increment(request, deadline: get_deadline(timeout))
      end

      def decrement(request, timeout: nil)
        @kv_stub.decrement(request, deadline: get_deadline(timeout))
      end

      def append(request, timeout: nil)
        @kv_stub.append(request, deadline: get_deadline(timeout))
      end

      def prepend(request, timeout: nil)
        @kv_stub.prepend(request, deadline: get_deadline(timeout))
      end

      def lookup_in(request, timeout: nil)
        @kv_stub.lookup_in(request, deadline: get_deadline(timeout))
      end

      def mutate_in(request, timeout: nil)
        @kv_stub.mutate_in(request, deadline: get_deadline(timeout))
      end

      def range_scan(request, timeout: nil)
        @kv_stub.range_scan(request, deadline: get_deadline(timeout))
      end

      def query(request, timeout: nil)
        @query_stub.query(request, deadline: get_deadline(timeout))
      end

      def get_deadline(timeout_millis)
        if timeout_millis.nil?
          nil
        else
          Time.now + (timeout_millis * 1000)
        end
      end
    end
  end
end
