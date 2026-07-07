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

require "fit/protocol/shared.cluster_pb"
require "fit/performer/connection"

module FIT
  module Performer
    class Connections
      def initialize
        @logger = Logger.new($stdout)
        @connections = {}
      end

      def create_connection(request)
        @connections[request.cluster_connection_id] = Connection.new(request)
        FIT::Protocol::Shared::ClusterConnectionCreateResponse.new(cluster_connection_count: count)
      end

      def close_connection(request)
        @connections[request.cluster_connection_id].close
        @connections.delete(request.cluster_connection_id)
        FIT::Protocol::Shared::ClusterConnectionCloseResponse.new(cluster_connection_count: count)
      end

      def disconnect_connections(_request)
        @connections.values.map(&:close)
        @connections.clear
        FIT::Protocol::Shared::DisconnectConnectionsResponse.new
      end

      def count
        @connections.size
      end

      def [](cluster_id)
        raise PerformerError, "Connection with ID #{cluster_id} not found" unless @connections.key?(cluster_id)

        @connections[cluster_id]
      end
    end
  end
end
