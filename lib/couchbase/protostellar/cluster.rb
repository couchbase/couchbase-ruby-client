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

require_relative "connect_options"
require_relative "bucket"
require_relative "client"
require_relative "timeouts"
require_relative "management/bucket_manager"
require_relative "request_generator/query"
require_relative "request_generator/search"
require_relative "response_converter/query"
require_relative "response_converter/search"

require "couchbase/options"

module Couchbase
  module Protostellar
    class Cluster
      attr_reader :client

      def self.connect(connection_string, options = Couchbase::Options::Cluster.new)
        params = connection_string.split("?")[1].split("&").to_h { |p| p.split("=") }

        connect_options = ConnectOptions.new(
          username: options.authenticator.username,
          password: options.authenticator.password,
          timeouts: Protostellar::Timeouts.from_cluster_options(options),
          root_certificates: File.read(params["trust_certificate"])
        )
        Cluster.new(connection_string.split("://")[1].split("?")[0], connect_options)
      end

      def initialize(host, options = ConnectOptions.new)
        @client = Client.new(
          host: host.include?(":") ? host : "#{host}:18098",
          credentials: options.grpc_credentials,
          channel_args: options.grpc_channel_args,
          call_metadata: options.grpc_call_metadata,
          timeouts: options.timeouts
        )

        @query_request_generator = RequestGenerator::Query.new
        @search_request_generator = RequestGenerator::Search.new
      end

      def disconnect
        @client.close
      end

      def bucket(name)
        Bucket.new(@client, name)
      end

      def buckets
        Management::BucketManager.new(@client)
      end

      def query(statement, options = Couchbase::Options::Query::DEFAULT)
        req = @query_request_generator.query_request(statement, options)
        resps = @client.send_request(req)
        ResponseConverter::Query.to_query_result(resps)
      end

      def search_query(index_name, query, options = Couchbase::Options::Search::DEFAULT)
        req = @search_request_generator.search_query_request(index_name, query, options)
        resp = @client.send_request(req)
        ResponseConverter::Search.to_search_result(resp, options)
      end
    end
  end
end
