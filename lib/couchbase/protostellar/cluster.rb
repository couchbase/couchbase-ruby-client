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
require_relative "request_generator/query"
require_relative "response_converter/query"

require "couchbase/options"

module Couchbase
  module Protostellar
    class Cluster
      attr_reader :client

      def self.connect(connection_string, options = Couchbase::Options::Cluster.new)
        connect_options = ConnectOptions.new(username: options.authenticator.username,
                                             password: options.authenticator.password)
        Cluster.new(connection_string.split("://")[1], connect_options)
      end

      def initialize(connection_string, options = ConnectOptions.new)
        host = connection_string.include?(":") ? connection_string : "#{connection_string}:18098"
        credentials = options.grpc_credentials
        channel_args = options.grpc_channel_args
        call_metadata = options.grpc_call_metadata

        @client = Client.new(host, credentials, channel_args, call_metadata)
        @query_request_generator = RequestGenerator::Query.new
      end

      def disconnect
        @client.close
      end

      def bucket(name)
        Bucket.new(@client, name)
      end

      def query(statement, options = Couchbase::Options::Query::DEFAULT)
        req = @query_request_generator.query_request(statement, options)
        resps = @client.send_request(req)
        ResponseConverter::Query.to_query_result(resps)
      end
    end
  end
end
