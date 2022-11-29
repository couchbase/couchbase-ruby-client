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

module Couchbase
  module StellarNebula
    class Cluster
      def initialize(connection_string, options = ConnectOptions.new)
        host = connection_string.include?(":") ? connection_string : "#{connection_string}:18091"
        credentials = options.grpc_credentials
        channel_args = options.grpc_channel_args

        @client = Client.new(host, credentials, channel_args)
      end

      def close
        @client.close
      end

      def bucket(name)
        Bucket.new(@client, name)
      end
    end
  end
end
