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

require "base64"

require_relative "timeouts"

module Couchbase
  module Protostellar
    class ConnectOptions
      attr_reader :timeouts

      def initialize(timeouts: Timeouts.new,
                     username: nil,
                     password: nil,
                     root_certificates: nil,
                     client_certificate: nil,
                     private_key: nil)
        @timeouts = timeouts
        @username = username
        @password = password
        @root_certificates = root_certificates
        @client_certificate = client_certificate
        @private_key = private_key
      end

      def grpc_credentials
        if @root_certificates
          GRPC::Core::ChannelCredentials.new(@root_certificates, @client_certificate, @private_key)
        else
          # Use default certificates
          GRPC::Core::ChannelCredentials.new(nil, @client_certificate, @private_key)
        end
      end

      def grpc_call_metadata
        if @username && @password
          {"authorization" => ["Basic #{Base64.strict_encode64("#{@username}:#{@password}")}"]}
        else
          {}
        end
      end

      def grpc_channel_args
        {}
      end
    end
  end
end
