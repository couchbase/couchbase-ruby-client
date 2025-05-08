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

require "couchbase/options"

module Couchbase
  module Protostellar
    class Cluster
      VALID_CONNECTION_STRING_PARAMS = [
        "trust_certificate",
      ].freeze

      attr_reader :client

      # Connect to the Couchbase cluster
      #
      # @overload connect(connection_string, options)
      #   @param [String] connection_string connection string used to locate the Couchbase Cluster
      #   @param [Options::Cluster] options custom options when creating the cluster connection
      #
      # @overload connect(connection_string, username, password, options)
      #   Shortcut for {PasswordAuthenticator}
      #   @param [String] connection_string connection string used to locate the Couchbase Cluster
      #   @param [String] username name of the user
      #   @param [String] password password of the user
      #   @param [Options::Cluster, nil] options custom options when creating the cluster connection
      #
      # @overload connect(configuration, options)
      #   @param [Configuration] configuration configuration object
      #   @param [Options::Cluster, nil] options custom options when creating the cluster connection
      def self.connect(connection_string_or_config, *args)
        require_relative "connect_options"
        require_relative "bucket"
        require_relative "client"
        require_relative "timeouts"
        require_relative "management/bucket_manager"
        require_relative "request_generator/query"
        require_relative "request_generator/search"
        require_relative "response_converter/query"
        require_relative "response_converter/search"
        require_relative "management/query_index_manager"

        connection_string = nil
        username = nil
        password = nil
        options = nil

        if connection_string_or_config.is_a?(Couchbase::Configuration)
          connection_string = connection_string_or_config.connection_string
          username = connection_string_or_config.username
          password = connection_string_or_config.password
          options = args[0] || Couchbase::Options::Cluster.new
        else
          connection_string = connection_string_or_config
          case args[0]
          when String
            username = args[0]
            password = args[1]
            options = args[2] || Couchbase::Options::Cluster.new
          when Couchbase::Options::Cluster
            options = args[0]
            case options.authenticator
            when Couchbase::PasswordAuthenticator
              username = options.authenticator.username
              password = options.authenticator.password
            when Couchbase::CertificateAuthenticator
              raise Couchbase::Error::FeatureNotAvailable,
                    "The #{Couchbase::Protostellar::NAME} protocol does not support the CertificateAuthenticator"
            else
              raise ArgumentError, "options must have authenticator configured"
            end
          else
            raise ArgumentError, "unexpected second argument, have to be String or Couchbase::Options::Cluster"
          end
        end

        raise ArgumentError, "missing username" unless username
        raise ArgumentError, "missing password" unless password

        params = parse_connection_string_params(connection_string)
        connect_options = ConnectOptions.new(
          username: username,
          password: password,
          timeouts: Protostellar::Timeouts.from_cluster_options(options),
          root_certificates: params.key?("trust_certificate") ? File.read(params["trust_certificate"]) : nil,
        )
        new(connection_string.split("://")[1].split("?")[0], connect_options)
      end

      def self.parse_connection_string_params(connection_string)
        params =
          if connection_string.include?("?")
            connection_string.split("?")[1].split("&").to_h { |p| p.split("=") }
          else
            {}
          end

        # Show warnings for the connection string parameters that are not supported
        params.each do |k, v|
          warn "Unsupported parameter '#{k}' in connection string (value '#{v}')" unless VALID_CONNECTION_STRING_PARAMS.include?(k)
        end

        params
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

      def query_indexes
        Management::QueryIndexManager.new(client: @client)
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

      def diagnostics(_options = Options::Diagnostics::DEFAULT)
        raise Couchbase::Error::FeatureNotAvailable, "The #{Protostellar::NAME} protocol does not support diagnostics"
      end

      def ping(_options = Options::Ping::DEFAULT)
        raise Couchbase::Error::FeatureNotAvailable, "The #{Protostellar::NAME} protocol does not support ping"
      end

      private

      def initialize(host, options = ConnectOptions.new)
        @client = Client.new(
          host: host.include?(":") ? host : "#{host}:18098",
          credentials: options.grpc_credentials,
          channel_args: options.grpc_channel_args,
          call_metadata: options.grpc_call_metadata,
          timeouts: options.timeouts,
        )

        @query_request_generator = RequestGenerator::Query.new
        @search_request_generator = RequestGenerator::Search.new
      end
    end
  end
end
