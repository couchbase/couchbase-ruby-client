# frozen_string_literal: true

#  Copyright 2020-2021 Couchbase, Inc.
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

module Couchbase
  class DiagnosticsResult
    class ServiceInfo
      # @return [String] endpoint unique identifier
      attr_accessor :id

      # Possible states are:
      #
      # :disconnected:: the endpoint is not reachable
      # :connecting:: currently connecting (includes auth, handshake, etc.)
      # :connected:: connected and ready
      # :disconnecting:: disconnecting (after being connected)
      #
      # @return [Symbol] state of the endpoint
      attr_accessor :state

      # @return [String, nil] optional string with additional explanation for the state
      attr_accessor :details

      # @return [Integer] how long ago the endpoint was active (in microseconds)
      attr_accessor :last_activity_us

      # @return [String] remote address of the connection
      attr_accessor :remote

      # @return [String] local address of the connection
      attr_accessor :local

      # @yieldparam [ServiceInfo] self
      def initialize
        yield self if block_given?
      end

      def to_json(*args)
        data = {
          id: @id,
          state: @state,
          remote: @remote,
          local: @local,
        }
        data[:details] = @details if @details
        data[:last_activity_us] = @last_activity_us if @last_activity_us
        data.to_json(*args)
      end
    end

    # @return [String] report id
    attr_accessor :id

    # @return [String] SDK identifier
    attr_accessor :sdk

    # Returns information about currently service endpoints, that known to the library at the moment.
    #
    # :kv:: Key/Value data service
    # :query:: N1QL query service
    # :analytics:: Analtyics service
    # :search:: Full text search service
    # :views:: Views service
    # :mgmt:: Management service
    #
    # @return [Hash<Symbol, ServiceInfo>] map service types to info
    attr_accessor :services

    # @yieldparam [DiagnosticsResult] self
    def initialize
      @services = {}
      yield self if block_given?
    end

    # @api private
    # @return [Integer] version
    attr_accessor :version

    def to_json(*args)
      {
        version: @version,
        id: @id,
        sdk: @sdk,
        services: @services,
      }.to_json(*args)
    end
  end

  class PingResult
    class ServiceInfo
      # @return [String] endpoint unique identifier
      attr_accessor :id

      # Possible states are:
      #
      # :ok:: endpoint is healthy
      # :timeout:: endpoint didn't respond in time
      # :error:: request to endpoint has failed, see {#error} for additional details
      #
      # @return [Symbol] state of the endpoint
      attr_accessor :state

      # @return [String, nil] optional string with additional explanation for the state
      attr_accessor :error

      # @return [Integer] how long ago the endpoint was active (in microseconds)
      attr_accessor :latency

      # @return [String] remote address of the connection
      attr_accessor :remote

      # @return [String] local address of the connection
      attr_accessor :local

      # @yieldparam [ServiceInfo] self
      def initialize
        @error = nil
        yield self if block_given?
      end

      def to_json(*args)
        data = {
          id: @id,
          state: @state,
          remote: @remote,
          local: @local,
          latency: @latency,
        }
        data[:error] = @error if @error
        data.to_json(*args)
      end
    end

    # @return [String] report id
    attr_accessor :id

    # @return [String] SDK identifier
    attr_accessor :sdk

    # Returns information about currently service endpoints, that known to the library at the moment.
    #
    # :kv:: Key/Value data service
    # :query:: N1QL query service
    # :analytics:: Analtyics service
    # :search:: Full text search service
    # :views:: Views service
    # :mgmt:: Management service
    #
    # @return [Hash<Symbol, ServiceInfo>] map service types to info
    attr_accessor :services

    # @yieldparam [DiagnosticsResult] self
    def initialize
      @services = {}
      yield self if block_given?
    end

    # @api private
    # @return [Integer] version
    attr_accessor :version

    def to_json(*args)
      {
        version: @version,
        id: @id,
        sdk: @sdk,
        services: @services,
      }.to_json(*args)
    end
  end
end
