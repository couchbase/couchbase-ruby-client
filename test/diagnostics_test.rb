# frozen_string_literal: true

#  Copyright 2024. Couchbase, Inc.
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
require_relative "test_helper"
module Couchbase
  class DiagnosticsTest < Minitest::Test
    include Couchbase::TestUtilities

    def setup
      skip("#{name}: The #{Couchbase::Protostellar::NAME} protocol does not support ping/diagnostics") if env.protostellar?

      connect

      @bucket = @cluster.bucket(env.bucket)
    end

    def teardown
      disconnect
    end

    def test_cluster_ping_multiple_services
      service_types = [:kv, :query, :search, :management]
      res = @cluster.ping(Options::Ping.new(service_types: service_types))

      assert_equal service_types.size, res.services.size
      service_types.each do |s|
        assert_includes res.services.keys, s
      end
    end

    def test_cluster_ping_single_service
      [:kv, :query, :search, :management].each do |service_type|
        res = @cluster.ping(Options::Ping.new(service_types: [service_type]))

        assert_equal 1, res.services.size
        assert_equal service_type, res.services.keys[0]
      end
    end

    def test_bucket_ping
      res = @bucket.ping

      assert_includes res.services.keys, :kv
      # Catches a ping collector that completes before the whole fan-out has been
      # dispatched (CXXCBC-1022). Comparing against a cluster ping keeps this
      # independent of the topology under test.
      assert_equal @cluster.ping.services.keys.sort, res.services.keys.sort
    end

    def test_bucket_ping_single_service
      service_types = @cluster.ping.services.keys

      assert_includes service_types, :kv
      service_types.each do |service_type|
        res = @bucket.ping(Options::Ping.new(service_types: [service_type]))

        assert_equal [service_type], res.services.keys
      end
    end
  end
end
