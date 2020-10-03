#    Copyright 2020 Couchbase, Inc.
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

# rubocop:disable Layout/LineLength

module Couchbase
  class ConnstrTest < Minitest::Test
    include TestUtilities

    def test_parse
      expected = {
        scheme: "couchbase",
        tls: false,
        nodes: [
          {address: "localhost", type: :dns, port: 8091, mode: :http},
          {address: "127.0.0.1", type: :ipv4, mode: :gcccp},
        ],
        params: {
          "foo" => "bar",
          "baz" => "",
        },
        default_bucket_name: "default",
        default_port: 11210,
        default_mode: :gcccp,
      }
      assert_equal(expected, Couchbase::Backend.parse_connection_string("couchbase://localhost:8091=http;127.0.0.1=mcd/default?foo=bar&baz="))

      assert_equal 'couchbase', Couchbase::Backend.parse_connection_string('couchbase://127.0.0.1')[:scheme]
      assert_equal 'http', Couchbase::Backend.parse_connection_string('http://127.0.0.1')[:scheme]
      assert_equal 'couchbase', Couchbase::Backend.parse_connection_string('couchbase://')[:scheme]
      assert_equal 'memcached', Couchbase::Backend.parse_connection_string('memcached://127.0.0.1')[:scheme]
      assert_equal(['1.2.3.4'], Couchbase::Backend.parse_connection_string('couchbase://1.2.3.4')[:nodes].map { |node| node[:address] })
      assert_equal(['123.123.12.4'], Couchbase::Backend.parse_connection_string('couchbase://123.123.12.4')[:nodes].map { |node| node[:address] })
      assert_equal(['231.1.1.1'], Couchbase::Backend.parse_connection_string('couchbase://231.1.1.1')[:nodes].map { |node| node[:address] })
      assert_equal(['255.1.1.1'], Couchbase::Backend.parse_connection_string('couchbase://255.1.1.1')[:nodes].map { |node| node[:address] })
      assert_equal(['::ffff:13.15.49.232'], Couchbase::Backend.parse_connection_string('couchbase://[::ffff:13.15.49.232]')[:nodes].map { |node| node[:address] })
      assert_equal(['::'], Couchbase::Backend.parse_connection_string('couchbase://[::]')[:nodes].map { |node| node[:address] })
      assert_equal(['::1'], Couchbase::Backend.parse_connection_string('couchbase://[::1]')[:nodes].map { |node| node[:address] })
      assert_equal(['2001:db8::1'], Couchbase::Backend.parse_connection_string('couchbase://[2001:db8::1]')[:nodes].map { |node| node[:address] })
      assert_equal(['2001:db8:85a3:8d3:1319:8a2e:370:7348'], Couchbase::Backend.parse_connection_string('couchbase://[2001:db8:85a3:8d3:1319:8a2e:370:7348]')[:nodes].map { |node| node[:address] })
      assert_equal(['example.com'], Couchbase::Backend.parse_connection_string('couchbase://example.com')[:nodes].map { |node| node[:address] })

      assert_equal "failed to parse connection string: empty input", Couchbase::Backend.parse_connection_string('')[:error]
      assert_equal 'failed to parse connection string (column: 15, trailer: "6.1.1.1")', Couchbase::Backend.parse_connection_string('couchbase://256.1.1.1')[:error]
      assert_equal 'failed to parse connection string (column: 15, trailer: "1.1.1.1")', Couchbase::Backend.parse_connection_string('couchbase://321.1.1.1')[:error]

      assert_equal 'failed to parse connection string (column: 16, trailer: "1:db8:85a3:8d3:1319:8a2e:370:7348")', Couchbase::Backend.parse_connection_string('couchbase://2001:db8:85a3:8d3:1319:8a2e:370:7348')[:error]
      assert_equal 'failed to parse connection string (column: 47, trailer: ":7348]")', Couchbase::Backend.parse_connection_string('couchbase://[2001:1:db8:85a3:8d3:1319:8a2e:370:7348]')[:error]
      assert_equal 'failed to parse connection string (column: 14, trailer: ":13.15.49.232]")', Couchbase::Backend.parse_connection_string('couchbase://[:13.15.49.232]')[:error]

      assert_equal(%w[1.2.3.4 4.3.2.1], Couchbase::Backend.parse_connection_string('couchbase://1.2.3.4,4.3.2.1')[:nodes].map { |node| node[:address] })
      assert_equal(%w[1.2.3.4 4.3.2.1], Couchbase::Backend.parse_connection_string('couchbase://1.2.3.4;4.3.2.1')[:nodes].map { |node| node[:address] })
      assert_equal(%w[2001:db8::1 123.123.12.4], Couchbase::Backend.parse_connection_string('couchbase://[2001:db8::1];123.123.12.4')[:nodes].map { |node| node[:address] })
      assert_equal(%w[example.com ::1 127.0.0.1], Couchbase::Backend.parse_connection_string('couchbase://example.com,[::1];127.0.0.1')[:nodes].map { |node| node[:address] })

      assert_equal([nil, 11210], Couchbase::Backend.parse_connection_string('couchbase://1.2.3.4,4.3.2.1:11210')[:nodes].map { |node| node[:port] })
      assert_equal([8091, nil], Couchbase::Backend.parse_connection_string('couchbase://1.2.3.4:8091;4.3.2.1')[:nodes].map { |node| node[:port] })
      assert_equal([18091, nil], Couchbase::Backend.parse_connection_string('couchbase://[2001:db8::1]:18091;123.123.12.4')[:nodes].map { |node| node[:port] })
      assert_equal([nil, 11211], Couchbase::Backend.parse_connection_string('couchbase://1.2.3.4,4.3.2.1:11211')[:nodes].map { |node| node[:port] })
      assert_equal([123, 456, 789], Couchbase::Backend.parse_connection_string('couchbase://example.com:123,[::1]:456;127.0.0.1:789')[:nodes].map { |node| node[:port] })

      assert_equal([nil, :gcccp], Couchbase::Backend.parse_connection_string('couchbase://1.2.3.4,4.3.2.1=MCD')[:nodes].map { |node| node[:mode] })
      assert_equal([:http, nil], Couchbase::Backend.parse_connection_string('couchbase://1.2.3.4:8091=http;4.3.2.1')[:nodes].map { |node| node[:mode] })
      assert_equal([:http, :gcccp], Couchbase::Backend.parse_connection_string('couchbase://1.2.3.4:8091=http;4.3.2.1=mcd')[:nodes].map { |node| node[:mode] })
      assert_equal([:gcccp, nil], Couchbase::Backend.parse_connection_string('couchbase://[2001:db8::1]:18091=mcd;123.123.12.4')[:nodes].map { |node| node[:mode] })
      assert_equal([:gcccp, nil, :http], Couchbase::Backend.parse_connection_string('couchbase://example.com=McD,[::1];127.0.0.1=Http')[:nodes].map { |node| node[:mode] })

      assert_equal 'bucket', Couchbase::Backend.parse_connection_string('couchbase://127.0.0.1/bucket')[:default_bucket_name]
      assert_equal 'bUcKeT', Couchbase::Backend.parse_connection_string('couchbase://127.0.0.1/bUcKeT')[:default_bucket_name]
      assert_equal 'bU%1F-K__big__.mp3', Couchbase::Backend.parse_connection_string('couchbase://127.0.0.1/bU%1F-K__big__.mp3')[:default_bucket_name]
      assert_nil Couchbase::Backend.parse_connection_string('couchbase://127.0.0.1')[:default_bucket_name]
      assert_equal 'failed to parse connection string (column: 29, trailer: "/foo")', Couchbase::Backend.parse_connection_string('couchbase://127.0.0.1/bucket/foo')[:error]

      assert_equal({"foo" => "bar", "log_level" => "4"}, Couchbase::Backend.parse_connection_string("couchbase://127.0.0.1/bucket?foo=bar&log_level=4")[:params])
      assert_equal({"foo" => "bar", "log_level" => "4"}, Couchbase::Backend.parse_connection_string("couchbase://127.0.0.1?foo=bar&log_level=4")[:params])
      assert_equal({"path" => "/foo/bar/baz"}, Couchbase::Backend.parse_connection_string('couchbase://127.0.0.1?path=/foo/bar/baz')[:params])
    end
  end
end

# rubocop:enable Layout/LineLength
