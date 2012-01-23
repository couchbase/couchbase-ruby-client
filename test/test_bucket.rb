# Author:: Couchbase <info@couchbase.com>
# Copyright:: 2011, 2012 Couchbase, Inc.
# License:: Apache License, Version 2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

require File.join(File.dirname(__FILE__), 'setup')

class TestBucket < MiniTest::Unit::TestCase

  def test_it_substitute_default_parts_to_url
    with_mock(:port => 8091, :buckets_spec => 'default,foo') do
      connections = [
        Couchbase.new,
        Couchbase.new('http://localhost'),
        Couchbase.new('http://localhost:8091'),
        Couchbase.new('http://localhost:8091/pools/default'),
        Couchbase.new(:hostname => 'localhost'),
        Couchbase.new(:hostname => 'localhost', :port => 8091)
      ]
      connections.each do |connection|
        assert_equal 'localhost', connection.hostname
        assert_equal 8091, connection.port
        assert_equal "localhost:8091", connection.authority
        assert_equal 'default', connection.bucket
        assert_equal 'http://localhost:8091/pools/default/buckets/default/', connection.url
      end

      connections = [
        Couchbase.new('http://localhost:8091/pools/default/buckets/foo'),
        Couchbase.new(:bucket => 'foo'),
        Couchbase.new('http://localhost:8091/pools/default/buckets/default', :bucket => 'foo')
      ]
      connections.each do |connection|
        assert_equal 'foo', connection.bucket
        assert_equal 'http://localhost:8091/pools/default/buckets/foo/', connection.url
      end
    end

    with_mock do |mock| # pick first free port
      connections = [
        Couchbase.new("http://localhost:#{mock.port}"),
        Couchbase.new(:port => mock.port),
        Couchbase.new('http://localhost:8091', :port => mock.port)
      ]
      connections.each do |connection|
        assert_equal mock.port, connection.port
        assert_equal "localhost:#{mock.port}", connection.authority
        assert_equal "http://localhost:#{mock.port}/pools/default/buckets/default/", connection.url
      end
    end

    with_mock(:host => '127.0.0.1') do |mock|
      connections = [
        Couchbase.new("http://127.0.0.1:#{mock.port}"),
        Couchbase.new(:hostname => '127.0.0.1', :port => mock.port),
        Couchbase.new('http://localhost:8091', :hostname => '127.0.0.1', :port => mock.port)
      ]
      connections.each do |connection|
        assert_equal '127.0.0.1', connection.hostname
        assert_equal "127.0.0.1:#{mock.port}", connection.authority
        assert_equal "http://127.0.0.1:#{mock.port}/pools/default/buckets/default/", connection.url
      end
    end
  end

  def test_it_raises_network_error_if_server_not_found
    refute(`netstat -tnl` =~ /12345/)
    assert_raises Couchbase::Error::Network do
      Couchbase.new(:port => 12345)
    end
  end

  def test_it_raises_argument_error_for_illegal_url
    illegal = [
      "ftp://localhost:8091/",
      "http:/localhost:8091/",
      ""
    ]
    illegal.each do |url|
      assert_raises ArgumentError do
        Couchbase.new(url)
      end
    end
  end

  def test_it_able_to_connect_to_protected_buckets
    with_mock(:buckets_spec => 'protected:secret') do |mock|
      connection = Couchbase.new(:port => mock.port,
                                 :bucket => 'protected',
                                 :username => 'protected',
                                 :password => 'secret')
      assert_equal "protected", connection.bucket
      assert_equal "protected", connection.username
      assert_equal "secret", connection.password
    end
  end

  def test_it_raises_error_with_wrong_credentials
    with_mock(:buckets_spec => 'protected:secret') do |mock|
      assert_raises Couchbase::Error::Protocol do
        Couchbase.new(:port => mock.port,
                      :bucket => 'protected',
                      :username => 'wrong',
                      :password => 'secret')
      end
      assert_raises Couchbase::Error::Protocol do
        Couchbase.new(:port => mock.port,
                      :bucket => 'protected',
                      :username => 'protected',
                      :password => 'wrong')
      end
    end
  end

  def test_it_allows_change_quiet_flag
    with_mock do |mock|
      connection = Couchbase.new(:port => mock.port)
      assert connection.quiet?

      connection = Couchbase.new(:port => mock.port, :quiet => true)
      assert connection.quiet?

      connection.quiet = nil
      assert_equal false, connection.quiet?

      connection.quiet = :foo
      assert_equal true, connection.quiet?
    end
  end
end
