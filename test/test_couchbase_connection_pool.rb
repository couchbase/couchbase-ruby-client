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
require 'couchbase/connection_pool'

class TestCouchbaseRailsCacheStore < MiniTest::Unit::TestCase

  def setup
    @mock = start_mock
    @pool = ::Couchbase::ConnectionPool.new(5, :hostname => @mock.host, :port => @mock.port)
  end

  def teardown
    stop_mock(@mock)
  end

  def test_basic_multithreaded_usage
    @pool.set('foo', 'bar')

    threads = []
    15.times do
      threads << Thread.new do
        @pool.get('foo')
      end
    end

    result = threads.map(&:value)
    result.each do |val|
      assert_equal 'bar', val
    end
  end

  def test_set_and_get
    @pool.set('fiz', 'buzz')
    assert_equal 'buzz', @pool.get('fiz')
  end

  def test_set_and_delete
    @pool.set('baz', 'bar')
    @pool.delete('baz')
    assert_raises Couchbase::Error::NotFound do
      @pool.get('baz')
    end
  end

  def test_incr
    @pool.set('counter', 0)
    @pool.incr('counter', 1)
    assert_equal 1, @pool.get('counter')
  end

  def test_decr
    @pool.set('counter', 1)
    @pool.decr('counter', 1)
    assert_equal 0, @pool.get('counter')
  end

end