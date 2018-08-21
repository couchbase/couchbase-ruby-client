# Author:: Couchbase <info@couchbase.com>
# Copyright:: 2013-2018 Couchbase, Inc.
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

require File.join(__dir__, 'setup')
require 'couchbase/connection_pool'

class TestCouchbaseConnectionPool < MiniTest::Test
  def setup
    @pool = ::Couchbase::ConnectionPool.new(5, mock.connstr)
  end

  def test_basic_multithreaded_usage
    refute @pool.set('foo', 'bar').error

    threads = []
    15.times do
      threads << Thread.new do
        @pool.get('foo')
      end
    end

    result = threads.map(&:value)
    result.each do |res|
      refute res.error
      assert_equal 'bar', res.value
    end
  end

  def test_set_and_get
    refute @pool.set('fiz', 'buzz').error
    res = @pool.get('fiz')
    refute res.error
    assert_equal 'buzz', res.value
  end

  def test_set_and_delete
    refute @pool.set('baz', 'bar').error
    refute @pool.delete('baz').error
    res = @pool.get('baz')
    assert_instance_of Couchbase::LibraryError, res.error
    assert res.error.data?
    assert_equal Couchbase::LibraryError::LCB_KEY_ENOENT, res.error.code
  end

  def test_incr
    refute @pool.set('counter', 0).error
    refute @pool.incr('counter', 1).error
    res = @pool.get('counter')
    refute res.error
    assert_equal 1, res.value
  end

  def test_decr
    refute @pool.set('counter', 1).error
    refute @pool.decr('counter', 1).error
    res = @pool.get('counter')
    refute res.error
    assert_equal 0, res.value
  end
end
