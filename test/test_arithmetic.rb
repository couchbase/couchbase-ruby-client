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

class TestArithmetic < MiniTest::Unit::TestCase

  def setup
    @mock = start_mock
  end

  def teardown
    stop_mock(@mock)
  end

  def test_trivial_incr_decr
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)

    connection.set(uniq_id, 1)
    val = connection.incr(uniq_id)
    assert_equal 2, val
    val = connection.get(uniq_id)
    assert_equal 2, val

    connection.set(uniq_id, 7)
    val = connection.decr(uniq_id)
    assert_equal 6, val
    val = connection.get(uniq_id)
    assert_equal 6, val
  end

  def test_it_fails_to_incr_decr_missing_key
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)

    assert_raises(Couchbase::Error::NotFound) do
      connection.incr(uniq_id(:missing))
    end
    assert_raises(Couchbase::Error::NotFound) do
      connection.decr(uniq_id(:missing))
    end
  end

  def test_it_creates_missing_key_when_initial_value_specified
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)

    val = connection.incr(uniq_id(:missing), :initial => 5)
    assert_equal 5, val
    val = connection.incr(uniq_id(:missing), :initial => 5)
    assert_equal 6, val
    val = connection.get(uniq_id(:missing))
    assert_equal 6, val
  end

  def test_it_uses_zero_as_default_value_for_missing_keys
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)

    val = connection.incr(uniq_id(:missing), :create => true)
    assert_equal 0, val
    val = connection.incr(uniq_id(:missing), :create => true)
    assert_equal 1, val
    val = connection.get(uniq_id(:missing))
    assert_equal 1, val
  end

  def test_it_allows_custom_ttl
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)

    val = connection.incr(uniq_id(:missing), :create => true, :ttl => 1)
    assert_equal 0, val
    val = connection.incr(uniq_id(:missing), :create => true)
    assert_equal 1, val
    sleep(2)
    refute connection.get(uniq_id(:missing))
  end

  def test_it_allows_custom_delta
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)

    connection.set(uniq_id, 12)
    val = connection.incr(uniq_id, 10)
    assert_equal 22, val
  end

end
