# Author:: Couchbase <info@couchbase.com>
# Copyright:: 2011-2017 Couchbase, Inc.
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

class TestTouch < MiniTest::Test
  def setup
    @mock = start_mock
  end

  def teardown
    stop_mock(@mock)
  end

  def test_trivial_touch
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)
    connection.set(uniq_id, "bar", :ttl => 1)
    connection.touch(uniq_id, :ttl => 2)
    sleep(1)
    assert connection.get(uniq_id)
    sleep(2)
    assert_raises(Couchbase::Error::NotFound) do
      connection.get(uniq_id)
    end
  end

  def test_multi_touch
    connection = Couchbase.new(:port => @mock.port)
    connection.set(uniq_id(1), "bar")
    connection.set(uniq_id(2), "baz")
    ret = connection.touch(uniq_id(1) => 1, uniq_id(2) => 1)
    assert ret[uniq_id(1)]
    assert ret[uniq_id(2)]
    sleep(2)
    assert_raises(Couchbase::Error::NotFound) do
      connection.get(uniq_id(1))
    end
    assert_raises(Couchbase::Error::NotFound) do
      connection.get(uniq_id(2))
    end
  end

  def test_it_uses_default_ttl_for_touch
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port, :default_ttl => 1)
    connection.set(uniq_id, "bar", :ttl => 10)
    connection.touch(uniq_id)
    sleep(2)
    assert_raises(Couchbase::Error::NotFound) do
      connection.get(uniq_id)
    end
  end

  def test_it_accepts_ttl_for_get_command
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)
    connection.set(uniq_id, "bar", :ttl => 10)
    val = connection.get(uniq_id, :ttl => 1)
    assert_equal "bar", val
    sleep(2)
    assert_raises(Couchbase::Error::NotFound) do
      connection.get(uniq_id)
    end
  end

  def test_missing_in_quiet_mode
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port, :quiet => true)
    cas1 = connection.set(uniq_id(1), "foo1")
    cas2 = connection.set(uniq_id(2), "foo2")

    assert_raises(Couchbase::Error::NotFound) do
      connection.touch(uniq_id(:missing), :quiet => false)
    end

    val = connection.touch(uniq_id(:missing))
    refute(val)

    ret = connection.touch(uniq_id(1), uniq_id(:missing), uniq_id(2))
    assert_equal true, ret[uniq_id(1)]
    assert_equal false, ret[uniq_id(:missing)]
    assert_equal true, ret[uniq_id(2)]
  end
end
