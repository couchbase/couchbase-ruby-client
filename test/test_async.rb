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

class TestAsync < MiniTest::Unit::TestCase

  def setup
    @mock = start_mock
  end

  def teardown
    stop_mock(@mock)
  end

  def test_result_object_provides_enough_info
    obj = Couchbase::Result.new
    assert obj.respond_to?(:success?)
    assert obj.respond_to?(:error)
    assert obj.respond_to?(:key)
    assert obj.respond_to?(:value)
    assert obj.respond_to?(:node)
    assert obj.respond_to?(:cas)
    assert obj.respond_to?(:flags)
  end

  def test_it_requires_block_for_running_loop
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)
    refute connection.async?
    assert_raises(LocalJumpError) do
      connection.run
    end
    connection.run do |conn|
      assert conn.async?
    end
  end

  def test_it_resets_async_flag_when_raising_exception_from_callback
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)

    assert_raises(RuntimeError) do
      connection.run do |conn|
        conn.set(uniq_id, "foo") { raise }
      end
    end
    refute connection.async?
  end

  def test_nested_async_get_set
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)
    connection.set(uniq_id, {"bar" => 1})
    connection.set(uniq_id(:hit), 0)

    connection.run do |conn|
      conn.get(uniq_id) do
        conn.get(uniq_id(:hit)) do |res|
          conn.set(uniq_id(:hit), res.value + 1)
        end
      end
    end

    val = connection.get(uniq_id(:hit))
    assert_equal 1, val
  end

  def test_nested_async_set_get
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)
    val = nil

    connection.run do |conn|
      conn.set(uniq_id, "foo") do
        conn.get(uniq_id) do |res|
          val = res.value
        end
      end
    end

    assert_equal "foo", val
  end

  def test_nested_async_touch_get
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)
    connection.set(uniq_id, "foo")
    success = false
    val = nil

    connection.run do |conn|
      conn.touch(uniq_id, :ttl => 1) do |res1|
        success = res1.success?
        conn.get(uniq_id) do |res2|
          val = res2.value
        end
      end
    end

    assert success
    assert_equal "foo", val
    sleep(2)
    assert_raises(Couchbase::Error::NotFound) do
      connection.get(uniq_id)
    end
  end

  def test_nested_async_delete_get
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)
    cas = connection.set(uniq_id, "foo")
    success = false
    val = :unknown

    connection.run do |conn|
      conn.delete(uniq_id, :cas => cas) do |res1|
        success = res1.success?
        conn.get(uniq_id) do |res2|
          val = res2.value
        end
      end
    end

    assert success
    refute val
  end

  def test_nested_async_stats_set
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)
    stats = {}

    connection.run do |conn|
      conn.stats do |res1|
        id = uniq_id(res1.node, res1.key)
        stats[id] = false
        conn.set(id, res1.value) do |res2|
          stats[id] = res2.cas
        end
      end
    end

    stats.keys.each do |key|
      assert stats[key].is_a?(Numeric)
    end
  end

  def test_nested_async_flush_set
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)
    cas = connection.set(uniq_id, "foo")
    res = {}

    connection.run do |conn|
      conn.flush do |res1|
        assert res1.success?
        id = uniq_id(res1.node)
        res[id] = false
        conn.set(id, true) do |res2|
          res[id] = res2.cas
        end
      end
    end

    assert_raises(Couchbase::Error::NotFound) do
      connection.get(uniq_id)
    end
    res.keys.each do |key|
      assert res[key].is_a?(Numeric)
      assert connection.get(key)
    end
  end

  def test_nested_async_incr_get
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)
    cas = connection.set(uniq_id, 1)
    val = nil

    connection.run do |conn|
      conn.incr(uniq_id) do
        conn.get(uniq_id) do |res|
          val = res.value
        end
      end
    end

    assert_equal 2, val
  end

  def test_it_doesnt_accept_callbacks_in_synchronous_mode
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)
    refute connection.async?

    assert_raises(ArgumentError) { connection.add(uniq_id, "foo") {} }
    assert_raises(ArgumentError) { connection.set(uniq_id, "foo") {} }
    assert_raises(ArgumentError) { connection.replace(uniq_id, "foo") {} }
    assert_raises(ArgumentError) { connection.get(uniq_id) {} }
    assert_raises(ArgumentError) { connection.touch(uniq_id) {} }
    assert_raises(ArgumentError) { connection.incr(uniq_id) {} }
    assert_raises(ArgumentError) { connection.decr(uniq_id) {} }
    assert_raises(ArgumentError) { connection.delete(uniq_id) {} }
    assert_raises(ArgumentError) { connection.append(uniq_id, "bar") {} }
    assert_raises(ArgumentError) { connection.prepend(uniq_id, "bar") {} }
    assert_raises(ArgumentError) { connection.flush {} }
    assert_raises(ArgumentError) { connection.stats {} }
  end

  def test_it_disallow_nested_run
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)
    assert_raises(Couchbase::Error::Invalid) do
      connection.run do
        connection.run do
        end
      end
    end
  end

  def test_it_extends_timeout_in_async_mode_if_needed
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)
    connection.set(uniq_id, "foo")

    connection.timeout = 100_000  # 100_000 us
    connection.run do
      connection.get(uniq_id) do |ret|
        assert ret.success?
        assert_equal "foo", ret.value
      end
      sleep(1.5)  # 1_500_000 us
    end
  end

  def test_send_threshold
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)

    sent = false
    connection.run(:send_threshold => 100) do # 100 bytes
      connection.set(uniq_id, "foo" * 100) {|r| sent = true}
      assert sent
    end
  end

end
