require File.join(File.dirname(__FILE__), 'setup')

class TestAsync < MiniTest::Unit::TestCase

  def setup
    @mock = start_mock
  end

  def teardown
    stop_mock(@mock)
  end

  def test_it_requires_block_for_running_loop
    connection = Couchbase.new(:port => @mock.port)
    refute connection.async?
    assert_raises(LocalJumpError) do
      connection.run
    end
    connection.run do |conn|
      assert conn.async?
    end
  end

  def test_it_resets_async_flag_when_raising_exception_from_callback
    connection = Couchbase.new(:port => @mock.port)

    assert_raises(RuntimeError) do
      connection.run do |conn|
        conn.set(test_id, "foo") { raise }
      end
    end
    refute connection.async?
  end

  def test_nested_async_get_set
    connection = Couchbase.new(:port => @mock.port)
    connection.set(test_id, {"bar" => 1})
    connection.set(test_id(:hit), 0)

    connection.run do |conn|
      conn.get(test_id) do |val, key|
        conn.get(test_id(:hit)) do |counter|
          conn.set(test_id(:hit), counter + 1)
        end
      end
    end

    val = connection.get(test_id(:hit))
    assert_equal 1, val
  end

  def test_nested_async_set_get
    connection = Couchbase.new(:port => @mock.port)
    val = nil

    connection.run do |conn|
      conn.set(test_id, "foo") do
        conn.get(test_id) do |v|
          val = v
        end
      end
    end

    assert_equal "foo", val
  end

  def test_nested_async_touch_get
    connection = Couchbase.new(:port => @mock.port)
    connection.set(test_id, "foo")
    success = false
    val = nil

    connection.run do |conn|
      conn.touch(test_id, :ttl => 1) do |k, res|
        success = res
        conn.get(test_id) do |v|
          val = v
        end
      end
    end

    assert success
    assert_equal "foo", val
    sleep(1)
    refute connection.get(test_id)
  end

  def test_nested_async_delete_get
    connection = Couchbase.new(:port => @mock.port)
    cas = connection.set(test_id, "foo")
    success = false
    val = :unknown

    connection.run do |conn|
      conn.delete(test_id, :cas => cas) do |k, res|
        success = res
        conn.get(test_id) do |v|
          val = v
        end
      end
    end

    assert success
    refute val
  end

  def test_nested_async_stats_set
    connection = Couchbase.new(:port => @mock.port)
    stats = {}

    connection.run do |conn|
      conn.stats do |host, key, val|
        id = test_id(host, key)
        stats[id] = false
        conn.set(id, val) do |cas|
          stats[id] = cas
        end
      end
    end

    stats.keys.each do |key|
      assert stats[key].is_a?(Numeric)
    end
  end

  def test_nested_async_flush_set
    connection = Couchbase.new(:port => @mock.port)
    cas = connection.set(test_id, "foo")
    res = {}

    connection.run do |conn|
      conn.flush do |host, ret|
        assert ret
        id = test_id(host)
        res[id] = false
        conn.set(id, true) do |cas|
          res[id] = cas
        end
      end
    end

    refute connection.get(test_id)
    res.keys.each do |key|
      assert res[key].is_a?(Numeric)
      assert connection.get(key)
    end
  end

  def test_nested_async_incr_get
    connection = Couchbase.new(:port => @mock.port)
    cas = connection.set(test_id, 1)
    val = nil

    connection.run do |conn|
      conn.incr(test_id) do
        conn.get(test_id) do |v|
          val = v
        end
      end
    end

    assert_equal 2, val
  end

  def test_it_doesnt_accept_callbacks_in_synchronous_mode
    connection = Couchbase.new(:port => @mock.port)
    refute connection.async?

    assert_raises(ArgumentError) { connection.add(test_id, "foo") {} }
    assert_raises(ArgumentError) { connection.set(test_id, "foo") {} }
    assert_raises(ArgumentError) { connection.replace(test_id, "foo") {} }
    assert_raises(ArgumentError) { connection.get(test_id) {} }
    assert_raises(ArgumentError) { connection.touch(test_id) {} }
    assert_raises(ArgumentError) { connection.incr(test_id) {} }
    assert_raises(ArgumentError) { connection.decr(test_id) {} }
    assert_raises(ArgumentError) { connection.delete(test_id) {} }
    assert_raises(ArgumentError) { connection.append(test_id, "bar") {} }
    assert_raises(ArgumentError) { connection.prepend(test_id, "bar") {} }
    assert_raises(ArgumentError) { connection.flush {} }
    assert_raises(ArgumentError) { connection.stats {} }
  end

end
