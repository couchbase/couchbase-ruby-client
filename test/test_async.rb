require File.join(File.dirname(__FILE__), 'setup')

class TestAsync < MiniTest::Unit::TestCase

  def setup
    @mock = start_mock
  end

  def teardown
    stop_mock(@mock)
  end

  def test_nested_async_get_set
    connection = Couchbase.new(:port => @mock.port)
    connection.set(test_id, {"bar" => 1})
    connection.set(test_id(:hit), 0)

    connection.async = true
    connection.get(test_id) do |val, key|
      connection.get(test_id(:hit)) do |counter|
        connection.set(test_id(:hit), counter + 1)
      end
    end
    connection.run

    connection.async = false
    val = connection.get(test_id(:hit))
    assert_equal 1, val
  end

  def test_nested_async_set_get
    connection = Couchbase.new(:port => @mock.port)
    val = nil

    connection.async = true
    connection.set(test_id, "foo") do
      connection.get(test_id) do |v|
        val = v
      end
    end
    connection.run

    connection.async = false
    assert_equal "foo", val
  end

  def test_nested_async_touch_get
    connection = Couchbase.new(:port => @mock.port)
    connection.set(test_id, "foo")
    success = false
    val = nil

    connection.async = true
    connection.touch(test_id, :ttl => 1) do |k, res|
      success = res
      connection.get(test_id) do |v|
        val = v
      end
    end
    connection.run
    connection.async = false

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

    connection.async = true
    connection.delete(test_id, :cas => cas) do |k, res|
      success = res
      connection.get(test_id) do |v|
        val = v
      end
    end
    connection.run
    connection.async = false

    assert success
    refute val
  end

  def test_nested_async_stats_set
    connection = Couchbase.new(:port => @mock.port)
    stats = {}

    connection.async = true
    connection.stats do |host, key, val|
      id = test_id(host, key)
      stats[id] = false
      connection.set(id, val) do |cas|
        stats[id] = cas
      end
    end
    connection.run
    connection.async = false

    stats.keys.each do |key|
      assert stats[key].is_a?(Numeric)
    end
  end

  def test_nested_async_flush_set
    connection = Couchbase.new(:port => @mock.port)
    cas = connection.set(test_id, "foo")
    res = {}

    connection.async = true
    connection.flush do |host, ret|
      assert ret
      id = test_id(host)
      res[id] = false
      connection.set(id, true) do |cas|
        res[id] = cas
      end
    end
    connection.run
    connection.async = false

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

    connection.async = true
    connection.incr(test_id) do
      connection.get(test_id) do |v|
        val = v
      end
    end
    connection.run

    connection.async = false
    assert_equal 2, val
  end

end
