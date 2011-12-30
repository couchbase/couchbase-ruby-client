require File.join(File.dirname(__FILE__), 'setup')

class TestTouch < MiniTest::Unit::TestCase

  def setup
    @mock = start_mock
  end

  def teardown
    stop_mock(@mock)
  end

  def test_trivial_touch
    connection = Couchbase.new(:port => @mock.port)
    connection.set(test_id, "bar", :ttl => 1)
    connection.touch(test_id, :ttl => 2)
    sleep(1)
    assert connection.get(test_id)
    sleep(1)
    refute connection.get(test_id)
  end

  def test_it_uses_default_ttl_for_touch
    connection = Couchbase.new(:port => @mock.port, :default_ttl => 1)
    connection.set(test_id, "bar", :ttl => 10)
    connection.touch(test_id, :ttl => 1)
    sleep(2)
    refute connection.get(test_id)
  end

  def test_it_accepts_ttl_for_get_command
    connection = Couchbase.new(:port => @mock.port)
    connection.set(test_id, "bar", :ttl => 10)
    val = connection.get(test_id, :ttl => 1)
    assert_equal "bar", val
    sleep(2)
    refute connection.get(test_id)
  end

end
