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

end
