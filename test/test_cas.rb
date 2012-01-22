require File.join(File.dirname(__FILE__), 'setup')

class TestCas < MiniTest::Unit::TestCase

  def setup
    @mock = start_mock
  end

  def teardown
    stop_mock(@mock)
  end

  def test_compare_and_swap
    connection = Couchbase.new(:port => @mock.port,
                               :default_format => :document)
    connection.set(test_id, {"bar" => 1})
    connection.cas(test_id) do |val|
      val["baz"] = 2
      val
    end
    val = connection.get(test_id)
    expected = {"bar" => 1, "baz" => 2}
    assert_equal expected, val
  end

  def test_compare_and_swap_async
    connection = Couchbase.new(:port => @mock.port,
                               :default_format => :document)
    connection.set(test_id, {"bar" => 1})
    connection.run do |conn|
      conn.cas(test_id) do |ret|
        new_val = ret.value
        new_val["baz"] = 2
        new_val
      end
    end
    val = connection.get(test_id)
    expected = {"bar" => 1, "baz" => 2}
    assert_equal expected, val
  end

end
