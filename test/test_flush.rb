require File.join(File.dirname(__FILE__), 'setup')

class TestFlush < MiniTest::Unit::TestCase

  def setup
    @mock = start_mock(:num_nodes => 7)
  end

  def teardown
    stop_mock(@mock)
  end

  def test_trivial_flush
    connection = Couchbase.new(:port => @mock.port)
    assert connection.flush
  end

  def test_flush_with_block
    connection = Couchbase.new(:port => @mock.port)
    flushed = {}
    on_node_flush = lambda{|node, res| flushed[node] = res}
    connection.run do |conn|
      conn.flush(&on_node_flush)
    end
    assert_equal 7, flushed.size
    flushed.each do |node, res|
      assert node.is_a?(String)
      assert res
    end
  end

end
