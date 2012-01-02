require File.join(File.dirname(__FILE__), 'setup')

class TestStats < MiniTest::Unit::TestCase

  def setup
    @mock = start_mock(:num_nodes => 4)
  end

  def teardown
    stop_mock(@mock)
  end

  def test_trivial_stats_without_argument
    connection = Couchbase.new(:port => @mock.port)
    stats = connection.stats
    assert stats.is_a?(Hash)
    assert_equal 4, stats.size
    node, info = stats.first
    assert node.is_a?(String)
    assert info.is_a?(Hash)
    assert info["pid"]
  end

  def test_stats_with_argument
    connection = Couchbase.new(:port => @mock.port)
    stats = connection.stats("pid")
    assert stats.is_a?(Hash)
    assert_equal 4, stats.size
    node, info = stats.first
    assert node.is_a?(String)
    assert info.is_a?(Hash)
    assert_equal 1, info.size
    assert info["pid"]
  end

end
