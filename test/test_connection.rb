require 'minitest/autorun'
require 'couchbase'

class TestConnection < MiniTest::Unit::TestCase
  def test_initialization_low_level_drivers
    connection = Couchbase.new('http://localhost:8091/pools/default', :per_page => 100, :data_mode => :plain)
    assert_equal :plain, connection.data_mode
    assert_equal 100, connection.per_page
  end
end
