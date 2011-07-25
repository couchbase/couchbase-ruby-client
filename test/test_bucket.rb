require 'minitest/autorun'
require 'couchbase'

class TestBucket < MiniTest::Unit::TestCase
  def test_initialization_low_level_drivers
    bucket = Couchbase.new('http://localhost:8091/pools/default', :per_page => 100, :data_mode => :plain)
    assert_equal :plain, bucket.data_mode
    assert_equal 100, bucket.per_page
  end
end
