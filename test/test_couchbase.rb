require 'minitest/autorun'
require 'couchbase'

class TestCouchbase < MiniTest::Unit::TestCase
  def test_that_it_create_instance_of_connection
    assert_instance_of Couchbase::Connection, Couchbase.new('http://localhost:8091/pools/default')
  end
end


