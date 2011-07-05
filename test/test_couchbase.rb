require 'minitest/autorun'
require 'couchbase'

class TestCouchbase < MiniTest::Unit::TestCase
  def test_that_it_create_instance_of_connection
    assert Couchbase.new.instance_of?(Couchbase::Connection)
  end
end


