require File.join(File.dirname(__FILE__), 'setup')

class TestCouchbase < MiniTest::Unit::TestCase
  def test_that_it_create_instance_of_bucket
    assert_instance_of Couchbase::Bucket, Couchbase.new('http://localhost:8091/pools/default')
  end
end


