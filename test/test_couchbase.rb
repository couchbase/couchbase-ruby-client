require File.join(File.dirname(__FILE__), 'setup')

class TestCouchbase < MiniTest::Unit::TestCase

  def test_that_it_create_instance_of_bucket
    with_mock do |mock|
      assert_instance_of Couchbase::Bucket, Couchbase.new("http://localhost:#{mock.port}/pools/default")
    end
  end

end
