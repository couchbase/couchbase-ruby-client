require File.join(File.dirname(__FILE__), 'setup')

class TestBucket < MiniTest::Unit::TestCase
  def setup
    config_fixture = json_fixture('buckets-config.json')
    Couchbase::Bucket.any_instance.stubs(:http_get).returns(config_fixture)
    Couchbase::Bucket.class_eval do
      alias :listen_for_config_changes_orig :listen_for_config_changes
      define_method :listen_for_config_changes do
        setup(config_fixture.first)
      end
    end
  end

  def teardown
    Couchbase::Bucket.class_eval do
      alias :listen_for_config_changes :listen_for_config_changes_orig
    end
  end

  def test_it_should_defaults_to_default_bucket_in_production_mode
    bucket = Couchbase.new('http://localhost:8091/pools/default')
    assert_equal 'default', bucket.name
    assert_equal :production, bucket.environment
  end

  def test_it_should_initialize_itself_from_config
    bucket = Couchbase.new('http://localhost:8091/pools/default')
    assert_equal 'http://localhost:8091/pools/default/buckets/default', bucket.uri.to_s
    assert_equal 'membase', bucket.type
    assert_equal 1, bucket.nodes.size
    assert_equal 256, bucket.vbuckets.size
  end

  def test_initialization_low_level_drivers
    bucket = Couchbase.new('http://localhost:8091/pools/default', :data_mode => :plain)
    assert_equal :plain, bucket.data_mode
  end
end
