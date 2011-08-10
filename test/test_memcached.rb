require File.join(File.dirname(__FILE__), 'setup')

class TestMemcached < MiniTest::Unit::TestCase

  def test_race_absence_during_initialization
    client = connection
    assert_raises(Memcached::NotFound) do
      client.get(test_id)
    end
  end

  def test_experimental_features_is_always_on
    assert_respond_to connection, :touch
    assert_respond_to connection(:experimental_features => false), :touch
    assert_respond_to connection('experimental_features' => false), :touch
  end

  protected

  def connection(options = {})
    uri = options.delete(:pool_uri) || "http://localhost:8091/pools/default"
    Couchbase.new(uri, options)
  end

  def test_id
    name = caller.first[/.*[` ](.*)'/, 1]
  end
end
