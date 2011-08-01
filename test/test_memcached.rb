require File.join(File.dirname(__FILE__), 'setup')

class TestMemcached < MiniTest::Unit::TestCase

  def test_race_absence_during_initialization
    client = Couchbase.new "http://localhost:8091/pools/default"
    assert_raises(Memcached::NotFound) do
      client.get(test_id)
    end
  end

  protected

  def test_id
    name = caller.first[/.*[` ](.*)'/, 1]
  end
end
