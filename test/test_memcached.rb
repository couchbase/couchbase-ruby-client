require File.join(File.dirname(__FILE__), 'setup')

class TestMemcached < MiniTest::Unit::TestCase

  def test_race_absence_during_initialization
    session = connect
    assert_raises(Memcached::NotFound) do
      session.get(test_id)
    end
  end

  def test_experimental_features_is_always_on
    assert_respond_to connect, :touch
    assert_respond_to connect(:experimental_features => false), :touch
    assert_respond_to connect('experimental_features' => false), :touch
  end

  def test_single_get
    session = connect
    session.set(test_id, "foo")
    assert "foo", session.get(test_id)
  end

  def test_single_get_missing
    assert_raises(Memcached::NotFound) do
      connect.get("missing-key")
    end
  end

  def test_multi_get
    session = connect
    session.set(test_id(1), "foo")
    session.set(test_id(2), "bar")
    expected = {test_id(1) => "foo", test_id(2) => "bar"}
    assert expected, session.get(test_id(1), test_id(2))
    ids = [1, 2].map{|n| test_id(n)}
    assert expected, session.get(ids)
  end

  def test_multi_get_missing
    session = connect
    session.set(test_id(1), "foo")
    expected = {test_id(1) => "foo"}
    assert expected, session.get(test_id(1), test_id(2))
    ids = [1, 2].map{|n| test_id(n)}
    assert expected, session.get(ids)
  end

  protected

  def connect(options = {})
    uri = options.delete(:pool_uri) || "http://localhost:8091/pools/default"
    Couchbase.new(uri, options)
  end

  def test_id(suffix = nil)
    "#{caller.first[/.*[` ](.*)'/, 1]}#{suffix}"
  end
end
