require File.join(File.dirname(__FILE__), 'setup')

class TestStore < MiniTest::Unit::TestCase

  def setup
    @mock = start_mock
  end

  def teardown
    stop_mock(@mock)
  end

  def test_trivial_set
    connection = Couchbase.new(:port => @mock.port)
    connection.set(test_id, "bar")
    assert connection.delete(test_id)
    refute connection.delete(test_id)
  end

  def test_delete_missing
    connection = Couchbase.new(:port => @mock.port)
    refute connection.delete(test_id(:missing))
    connection.quiet = false
    assert_raises(Couchbase::Error::NotFound) do
      connection.delete(test_id(:missing))
    end
    refute connection.delete(test_id(:missing), :quiet => true)
  end

  def test_delete_with_cas
    connection = Couchbase.new(:port => @mock.port)
    cas = connection.set(test_id, "bar")
    missing_cas = cas - 1
    assert_raises(Couchbase::Error::KeyExists) do
      connection.delete(test_id, :cas => missing_cas)
    end
    assert connection.delete(test_id, :cas => cas)
  end

  def test_allow_fixnum_as_cas_parameter
    connection = Couchbase.new(:port => @mock.port)
    cas = connection.set(test_id, "bar")
    assert connection.delete(test_id, cas)
  end

end
