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
    cas = connection.set(test_id, "bar")
    assert(cas > 0)
  end

  def test_set_with_cas
    connection = Couchbase.new(:port => @mock.port)

    cas1 = connection.set(test_id, "bar1")
    assert cas1 > 0

    assert_raises(Couchbase::Error::KeyExists) do
      connection.set(test_id, "bar2", :cas => cas1+1)
    end

    cas2 = connection.set(test_id, "bar2", :cas => cas1)
    assert cas2 > 0
    refute_equal cas2, cas1

    cas3 = connection.set(test_id, "bar3")
    assert cas3 > 0
    refute_equal cas3, cas2
    refute_equal cas3, cas1
  end

  def test_add
    connection = Couchbase.new(:port => @mock.port)

    cas1 = connection.add(test_id, "bar")
    assert cas1 > 0

    assert_raises(Couchbase::Error::KeyExists) do
      connection.add(test_id, "bar")
    end

    assert_raises(Couchbase::Error::KeyExists) do
      connection.add(test_id, "bar", :cas => cas1)
    end
  end

  def test_replace
    connection = Couchbase.new(:port => @mock.port)

    assert_raises(Couchbase::Error::NotFound) do
      connection.replace(test_id, "bar")
    end

    cas1 = connection.set(test_id, "bar")
    assert cas1 > 0

    connection.replace(test_id, "bar")
  end

  def test_acceptable_keys
    connection = Couchbase.new(:port => @mock.port)

    cas = connection.set(test_id.to_sym, "bar")
    assert cas > 0

    cas = connection.set(test_id.to_s, "bar")
    assert cas > 0

    assert_raises(TypeError) do
      connection.set(nil, "bar")
    end

    obj = {:foo => "bar", :baz => 1}
    assert_raises(TypeError) do
      connection.set(obj, "bar")
    end

    class << obj
      alias :to_str :to_s
    end

    connection.set(obj, "bar")
    assert cas > 0
  end

  def test_asynchronous_set
    connection = Couchbase.new(:port => @mock.port, :async => true)
    cas1 = cas2 = cas4 = nil
    connection.set(test_id("1"), "foo1") {|c| cas1 = c}
    connection.set(test_id("2"), "foo2") # ignore result
    connection.set(test_id("3"), "foo3") {|c, k, o| cas2 = {:cas => c, :key => k, :op => o}}
    connection.set(test_id("3"), "foo4") {|c| cas4 = c}
    assert_equal 4, connection.seqno
    connection.run
    assert_equal test_id("3"), cas2[:key]
    assert_equal :set, cas2[:op]
    assert cas2[:cas].is_a?(Numeric)
    # prove that latest call wins
    connection.set(test_id("3"), "bar", :cas => cas4)
  end

  def test_it_raises_error_when_appending_or_prepending_to_missing_key
    connection = Couchbase.new(:port => @mock.port)

    assert_raises(Couchbase::Error::NotStored) do
      connection.append(test_id(:missing), "foo")
    end

    assert_raises(Couchbase::Error::NotStored) do
      connection.prepend(test_id(:missing), "foo")
    end
  end

  def test_append
    connection = Couchbase.new(:port => @mock.port, :default_format => :plain)

    cas1 = connection.set(test_id, "foo")
    assert cas1 > 0
    cas2 = connection.append(test_id, "bar")
    assert cas2 > 0
    refute_equal cas2, cas1

    val = connection.get(test_id)
    assert_equal "foobar", val
  end

  def test_prepend
    connection = Couchbase.new(:port => @mock.port, :default_format => :plain)

    cas1 = connection.set(test_id, "foo")
    assert cas1 > 0
    cas2 = connection.prepend(test_id, "bar")
    assert cas2 > 0
    refute_equal cas2, cas1

    val = connection.get(test_id)
    assert_equal "barfoo", val
  end
end
