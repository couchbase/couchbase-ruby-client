# Author:: Couchbase <info@couchbase.com>
# Copyright:: 2011, 2012 Couchbase, Inc.
# License:: Apache License, Version 2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

require File.join(File.dirname(__FILE__), 'setup')

class TestGet < MiniTest::Unit::TestCase

  def setup
    @mock = start_mock
  end

  def teardown
    stop_mock(@mock)
  end

  def test_trivial_get
    connection = Couchbase.new(:port => @mock.port)
    connection.set(uniq_id, "bar")
    val = connection.get(uniq_id)
    assert_equal "bar", val
  end

  def test_extended_get
    connection = Couchbase.new(:port => @mock.port)

    orig_cas = connection.set(uniq_id, "bar")
    val, flags, cas = connection.get(uniq_id, :extended => true)
    assert_equal "bar", val
    assert_equal 0x0, flags
    assert_equal orig_cas, cas

    orig_cas = connection.set(uniq_id, "bar", :flags => 0x1000)
    val, flags, cas = connection.get(uniq_id, :extended => true)
    assert_equal "bar", val
    assert_equal 0x1000, flags
    assert_equal orig_cas, cas
  end

  def test_multi_get
    connection = Couchbase.new(:port => @mock.port)

    connection.set(uniq_id(1), "foo1")
    connection.set(uniq_id(2), "foo2")

    val1, val2 = connection.get(uniq_id(1), uniq_id(2))
    assert_equal "foo1", val1
    assert_equal "foo2", val2
  end

  def test_multi_get_extended
    connection = Couchbase.new(:port => @mock.port)

    cas1 = connection.set(uniq_id(1), "foo1")
    cas2 = connection.set(uniq_id(2), "foo2")

    results = connection.get(uniq_id(1), uniq_id(2), :extended => true)
    assert_equal ["foo1", 0x0, cas1], results[uniq_id(1)]
    assert_equal ["foo2", 0x0, cas2], results[uniq_id(2)]
  end

  def test_missing_in_quiet_mode
    connection = Couchbase.new(:port => @mock.port)
    cas1 = connection.set(uniq_id(1), "foo1")
    cas2 = connection.set(uniq_id(2), "foo2")

    val = connection.get(uniq_id(:missing))
    refute(val)
    val = connection.get(uniq_id(:missing), :extended => true)
    refute(val)

    val1, missing, val2  = connection.get(uniq_id(1), uniq_id(:missing), uniq_id(2))
    assert_equal "foo1", val1
    refute missing
    assert_equal "foo2", val2

    results = connection.get(uniq_id(1), uniq_id(:missing), uniq_id(2), :extended => true)
    assert_equal ["foo1", 0x0, cas1], results[uniq_id(1)]
    refute results[uniq_id(:missing)]
    assert_equal ["foo2", 0x0, cas2], results[uniq_id(2)]
  end

  def test_it_allows_temporary_quiet_flag
    connection = Couchbase.new(:port => @mock.port, :quiet => false)
    assert_raises(Couchbase::Error::NotFound) do
      connection.get(uniq_id(:missing))
    end
    refute connection.get(uniq_id(:missing), :quiet => true)
  end

  def test_missing_in_verbose_mode
    connection = Couchbase.new(:port => @mock.port, :quiet => false)
    connection.set(uniq_id(1), "foo1")
    connection.set(uniq_id(2), "foo2")

    assert_raises(Couchbase::Error::NotFound) do
      connection.get(uniq_id(:missing))
    end

    assert_raises(Couchbase::Error::NotFound) do
      connection.get(uniq_id(:missing), :extended => true)
    end

    assert_raises(Couchbase::Error::NotFound) do
      connection.get(uniq_id(1), uniq_id(:missing), uniq_id(2))
    end

    assert_raises(Couchbase::Error::NotFound) do
      connection.get(uniq_id(1), uniq_id(:missing), uniq_id(2), :extended => true)
    end
  end

  def test_asynchronous_get
    connection = Couchbase.new(:port => @mock.port)
    cas = connection.set(uniq_id, "foo", :flags => 0x6660)
    res = []

    suite = lambda do |conn|
      res.clear
      conn.get(uniq_id) # ignore result
      conn.get(uniq_id) {|ret| res << ret}
      handler = lambda {|ret| res << ret}
      conn.get(uniq_id, &handler)
      assert_equal 3, conn.seqno
    end

    checks = lambda do
      res.each do |r|
        assert r.is_a?(Couchbase::Result)
        assert r.success?
        assert_equal uniq_id, r.key
        assert_equal "foo", r.value
        assert_equal 0x6660, r.flags
        assert_equal cas, r.cas
      end
    end

    connection.run(&suite)
    checks.call

    connection.run{ suite.call(connection) }
    checks.call
  end

  def test_asynchronous_multi_get
    connection = Couchbase.new(:port => @mock.port)
    connection.set(uniq_id(1), "foo")
    connection.set(uniq_id(2), "bar")

    res = {}
    connection.run do |conn|
      conn.get(uniq_id(1), uniq_id(2)) {|ret| res[ret.key] = ret.value}
      assert_equal 2, conn.seqno
    end

    assert res[uniq_id(1)]
    assert_equal "foo", res[uniq_id(1)]
    assert res[uniq_id(2)]
    assert_equal "bar", res[uniq_id(2)]
  end

  def test_asynchronous_get_missing
    connection = Couchbase.new(:port => @mock.port)
    connection.set(uniq_id, "foo")
    res = {}
    missing = []

    get_handler = lambda do |ret|
      assert_equal :get, ret.operation
      if ret.success?
        res[ret.key] = ret.value
      else
        if ret.error.is_a?(Couchbase::Error::NotFound)
          missing << ret.key
        else
          raise ret.error
        end
      end
    end

    suite = lambda do |conn|
      res.clear
      missing.clear
      conn.get(uniq_id(:missing1), &get_handler)
      conn.get(uniq_id, uniq_id(:missing2), &get_handler)
      assert 3, conn.seqno
    end

    connection.run(&suite)
    assert_equal "foo", res[uniq_id]
    assert res.has_key?(uniq_id(:missing1)) # handler was called with nil
    refute res[uniq_id(:missing1)]
    assert res.has_key?(uniq_id(:missing2))
    refute res[uniq_id(:missing2)]
    assert_empty missing

    connection.quiet = false

    connection.run(&suite)
    refute res.has_key?(uniq_id(:missing1))
    refute res.has_key?(uniq_id(:missing2))
    assert_equal [uniq_id(:missing1), uniq_id(:missing2)], missing.sort
    assert_equal "foo", res[uniq_id]
  end

  def test_get_using_brackets
    connection = Couchbase.new(:port => @mock.port)

    orig_cas = connection.set(uniq_id, "foo", :flags => 0x1100)

    val = connection[uniq_id]
    assert_equal "foo", val

    if RUBY_VERSION =~ /^1\.9/
      eval <<-EOC
      val, flags, cas = connection[uniq_id, :extended => true]
      assert_equal "foo", val
      assert_equal 0x1100, flags
      assert_equal orig_cas, cas
      EOC
    end
  end
end
