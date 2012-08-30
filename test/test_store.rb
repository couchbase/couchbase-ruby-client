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

class TestStore < MiniTest::Unit::TestCase

  def setup
    @mock = start_mock
  end

  def teardown
    stop_mock(@mock)
  end

  def test_trivial_set
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)
    cas = connection.set(uniq_id, "bar")
    assert(cas > 0)
  end

  def test_set_with_cas
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)

    cas1 = connection.set(uniq_id, "bar1")
    assert cas1 > 0

    assert_raises(Couchbase::Error::KeyExists) do
      connection.set(uniq_id, "bar2", :cas => cas1+1)
    end

    cas2 = connection.set(uniq_id, "bar2", :cas => cas1)
    assert cas2 > 0
    refute_equal cas2, cas1

    cas3 = connection.set(uniq_id, "bar3")
    assert cas3 > 0
    refute_equal cas3, cas2
    refute_equal cas3, cas1
  end

  def test_add
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)

    cas1 = connection.add(uniq_id, "bar")
    assert cas1 > 0

    assert_raises(Couchbase::Error::KeyExists) do
      connection.add(uniq_id, "bar")
    end

    assert_raises(Couchbase::Error::KeyExists) do
      connection.add(uniq_id, "bar", :cas => cas1)
    end
  end

  def test_replace
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)

    assert_raises(Couchbase::Error::NotFound) do
      connection.replace(uniq_id, "bar")
    end

    cas1 = connection.set(uniq_id, "bar")
    assert cas1 > 0

    connection.replace(uniq_id, "bar")
  end

  def test_acceptable_keys
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)

    cas = connection.set(uniq_id.to_sym, "bar")
    assert cas > 0

    cas = connection.set(uniq_id.to_s, "bar")
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
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)
    ret = nil
    connection.run do |conn|
      conn.set(uniq_id("1"), "foo1") {|res| ret = res}
      conn.set(uniq_id("2"), "foo2") # ignore result
    end
    assert ret.is_a?(Couchbase::Result)
    assert ret.success?
    assert_equal uniq_id("1"), ret.key
    assert_equal :set, ret.operation
    assert ret.cas.is_a?(Numeric)
  end

  def test_it_raises_error_when_appending_or_prepending_to_missing_key
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)

    assert_raises(Couchbase::Error::NotStored) do
      connection.append(uniq_id(:missing), "foo")
    end

    assert_raises(Couchbase::Error::NotStored) do
      connection.prepend(uniq_id(:missing), "foo")
    end
  end

  def test_append
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port, :default_format => :plain)

    cas1 = connection.set(uniq_id, "foo")
    assert cas1 > 0
    cas2 = connection.append(uniq_id, "bar")
    assert cas2 > 0
    refute_equal cas2, cas1

    val = connection.get(uniq_id)
    assert_equal "foobar", val
  end

  def test_prepend
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port, :default_format => :plain)

    cas1 = connection.set(uniq_id, "foo")
    assert cas1 > 0
    cas2 = connection.prepend(uniq_id, "bar")
    assert cas2 > 0
    refute_equal cas2, cas1

    val = connection.get(uniq_id)
    assert_equal "barfoo", val
  end

  def test_set_with_prefix
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port, :key_prefix => "prefix:")
    connection.set(uniq_id(:foo), "bar")
    assert_equal "bar", connection.get(uniq_id(:foo))
    expected = {uniq_id(:foo) => "bar"}
    assert_equal expected, connection.get(uniq_id(:foo), :assemble_hash => true)

    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port, :key_prefix => nil)
    expected = {"prefix:#{uniq_id(:foo)}" => "bar"}
    assert_equal expected, connection.get("prefix:#{uniq_id(:foo)}", :assemble_hash => true)
  end

  ArbitraryData = Struct.new(:baz)

  def test_set_using_brackets
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)

    connection[uniq_id(1)] = "foo"
    val = connection.get(uniq_id(1))
    assert_equal "foo", val

    if RUBY_VERSION =~ /^1\.9/
      eval <<-EOC
      connection[uniq_id(2), :flags => 0x1100] = "bar"
      val, flags = connection.get(uniq_id(2), :extended => true)
      assert_equal "bar", val
      assert_equal 0x1100, flags

      connection[uniq_id(3), :format => :marshal] = ArbitraryData.new("thing")
      val = connection.get(uniq_id(3))
      assert val.is_a?(ArbitraryData)
      assert_equal "thing", val.baz
      EOC
    end
  end

  def test_multi_store
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port, :default_format => :plain)
    connection.add(uniq_id(:a) => "bbb", uniq_id(:z) => "yyy")
    assert_equal ["bbb", "yyy"], connection.get(uniq_id(:a), uniq_id(:z))

    connection.prepend(uniq_id(:a) => "aaa", uniq_id(:z) => "xxx")
    assert_equal ["aaabbb", "xxxyyy"], connection.get(uniq_id(:a), uniq_id(:z))

    connection.append(uniq_id(:a) => "ccc", uniq_id(:z) => "zzz")
    assert_equal ["aaabbbccc", "xxxyyyzzz"], connection.get(uniq_id(:a), uniq_id(:z))

    connection.replace(uniq_id(:a) => "foo", uniq_id(:z) => "bar")
    assert_equal ["foo", "bar"], connection.get(uniq_id(:a), uniq_id(:z))

    res = connection.set(uniq_id(:a) => "bar", uniq_id(:z) => "foo")
    assert_equal ["bar", "foo"], connection.get(uniq_id(:a), uniq_id(:z))
    assert res.is_a?(Hash)
  end
end
