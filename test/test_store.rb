# Author:: Couchbase <info@couchbase.com>
# Copyright:: 2011-2018 Couchbase, Inc.
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

require File.join(__dir__, 'setup')

class TestStore < MiniTest::Test
  def test_trivial_set
    connection = Couchbase.new(mock.connstr)
    res = connection.set(uniq_id, "bar")
    refute res.error
    assert_operator res.cas, :>, 0
  end

  def test_set_with_cas
    connection = Couchbase.new(mock.connstr)

    res = connection.set(uniq_id, "bar1")
    refute res.error
    assert_operator res.cas, :>, 0
    cas1 = res.cas

    res = connection.set(uniq_id, "bar2", :cas => cas1 + 1)
    assert_instance_of Couchbase::LibraryError, res.error
    assert_equal 'LCB_KEY_EEXISTS', res.error.name

    res = connection.set(uniq_id, "bar2", :cas => cas1)
    refute res.error
    assert_operator res.cas, :>, 0
    cas2 = res.cas
    refute_equal cas2, cas1

    res = connection.set(uniq_id, "bar3")
    refute res.error
    assert_operator res.cas, :>, 0
    cas3 = res.cas

    refute_equal cas3, cas2
    refute_equal cas3, cas1
  end

  def test_add
    connection = Couchbase.new(mock.connstr)

    res = connection.add(uniq_id, "bar")
    refute res.error
    assert_operator res.cas, :>, 0
    cas = res.cas

    res = connection.add(uniq_id, "bar")
    assert_instance_of Couchbase::LibraryError, res.error
    assert_equal 'LCB_KEY_EEXISTS', res.error.name

    assert_raises(ArgumentError) do
      connection.add(uniq_id, "bar", :cas => cas)
    end
  end

  def test_replace
    connection = Couchbase.new(mock.connstr)

    res = connection.replace(uniq_id, "bar")
    assert_instance_of Couchbase::LibraryError, res.error
    assert_equal 'LCB_KEY_ENOENT', res.error.name

    res = connection.set(uniq_id, "bar")
    refute res.error
    assert_operator res.cas, :>, 0

    res = connection.replace(uniq_id, "bar")
    refute res.error
    assert_operator res.cas, :>, 0
  end

  def test_acceptable_keys
    connection = Couchbase.new(mock.connstr)

    res = connection.set(uniq_id.to_sym, "bar")
    refute res.error
    assert_operator res.cas, :>, 0

    res = connection.set(uniq_id.to_s, "bar")
    refute res.error
    assert_operator res.cas, :>, 0

    assert_raises(ArgumentError) do
      connection.set(nil, "bar")
    end

    obj = {:foo => "bar", :baz => 1}
    assert_raises(ArgumentError) do
      connection.set(obj, "bar")
    end

    class << obj
      alias_method :to_str, :to_s
    end

    # does not matter if object can act as string implicitly
    assert_raises(ArgumentError) do
      connection.set(obj, "bar")
    end
  end

  def test_it_raises_error_when_appending_or_prepending_to_missing_key
    connection = Couchbase.new(mock.connstr)

    res = connection.append(uniq_id(:missing), "foo")
    assert_instance_of Couchbase::LibraryError, res.error
    assert_equal 'LCB_NOT_STORED', res.error.name

    res = connection.prepend(uniq_id(:missing), "foo")
    assert_instance_of Couchbase::LibraryError, res.error
    assert_equal 'LCB_NOT_STORED', res.error.name
  end

  def test_append
    connection = Couchbase.new(mock.connstr, :default_format => :plain)

    res = connection.set(uniq_id, "foo")
    refute res.error
    assert_operator res.cas, :>, 0
    cas1 = res.cas

    res = connection.append(uniq_id, "bar")
    refute res.error
    assert_operator res.cas, :>, 0
    cas2 = res.cas

    refute_equal cas2, cas1

    res = connection.get(uniq_id)
    refute res.error
    assert_equal "foobar", res.value
  end

  def test_prepend
    connection = Couchbase.new(mock.connstr, :default_format => :plain)

    res = connection.set(uniq_id, "foo")
    refute res.error
    assert_operator res.cas, :>, 0
    cas1 = res.cas

    res = connection.prepend(uniq_id, "bar")
    refute res.error
    assert_operator res.cas, :>, 0
    cas2 = res.cas

    refute_equal cas2, cas1

    res = connection.get(uniq_id)
    refute res.error
    assert_equal "barfoo", res.value
  end

  ArbitraryData = Struct.new(:baz)

  def test_set_using_brackets
    connection = Couchbase.new(mock.connstr)

    connection[uniq_id(1)] = "foo"

    res = connection.get(uniq_id(1))
    refute res.error
    assert_equal "foo", res.value

    connection[uniq_id(3), :format => :marshal] = ArbitraryData.new("thing")

    res = connection.get(uniq_id(3))
    refute res.error
    assert_instance_of ArbitraryData, res.value
    assert_equal "thing", res.value.baz
  end

  def test_multi_store
    connection = Couchbase.new(mock.connstr, :default_format => :plain)

    res = connection.add(uniq_id(:a) => "bbb", uniq_id(:z) => "yyy")
    refute res[uniq_id(:a)].error
    refute res[uniq_id(:z)].error

    res = connection.get([uniq_id(:a), uniq_id(:z)])
    refute res[uniq_id(:a)].error
    assert_equal 'bbb', res[uniq_id(:a)].value
    refute res[uniq_id(:z)].error
    assert_equal 'yyy', res[uniq_id(:z)].value

    res = connection.prepend(uniq_id(:a) => "aaa", uniq_id(:z) => "xxx")
    refute res[uniq_id(:a)].error
    refute res[uniq_id(:z)].error

    res = connection.get([uniq_id(:a), uniq_id(:z)])
    refute res[uniq_id(:a)].error
    assert_equal 'aaabbb', res[uniq_id(:a)].value
    refute res[uniq_id(:z)].error
    assert_equal 'xxxyyy', res[uniq_id(:z)].value

    res = connection.append(uniq_id(:a) => "ccc", uniq_id(:z) => "zzz")
    refute res[uniq_id(:a)].error
    refute res[uniq_id(:z)].error

    res = connection.get([uniq_id(:a), uniq_id(:z)])
    refute res[uniq_id(:a)].error
    assert_equal 'aaabbbccc', res[uniq_id(:a)].value
    refute res[uniq_id(:z)].error
    assert_equal 'xxxyyyzzz', res[uniq_id(:z)].value

    res = connection.replace(uniq_id(:a) => "foo", uniq_id(:z) => "bar")
    refute res[uniq_id(:a)].error
    refute res[uniq_id(:z)].error

    res = connection.get([uniq_id(:a), uniq_id(:z)])
    refute res[uniq_id(:a)].error
    assert_equal 'foo', res[uniq_id(:a)].value
    refute res[uniq_id(:z)].error
    assert_equal 'bar', res[uniq_id(:z)].value

    res = connection.set(uniq_id(:a) => "bar", uniq_id(:z) => "foo")
    refute res[uniq_id(:a)].error
    refute res[uniq_id(:z)].error

    res = connection.get([uniq_id(:a), uniq_id(:z)])
    refute res[uniq_id(:a)].error
    assert_equal 'bar', res[uniq_id(:a)].value
    refute res[uniq_id(:z)].error
    assert_equal 'foo', res[uniq_id(:z)].value
  end

  def test_append_format
    connection = Couchbase.new(mock.connstr)
    assert_equal :document, connection.default_format

    res = connection.set(uniq_id, 'bar', :format => :plain)
    refute res.error

    res = connection.append(uniq_id, 'baz', :format => :plain)
    refute res.error

    res = connection.get(uniq_id)
    refute res.error
    assert_equal 'barbaz', res.value
  end
end
