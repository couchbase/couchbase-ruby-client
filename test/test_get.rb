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

class TestGet < MiniTest::Test
  def test_trivial_get
    connection = Couchbase.new(mock.connstr)

    res = connection.set(uniq_id, "bar")
    refute res.error
    orig_cas = res.cas

    res = connection.get(uniq_id)
    assert_equal "bar", res.value
    assert_equal orig_cas, res.cas
  end

  def test_multi_get
    connection = Couchbase.new(mock.connstr)

    res = connection.set(uniq_id(1), "foo1")
    refute res.error
    cas1 = res.cas
    res = connection.set(uniq_id(2), "foo2")
    refute res.error
    cas2 = res.cas

    res = connection.get([uniq_id(1), uniq_id(2)])
    refute res[uniq_id(1)].error
    refute res[uniq_id(2)].error
    assert_equal "foo1", res[uniq_id(1)].value
    assert_equal cas1, res[uniq_id(1)].cas
    assert_equal "foo2", res[uniq_id(2)].value
    assert_equal cas2, res[uniq_id(2)].cas
  end

  def test_multi_get_and_touch
    connection = Couchbase.new(mock.connstr)
    refute connection.set(uniq_id(1), "foo1").error
    refute connection.set(uniq_id(2), "foo2").error

    res = connection.get(uniq_id(1) => 1, uniq_id(2) => 1)
    assert_instance_of Hash, res
    refute res[uniq_id(1)].error
    assert_equal "foo1", res[uniq_id(1)].value
    refute res[uniq_id(2)].error
    assert_equal "foo2", res[uniq_id(2)].value
    mock.time_travel(2)
    res = connection.get([uniq_id(1), uniq_id(2)])
    assert_instance_of Couchbase::LibraryError, res[uniq_id(1)].error
    assert res[uniq_id(1)].error.data?
    assert_equal 'LCB_KEY_ENOENT', res[uniq_id(1)].error.name
    assert_equal Couchbase::LibraryError::LCB_KEY_ENOENT, res[uniq_id(1)].error.code
    assert_instance_of Couchbase::LibraryError, res[uniq_id(2)].error
    assert_equal 'LCB_KEY_ENOENT', res[uniq_id(2)].error.name
    assert_equal Couchbase::LibraryError::LCB_KEY_ENOENT, res[uniq_id(2)].error.code
  end

  def test_multi_get_and_touch_with_single_key
    connection = Couchbase.new(mock.connstr)
    refute connection.set(uniq_id, "foo1").error

    res = connection.get(uniq_id => 1)
    assert res.is_a?(Hash)
    refute res[uniq_id].error
    assert_equal "foo1", res[uniq_id].value
    mock.time_travel(2)
    res = connection.get(uniq_id)
    assert_instance_of Couchbase::LibraryError, res.error
    assert_equal 'LCB_KEY_ENOENT', res.error.name
    assert_equal Couchbase::LibraryError::LCB_KEY_ENOENT, res.error.code
  end

  def test_missing
    connection = Couchbase.new(mock.connstr)
    refute connection.set(uniq_id(1), "foo1").error
    refute connection.set(uniq_id(2), "foo2").error

    res = connection.get(uniq_id(:missing))
    assert_instance_of Couchbase::LibraryError, res.error
    assert_equal 'LCB_KEY_ENOENT', res.error.name
    assert_equal Couchbase::LibraryError::LCB_KEY_ENOENT, res.error.code

    res = connection.get([uniq_id(1), uniq_id(:missing), uniq_id(2)])
    refute res[uniq_id(1)].error
    refute res[uniq_id(2)].error
    assert_instance_of Couchbase::LibraryError, res[uniq_id(:missing)].error
    assert_equal 'LCB_KEY_ENOENT', res[uniq_id(:missing)].error.name
    assert_equal Couchbase::LibraryError::LCB_KEY_ENOENT, res[uniq_id(:missing)].error.code
  end

  def test_get_using_brackets
    connection = Couchbase.new(mock.connstr)

    refute connection.set(uniq_id, "foo").error

    res = connection[uniq_id]
    refute res.error
    assert_equal "foo", res.value
  end

  def test_it_allows_to_store_nil
    connection = Couchbase.new(mock.connstr)

    res = connection.set(uniq_id, nil)
    refute res.error
    assert res.cas.is_a?(Numeric)
    orig_cas = res.cas

    res = connection.get(uniq_id)
    refute res.error
    assert_nil res.value
    assert_equal orig_cas, res.cas
  end

  def test_zero_length_string_is_not_nil
    connection = Couchbase.new(mock.connstr)

    refute connection.set(uniq_id, "", :format => :document).error

    res = connection.get(uniq_id)
    refute res.error
    assert_equal '', res.value

    refute connection.set(uniq_id, "", :format => :plain).error
    res = connection.get(uniq_id)
    assert_equal '', res.value

    refute connection.set(uniq_id, "", :format => :marshal).error
    res = connection.get(uniq_id)
    assert_equal '', res.value

    refute connection.set(uniq_id, nil, :format => :document).error
    res = connection.get(uniq_id)
    refute res.error

    assert_raises Couchbase::Error::ValueFormat do
      connection.set(uniq_id, nil, :format => :plain)
    end

    connection.set(uniq_id, nil, :format => :marshal)
    res = connection.get(uniq_id)
    refute res.error
    assert_nil res.value
  end

  def test_format_forcing
    connection = Couchbase.new(mock.connstr)

    refute connection.set(uniq_id, '{"foo":"bar"}', :format => :plain).error
    res = connection.get(uniq_id)
    refute res.error
    assert_equal('{"foo":"bar"}', res.value)

    res = connection.get(uniq_id, :format => :document)
    assert_equal({"foo" => "bar"}, res.value)

    refute connection.prepend(uniq_id, "NOT-A-JSON").error
    res = connection.get(uniq_id, :format => :document)
    assert_instance_of Couchbase::Error::ValueFormat, res.error
    assert_nil res.value
  end

  def test_get_with_lock_trivial
    connection = Couchbase.new(mock.connstr)
    refute connection.set(uniq_id, "foo").error

    res = connection.get(uniq_id, :lock => 1)
    refute res.error
    assert_equal 'foo', res.value

    res = connection.set(uniq_id, 'bar')
    assert_instance_of Couchbase::LibraryError, res.error
    assert res.error.data?
    assert_equal 'LCB_KEY_EEXISTS', res.error.name
    assert_equal 12, res.error.code

    mock.time_travel(2)
    refute connection.set(uniq_id, "bar").error
  end

  def test_multi_get_with_lock
    connection = Couchbase.new(mock.connstr)
    refute connection.set(uniq_id(1), "foo1").error
    refute connection.set(uniq_id(2), "foo2").error

    res = connection.get([uniq_id(1), uniq_id(2)], :lock => 1)
    refute res[uniq_id(1)].error
    assert_equal 'foo1', res[uniq_id(1)].value
    refute res[uniq_id(2)].error
    assert_equal 'foo2', res[uniq_id(2)].value

    [1, 2].each do |arg|
      res = connection.set(uniq_id(arg), 'bar')
      assert_instance_of Couchbase::LibraryError, res.error
      assert res.error.data?
      assert_equal 'LCB_KEY_EEXISTS', res.error.name
      assert_equal 12, res.error.code
    end
  end

  def test_multi_get_with_custom_locks
    connection = Couchbase.new(mock.connstr)
    refute connection.set(uniq_id(1), "foo1").error
    refute connection.set(uniq_id(2), "foo2").error

    res = connection.get({uniq_id(1) => 1, uniq_id(2) => 3}, :lock => true)
    refute res[uniq_id(1)].error
    assert_equal 'foo1', res[uniq_id(1)].value
    refute res[uniq_id(2)].error
    assert_equal 'foo2', res[uniq_id(2)].value

    [1, 2].each do |arg|
      res = connection.set(uniq_id(arg), 'bar')
      assert_instance_of Couchbase::LibraryError, res.error
      assert res.error.data?
      assert_equal 'LCB_KEY_EEXISTS', res.error.name
      assert_equal 12, res.error.code
    end

    mock.time_travel(2)

    refute connection.set(uniq_id(1), 'bar').error

    res = connection.set(uniq_id(2), 'bar')
    assert_instance_of Couchbase::LibraryError, res.error
    assert res.error.data?
    assert_equal 'LCB_KEY_EEXISTS', res.error.name
    assert_equal 12, res.error.code
  end
end
