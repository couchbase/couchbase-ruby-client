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

class TestArithmetic < MiniTest::Test
  def test_trivial_incr_decr
    connection = Couchbase.new(mock.connstr)

    refute connection.set(uniq_id, 1).error

    res = connection.incr(uniq_id)
    refute res.error
    assert_equal 2, res.value
    assert_kind_of Numeric, res.cas

    res = connection.get(uniq_id)
    refute res.error
    assert_equal 2, res.value

    refute connection.set(uniq_id, 7).error

    res = connection.decr(uniq_id)
    assert_equal 6, res.value
    assert_kind_of Numeric, res.cas

    res = connection.get(uniq_id)
    assert_equal 6, res.value
  end

  def test_it_fails_to_incr_decr_missing_key
    connection = Couchbase.new(mock.connstr)

    res = connection.incr(uniq_id(:missing))
    assert_instance_of Couchbase::LibraryError, res.error
    assert res.error.data?
    assert_equal 'LCB_KEY_ENOENT', res.error.name
    assert_equal Couchbase::LibraryError::LCB_KEY_ENOENT, res.error.code

    res = connection.decr(uniq_id(:missing))
    assert_instance_of Couchbase::LibraryError, res.error
    assert res.error.data?
    assert_equal 'LCB_KEY_ENOENT', res.error.name
    assert_equal Couchbase::LibraryError::LCB_KEY_ENOENT, res.error.code
  end

  def test_it_allows_to_make_increments_less_verbose_by_forcing_create_by_default
    connection = Couchbase.connect(mock.connstr, :default_arithmetic_init => true)

    res = connection.get(uniq_id)
    assert_instance_of Couchbase::LibraryError, res.error
    assert_equal 'LCB_KEY_ENOENT', res.error.name

    res = connection.incr(uniq_id)
    refute res.error
    assert_equal 0, res.value

    assert_equal 0, connection.get(uniq_id).value
  end

  def test_it_allows_to_setup_initial_value_during_connection
    connection = Couchbase.connect(mock.connstr, :default_arithmetic_init => 10)

    res = connection.get(uniq_id)
    assert_instance_of Couchbase::LibraryError, res.error
    assert_equal 'LCB_KEY_ENOENT', res.error.name

    res = connection.incr(uniq_id)
    refute res.error
    assert_equal 10, res.value

    assert_equal 10, connection.get(uniq_id).value
  end

  def test_it_allows_to_change_default_initial_value_after_connection
    connection = Couchbase.connect(mock.connstr)

    assert_equal 0, connection.default_arithmetic_init
    res = connection.incr(uniq_id)
    assert_instance_of Couchbase::LibraryError, res.error
    assert_equal 'LCB_KEY_ENOENT', res.error.name

    connection.default_arithmetic_init = 10
    assert_equal 10, connection.default_arithmetic_init
    res = connection.get(uniq_id)
    assert_instance_of Couchbase::LibraryError, res.error
    assert_equal 'LCB_KEY_ENOENT', res.error.name

    res = connection.incr(uniq_id)
    refute res.error
    assert_equal 10, res.value

    res = connection.get(uniq_id)
    refute res.error
    assert_equal 10, res.value
  end

  def test_it_creates_missing_key_when_initial_value_specified
    connection = Couchbase.new(mock.connstr)

    res = connection.incr(uniq_id(:missing), :initial => 5)
    refute res.error
    assert_equal 5, res.value

    res = connection.incr(uniq_id(:missing), :initial => 5)
    refute res.error
    assert_equal 6, res.value

    res = connection.get(uniq_id(:missing))
    refute res.error
    assert_equal 6, res.value
  end

  def test_it_uses_zero_as_default_value_for_missing_keys
    connection = Couchbase.new(mock.connstr)

    res = connection.incr(uniq_id(:missing), :create => true)
    refute res.error
    assert_equal 0, res.value

    res = connection.incr(uniq_id(:missing), :create => true)
    refute res.error
    assert_equal 1, res.value

    res = connection.get(uniq_id(:missing))
    refute res.error
    assert_equal 1, res.value
  end

  def test_it_allows_custom_ttl
    connection = Couchbase.new(mock.connstr)

    res = connection.incr(uniq_id, :create => true, :ttl => 1)
    refute res.error
    assert_equal 0, res.value

    res = connection.incr(uniq_id, :create => true)
    refute res.error
    assert_equal 1, res.value

    mock.time_travel(3)

    res = connection.get(uniq_id)
    assert_instance_of Couchbase::LibraryError, res.error
    assert_equal 'LCB_KEY_ENOENT', res.error.name
  end

  def test_decrement_with_absolute_ttl
    connection = Couchbase.new(mock.connstr)
    # absolute TTL: one second from now
    exp = Time.now.to_i + 2
    res = connection.decr(uniq_id, 12, :initial => 0, :ttl => exp)
    refute res.error
    assert_equal 0, res.value

    res = connection.get(uniq_id)
    refute res.error
    assert_equal 0, res.value

    mock.time_travel(2)

    res = connection.get(uniq_id)
    assert_instance_of Couchbase::LibraryError, res.error
    assert_equal 'LCB_KEY_ENOENT', res.error.name
  end

  def test_it_allows_custom_delta
    connection = Couchbase.new(mock.connstr)

    refute connection.set(uniq_id, 12).error

    res = connection.incr(uniq_id, 10)
    refute res.error
    assert_equal 22, res.value
  end

  def test_it_allows_to_specify_delta_in_options
    connection = Couchbase.new(mock.connstr)
    refute connection.set(uniq_id, 12).error

    res = connection.incr(uniq_id, :delta => 10)
    refute res.error
    assert_equal 22, res.value
  end

  def test_multi_incr
    connection = Couchbase.new(mock.connstr)
    res = connection.set(uniq_id(:foo) => 1, uniq_id(:bar) => 1)
    refute res[uniq_id(:foo)].error
    refute res[uniq_id(:bar)].error

    res = connection.incr([uniq_id(:foo), uniq_id(:bar)])
    refute res[uniq_id(:foo)].error
    assert_equal 2, res[uniq_id(:foo)].value
    refute res[uniq_id(:bar)].error
    assert_equal 2, res[uniq_id(:bar)].value

    res = connection.incr([uniq_id(:foo), uniq_id(:bar)], :delta => 10)
    refute res[uniq_id(:foo)].error
    assert_equal 12, res[uniq_id(:foo)].value
    refute res[uniq_id(:bar)].error
    assert_equal 12, res[uniq_id(:bar)].value

    res = connection.incr(uniq_id(:foo) => 2, uniq_id(:bar) => 3)
    refute res[uniq_id(:foo)].error
    assert_equal 14, res[uniq_id(:foo)].value
    refute res[uniq_id(:bar)].error
    assert_equal 15, res[uniq_id(:bar)].value
  end

  def test_multi_decr
    connection = Couchbase.new(mock.connstr)
    res = connection.set(uniq_id(:foo) => 14, uniq_id(:bar) => 15)
    refute res[uniq_id(:foo)].error
    refute res[uniq_id(:bar)].error

    res = connection.decr(uniq_id(:foo) => 2, uniq_id(:bar) => 3)
    refute res[uniq_id(:foo)].error
    assert_equal 12, res[uniq_id(:foo)].value
    refute res[uniq_id(:bar)].error
    assert_equal 12, res[uniq_id(:bar)].value

    res = connection.decr([uniq_id(:foo), uniq_id(:bar)], :delta => 10)
    refute res[uniq_id(:foo)].error
    assert_equal 2, res[uniq_id(:foo)].value
    refute res[uniq_id(:bar)].error
    assert_equal 2, res[uniq_id(:bar)].value

    res = connection.decr([uniq_id(:foo), uniq_id(:bar)])
    refute res[uniq_id(:foo)].error
    assert_equal 1, res[uniq_id(:foo)].value
    refute res[uniq_id(:bar)].error
    assert_equal 1, res[uniq_id(:bar)].value
  end
end
