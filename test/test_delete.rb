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
  def test_trivial_delete
    connection = Couchbase.new(mock.connstr)
    refute connection.set(uniq_id, "bar").error

    res = connection.delete(uniq_id)
    refute res.error
    assert_kind_of Numeric, res.cas

    res = connection.delete(uniq_id)
    assert_instance_of Couchbase::LibraryError, res.error
    assert_equal 'LCB_KEY_ENOENT', res.error.name
  end

  def test_delete_missing
    connection = Couchbase.new(mock.connstr)
    res = connection.delete(uniq_id(:missing))
    assert_instance_of Couchbase::LibraryError, res.error
    assert_equal 'LCB_KEY_ENOENT', res.error.name
  end

  def test_delete_with_cas
    connection = Couchbase.new(mock.connstr)
    res = connection.set(uniq_id, "bar")
    refute res.error
    cas = res.cas
    missing_cas = res.cas + 1

    res = connection.delete(uniq_id, :cas => missing_cas)
    assert_instance_of Couchbase::LibraryError, res.error
    assert_equal 'LCB_KEY_EEXISTS', res.error.name

    res = connection.delete(uniq_id, :cas => cas)
    refute res.error
    assert_kind_of Numeric, res.cas
  end

  def test_allow_fixnum_as_cas_parameter
    connection = Couchbase.new(mock.connstr)
    res = connection.set(uniq_id, "bar")
    refute res.error
    cas = res.cas

    res = connection.delete(uniq_id, cas)
    refute res.error
    assert_kind_of Numeric, res.cas
  end

  def test_simple_multi_delete
    connection = Couchbase.new(mock.connstr)
    res = connection.set(uniq_id(1) => "bar", uniq_id(2) => "foo")
    refute res[uniq_id(1)].error
    refute res[uniq_id(2)].error

    res = connection.delete([uniq_id(1), uniq_id(2)])
    assert res.is_a?(Hash)
    refute res[uniq_id(1)].error
    assert_kind_of Numeric, res[uniq_id(1)].cas
    refute res[uniq_id(2)].error
    assert_kind_of Numeric, res[uniq_id(2)].cas
  end

  def test_simple_multi_delete_missing
    connection = Couchbase.new(mock.connstr)
    res = connection.set(uniq_id(1) => "bar", uniq_id(2) => "foo")
    refute res[uniq_id(1)].error
    refute res[uniq_id(2)].error

    res = connection.delete([uniq_id(1), uniq_id(:missing)])
    assert res.is_a?(Hash)
    refute res[uniq_id(1)].error
    assert_kind_of Numeric, res[uniq_id(1)].cas
    assert_instance_of Couchbase::LibraryError, res[uniq_id(:missing)].error
    assert_equal 'LCB_KEY_ENOENT', res[uniq_id(:missing)].error.name
  end

  def test_multi_delete_with_cas_check
    connection = Couchbase.new(mock.connstr)
    res = connection.set(uniq_id(1) => "bar", uniq_id(2) => "foo")
    refute res[uniq_id(1)].error
    assert_kind_of Numeric, res[uniq_id(1)].cas
    refute res[uniq_id(2)].error
    assert_kind_of Numeric, res[uniq_id(2)].cas

    res = connection.delete(uniq_id(1) => res[uniq_id(1)].cas, uniq_id(2) => res[uniq_id(2)].cas)
    assert res.is_a?(Hash)
    refute res[uniq_id(1)].error
    refute res[uniq_id(2)].error
  end

  def test_multi_delete_missing_with_cas_check
    connection = Couchbase.new(mock.connstr)
    res = connection.set(uniq_id(1) => "bar", uniq_id(2) => "foo")
    refute res[uniq_id(1)].error
    assert_kind_of Numeric, res[uniq_id(1)].cas
    refute res[uniq_id(2)].error
    assert_kind_of Numeric, res[uniq_id(2)].cas

    res = connection.delete(uniq_id(1) => res[uniq_id(1)].cas, uniq_id(:missing) => res[uniq_id(2)].cas)
    assert res.is_a?(Hash)
    refute res[uniq_id(1)].error
    assert_kind_of Numeric, res[uniq_id(1)].cas
    assert_instance_of Couchbase::LibraryError, res[uniq_id(:missing)].error
    assert_equal 'LCB_KEY_ENOENT', res[uniq_id(:missing)].error.name
  end

  def test_multi_delete_with_cas_check_mismatch
    connection = Couchbase.new(mock.connstr, :quiet => true)
    res = connection.set(uniq_id(1) => "bar", uniq_id(2) => "foo")
    refute res[uniq_id(1)].error
    assert_kind_of Numeric, res[uniq_id(1)].cas
    refute res[uniq_id(2)].error
    assert_kind_of Numeric, res[uniq_id(2)].cas

    res = connection.delete(uniq_id(1) => res[uniq_id(1)].cas + 1,
                            uniq_id(2) => res[uniq_id(2)].cas)
    assert res.is_a?(Hash)
    assert_instance_of Couchbase::LibraryError, res[uniq_id(1)].error
    assert_equal 'LCB_KEY_EEXISTS', res[uniq_id(1)].error.name
    refute res[uniq_id(2)].error
    assert_kind_of Numeric, res[uniq_id(2)].cas
  end
end
