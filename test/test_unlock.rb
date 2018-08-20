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

class TestTouch < MiniTest::Test
  def test_trivial_unlock
    connection = Couchbase.new(mock.connstr)

    refute connection.set(uniq_id, "foo").error
    res = connection.get(uniq_id, :lock => true)
    refute res.error
    cas = res.cas

    res = connection.set(uniq_id, "bar")
    assert_instance_of Couchbase::LibraryError, res.error
    assert_equal 'LCB_KEY_EEXISTS', res.error.name

    res = connection.unlock(uniq_id, :cas => cas)
    refute res.error

    res = connection.set(uniq_id, "bar")
    refute res.error
  end

  def test_alternative_syntax_for_single_key
    connection = Couchbase.new(mock.connstr)
    refute connection.set(uniq_id, "foo").error
    res = connection.get(uniq_id, :lock => true)
    cas = res.cas

    res = connection.set(uniq_id, "bar")
    assert_instance_of Couchbase::LibraryError, res.error
    assert_equal 'LCB_KEY_EEXISTS', res.error.name

    res = connection.unlock(uniq_id, cas)
    refute res.error

    res = connection.set(uniq_id, "bar")
    refute res.error
  end

  def test_multiple_unlock
    connection = Couchbase.new(mock.connstr)
    refute connection.set(uniq_id(1), "foo").error
    refute connection.set(uniq_id(2), "foo").error

    res = connection.get([uniq_id(1), uniq_id(2)], :lock => true)
    cas1 = res[uniq_id(1)].cas
    cas2 = res[uniq_id(2)].cas
    [1, 2].each do |suff|
      res = connection.set(uniq_id(suff), 'bar')
      assert_instance_of Couchbase::LibraryError, res.error
      assert_equal 'LCB_KEY_EEXISTS', res.error.name
    end
    res = connection.unlock(uniq_id(1) => cas1, uniq_id(2) => cas2)
    refute res[uniq_id(1)].error
    refute res[uniq_id(2)].error

    refute connection.set(uniq_id(1), "bar").error
    refute connection.set(uniq_id(2), "bar").error
  end

  def test_quiet_mode
    connection = Couchbase.new(mock.connstr)
    refute connection.set(uniq_id, "foo").error
    res = connection.get(uniq_id, :lock => true)
    cas = res.cas
    res = connection.unlock(uniq_id(:missing), :cas => 0xdeadbeef)
    assert_instance_of Couchbase::LibraryError, res.error
    assert_equal 'LCB_KEY_ENOENT', res.error.name
    res = connection.unlock(uniq_id => cas,
                            uniq_id(:missing) => 0xdeadbeef)
    refute res[uniq_id].error
    assert_instance_of Couchbase::LibraryError, res[uniq_id(:missing)].error
    assert_equal 'LCB_KEY_ENOENT', res[uniq_id(:missing)].error.name
  end

  def test_tmp_failure
    connection = Couchbase.new(mock.connstr)

    res = connection.set(uniq_id(1), "foo")
    refute res.error
    cas1 = res.cas

    res = connection.set(uniq_id(2), "foo")
    refute res.error
    cas2 = res.cas

    res = connection.get(uniq_id(1), :lock => true) # get with lock will update CAS
    refute res.error

    res = connection.unlock(uniq_id(1), cas1)
    assert_instance_of Couchbase::LibraryError, res.error
    assert res.error.transient?
    assert_equal 'LCB_ETMPFAIL', res.error.name

    res = connection.unlock(uniq_id(2), cas2)
    assert_instance_of Couchbase::LibraryError, res.error
    assert res.error.transient?
    assert_equal 'LCB_ETMPFAIL', res.error.name
  end
end
