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
  def test_trivial_touch
    connection = Couchbase.new(mock.connstr)
    refute connection.set(uniq_id, "bar", :ttl => 1).error
    refute connection.touch(uniq_id, :ttl => 3).error

    mock.time_travel(2)

    res = connection.get(uniq_id)
    refute res.error

    mock.time_travel(4)

    res = connection.get(uniq_id)
    assert_instance_of Couchbase::LibraryError, res.error
    assert_equal 'LCB_KEY_ENOENT', res.error.name
  end

  def test_multi_touch
    connection = Couchbase.new(mock.connstr)
    refute connection.set(uniq_id(1), "bar").error
    refute connection.set(uniq_id(2), "baz").error

    res = connection.touch(uniq_id(1) => 1, uniq_id(2) => 1)
    refute res[uniq_id(1)].error
    refute res[uniq_id(2)].error

    mock.time_travel(2)

    [1, 2].each do |suff|
      res = connection.get(uniq_id(suff))
      assert_instance_of Couchbase::LibraryError, res.error
      assert_equal 'LCB_KEY_ENOENT', res.error.name
    end
  end

  def test_it_uses_default_ttl_for_touch
    connection = Couchbase.new(mock.connstr, :default_ttl => 1)
    refute connection.set(uniq_id, "bar", :ttl => 10).error
    refute connection.touch(uniq_id).error

    mock.time_travel(2)

    res = connection.get(uniq_id)
    assert_instance_of Couchbase::LibraryError, res.error
    assert_equal 'LCB_KEY_ENOENT', res.error.name
  end

  def test_it_accepts_ttl_for_get_command
    connection = Couchbase.new(mock.connstr)
    refute connection.set(uniq_id, "bar", :ttl => 10).error

    res = connection.get(uniq_id, :ttl => 1)
    refute res.error
    assert_equal "bar", res.value

    mock.time_travel(2)

    res = connection.get(uniq_id)
    assert_instance_of Couchbase::LibraryError, res.error
    assert_equal 'LCB_KEY_ENOENT', res.error.name
  end
end
