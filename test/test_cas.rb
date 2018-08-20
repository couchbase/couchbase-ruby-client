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

class TestCas < MiniTest::Test
  def test_compare_and_swap
    connection = Couchbase.new(mock.connstr)
    refute connection.set(uniq_id, "bar" => 1).error

    res = connection.cas(uniq_id) do |val|
      val["baz"] = 2
      val
    end
    refute res.error

    res = connection.get(uniq_id)
    refute res.error
    assert_equal({"bar" => 1, "baz" => 2}, res.value)
  end

  def test_compare_and_swap_collision
    connection = Couchbase.new(mock.connstr)

    refute connection.set(uniq_id, "bar" => 1).error

    res = connection.cas(uniq_id) do |val|
      # Simulate collision with a separate writer. This will
      # change the CAS value to be different than what #cas just loaded.
      refute connection.set(uniq_id, "bar" => 2).error

      # Complete the modification we desire, which should fail when set.
      val["baz"] = 3
      val
    end
    assert_instance_of Couchbase::LibraryError, res.error
    assert_equal 'LCB_KEY_EEXISTS', res.error.name
  end

  def test_compare_and_swap_retry
    connection = Couchbase.new(mock.connstr)
    refute connection.set(uniq_id, "bar" => 1).error

    calls = 0
    res = connection.cas(uniq_id, :retry => 1) do |val|
      calls += 1
      if calls == 1
        # Simulate collision with a separate writer. This will
        # change the CAS value to be different than what #cas just loaded.
        # Only do this the first time this block is executed.
        refute connection.set(uniq_id, "bar" => 2).error
      end

      # Complete the modification we desire, which should fail when set.
      val["baz"] = 3
      val
    end

    refute res.error
    assert_equal 2, calls

    res = connection.get(uniq_id)
    refute res.error
    assert_equal({"bar" => 2, "baz" => 3}, res.value)
  end

  def test_compare_and_swap_too_many_retries
    connection = Couchbase.new(mock.connstr)
    refute connection.set(uniq_id, "bar" => 0).error
    calls = 0
    res = connection.cas(uniq_id, :retry => 10) do |val|
      calls += 1

      # Simulate collision with a separate writer. This will
      # change the CAS value to be different than what #cas just loaded.
      # Do it every time so we just keep retrying and failing.
      refute connection.set(uniq_id, "bar" => calls).error

      # Complete the modification we desire, which should fail when set.
      val["baz"] = 3
      val
    end
    assert_instance_of Couchbase::LibraryError, res.error
    assert_equal 'LCB_KEY_EEXISTS', res.error.name
    assert_equal 11, calls
  end

  def test_format_replication
    connection = Couchbase.new(mock.connstr)
    refute connection.set(uniq_id, 'bar', :format => :plain).error

    res = connection.cas(uniq_id, :format => :plain) do |val|
      assert_equal 'bar', val
      'baz'
    end
    refute res.error

    res = connection.get(uniq_id, :format => :plain)
    refute res.error
    assert_equal 'baz', res.value
  end
end
