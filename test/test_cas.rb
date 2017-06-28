# Author:: Couchbase <info@couchbase.com>
# Copyright:: 2011-2017 Couchbase, Inc.
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

class TestCas < MiniTest::Test
  def setup
    @mock = start_mock
  end

  def teardown
    stop_mock(@mock)
  end

  def test_compare_and_swap
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port,
                               :default_format => :document)
    connection.set(uniq_id, "bar" => 1)
    connection.cas(uniq_id) do |val|
      val["baz"] = 2
      val
    end
    val = connection.get(uniq_id)
    expected = {"bar" => 1, "baz" => 2}
    assert_equal expected, val
  end

  def test_compare_and_swap_collision
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port,
                               :default_format => :document)
    connection.set(uniq_id, "bar" => 1)
    assert_raises(Couchbase::Error::KeyExists) do
      connection.cas(uniq_id) do |val|
        # Simulate collision with a separate writer. This will
        # change the CAS value to be different than what #cas just loaded.
        connection.set(uniq_id, "bar" => 2)

        # Complete the modification we desire, which should fail when set.
        val["baz"] = 3
        val
      end
    end
  end

  def test_compare_and_swap_retry
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port,
                               :default_format => :document)
    connection.set(uniq_id, "bar" => 1)
    calls = 0
    connection.cas(uniq_id, :retry => 1) do |val|
      calls += 1
      if calls == 1
        # Simulate collision with a separate writer. This will
        # change the CAS value to be different than what #cas just loaded.
        # Only do this the first time this block is executed.
        connection.set(uniq_id, "bar" => 2)
      end

      # Complete the modification we desire, which should fail when set.
      val["baz"] = 3
      val
    end
    assert_equal 2, calls
    val = connection.get(uniq_id)
    expected = {"bar" => 2, "baz" => 3}
    assert_equal expected, val
  end

  def test_compare_and_swap_too_many_retries
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port,
                               :default_format => :document)
    connection.set(uniq_id, "bar" => 0)
    calls = 0
    assert_raises(Couchbase::Error::KeyExists) do
      connection.cas(uniq_id, :retry => 10) do |val|
        calls += 1

        # Simulate collision with a separate writer. This will
        # change the CAS value to be different than what #cas just loaded.
        # Do it every time so we just keep retrying and failing.
        connection.set(uniq_id, "bar" => calls)

        # Complete the modification we desire, which should fail when set.
        val["baz"] = 3
        val
      end
    end
    assert_equal 11, calls
  end

  def test_compare_and_swap_async
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port,
                               :default_format => :document)
    connection.set(uniq_id, "bar" => 1)
    calls = 0
    connection.run do |conn|
      conn.cas(uniq_id) do |ret|
        calls += 1
        case ret.operation
        when :get
          new_val = ret.value
          new_val["baz"] = 2
          new_val
        when :set
          assert ret.success?
        else
          flunk "Unexpected operation: #{ret.operation.inspect}"
        end
      end
    end
    assert_equal 2, calls
    val = connection.get(uniq_id)
    expected = {"bar" => 1, "baz" => 2}
    assert_equal expected, val
  end

  def test_compare_and_swap_async_collision
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port,
                               :default_format => :document)
    connection.set(uniq_id, "bar" => 1)
    calls = 0
    connection.run do |conn|
      conn.cas(uniq_id) do |ret|
        calls += 1
        case ret.operation
        when :get
          new_val = ret.value

          # Simulate collision with a separate writer. This will
          # change the CAS value to be different than what #cas just loaded.
          connection.set(uniq_id, "bar" => 2)

          # Complete the modification we desire, which should fail when set.
          new_val["baz"] = 3
          new_val
        when :set
          assert ret.error.is_a? Couchbase::Error::KeyExists
        else
          flunk "Unexpected operation: #{ret.operation.inspect}"
        end
      end
    end
    assert_equal 2, calls
  end

  def test_compare_and_swap_async_retry
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port,
                               :default_format => :document)
    connection.set(uniq_id, "bar" => 1)
    calls = 0
    connection.run do |conn|
      conn.cas(uniq_id, :retry => 1) do |ret|
        calls += 1
        case ret.operation
        when :get
          new_val = ret.value

          if calls == 1
            # Simulate collision with a separate writer. This will
            # change the CAS value to be different than what #cas just loaded.
            # Only do this the first time this block is executed.
            connection.set(uniq_id, "bar" => 2)
          end

          # Complete the modification we desire, which should fail when set.
          new_val["baz"] = 3
          new_val
        when :set
          assert ret.success?
        else
          flunk "Unexpected operation: #{ret.operation.inspect}"
        end
      end
    end
    assert_equal 3, calls
    val = connection.get(uniq_id)
    expected = {"bar" => 2, "baz" => 3}
    assert_equal expected, val
  end

  def test_compare_and_swap_async_too_many_retries
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port,
                               :default_format => :document)
    connection.set(uniq_id, "bar" => 0)
    calls = 0
    connection.run do |conn|
      conn.cas(uniq_id, :retry => 10) do |ret|
        calls += 1
        case ret.operation
        when :get
          new_val = ret.value

          # Simulate collision with a separate writer. This will
          # change the CAS value to be different than what #cas just loaded.
          # Do it every time so we just keep retrying and failing.
          connection.set(uniq_id, "bar" => calls)

          # Complete the modification we desire, which should fail when set.
          new_val["baz"] = 3
          new_val
        when :set
          assert ret.error.is_a? Couchbase::Error::KeyExists
        else
          flunk "Unexpected operation: #{ret.operation.inspect}"
        end
      end
    end
    assert_equal 12, calls
  end

  def test_flags_replication
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port,
                               :default_format => :document)
    connection.set(uniq_id, "bar", :flags => 0x100)
    connection.cas(uniq_id) { "baz" }
    _, flags, = connection.get(uniq_id, :extended => true)
    assert_equal 0x100, flags
  end
end
