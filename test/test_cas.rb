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

class TestCas < MiniTest::Unit::TestCase

  def setup
    @mock = start_mock
  end

  def teardown
    stop_mock(@mock)
  end

  def test_compare_and_swap
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port,
                               :default_format => :document)
    connection.set(uniq_id, {"bar" => 1})
    connection.cas(uniq_id) do |val|
      val["baz"] = 2
      val
    end
    val = connection.get(uniq_id)
    expected = {"bar" => 1, "baz" => 2}
    assert_equal expected, val
  end

  def test_compare_and_swap_async
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port,
                               :default_format => :document)
    connection.set(uniq_id, {"bar" => 1})
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

end
