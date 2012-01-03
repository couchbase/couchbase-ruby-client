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

class TestTouch < MiniTest::Unit::TestCase

  def setup
    @mock = start_mock
  end

  def teardown
    stop_mock(@mock)
  end

  def test_trivial_touch
    connection = Couchbase.new(:port => @mock.port)
    connection.set(test_id, "bar", :ttl => 1)
    connection.touch(test_id, :ttl => 2)
    sleep(1)
    assert connection.get(test_id)
    sleep(1)
    refute connection.get(test_id)
  end

  def test_it_uses_default_ttl_for_touch
    connection = Couchbase.new(:port => @mock.port, :default_ttl => 1)
    connection.set(test_id, "bar", :ttl => 10)
    connection.touch(test_id, :ttl => 1)
    sleep(2)
    refute connection.get(test_id)
  end

  def test_it_accepts_ttl_for_get_command
    connection = Couchbase.new(:port => @mock.port)
    connection.set(test_id, "bar", :ttl => 10)
    val = connection.get(test_id, :ttl => 1)
    assert_equal "bar", val
    sleep(2)
    refute connection.get(test_id)
  end

end
