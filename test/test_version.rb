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

class TestVersion < MiniTest::Unit::TestCase

  def setup
    @mock = start_mock
  end

  def teardown
    stop_mock(@mock)
  end

  def test_sync_version
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)
    ver = connection.version
    assert ver.is_a?(Hash)
    assert_equal @mock.num_nodes, ver.size
  end

  def test_async_version
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)
    ver = {}
    connection.run do |conn|
      conn.version do |ret|
        assert ret.success?
        ver[ret.node] = ret.value
      end
    end
    assert_equal @mock.num_nodes, ver.size
    ver.each do |node, v|
      assert v.is_a?(String)
    end
  end

end
