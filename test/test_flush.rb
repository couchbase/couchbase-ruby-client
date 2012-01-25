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

class TestFlush < MiniTest::Unit::TestCase

  def setup
    @mock = start_mock(:num_nodes => 7)
  end

  def teardown
    stop_mock(@mock)
  end

  def test_trivial_flush
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)
    assert connection.flush
  end

  def test_flush_with_block
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)
    flushed = {}
    on_node_flush = lambda{|res| flushed[res.node] = res.success?}
    connection.run do |conn|
      conn.flush(&on_node_flush)
    end
    assert_equal 7, flushed.size
    flushed.each do |node, res|
      assert node.is_a?(String)
      assert res
    end
  end

end
