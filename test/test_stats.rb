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

class TestStats < MiniTest::Test
  def test_trivial_stats_without_argument
    connection = Couchbase.new(mock.connstr)
    stats = connection.stats
    assert_instance_of Array, stats
    assert(stats.find { |s| s.key == "pid" })
    info = stats.first
    assert info.key.is_a?(String)
    assert_equal(mock.num_nodes, stats.group_by(&:node).size)
  end

  def test_stats_with_argument
    connection = Couchbase.new(mock.connstr)
    stats = connection.stats("memory")
    assert_instance_of Array, stats
    assert(stats.find { |s| s.key == "mem_used" })
    info = stats.first
    assert info.key.is_a?(String)
    assert_equal(mock.num_nodes, stats.group_by(&:node).size)
  end
end
