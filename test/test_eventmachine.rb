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
require 'eventmachine'
require 'em-synchrony'

class TestEventmachine < MiniTest::Test

  def setup
    @mock = start_mock
  end

  def teardown
    stop_mock(@mock)
  end

  if RUBY_VERSION.to_f >= 1.9

    def test_trivial_set
      EM.run do
        conn = Couchbase.new(:hostname => @mock.host, :port => @mock.port,
                             :engine => :eventmachine, :async => true)
        conn.on_connect do |res|
          assert res.success?, "connection must be successful"
          conn.set(uniq_id, "bar") do |res|
            assert res.success?, "the operation must be successful"
            assert res.cas > 0, "the CAS value must be non-zero"
            EM.stop
          end
        end
      end
    end

    def test_trivial_get
      EM.run do
        conn = Couchbase.new(:hostname => @mock.host, :port => @mock.port,
                             :engine => :eventmachine, :async => true)
        conn.on_connect do |res|
          assert res.success?, "connection must be successful"
          conn.set(uniq_id, "bar") do |res|
            assert res.success?, "the set operation must be successful"
            cas = res.cas
            conn.get(uniq_id) do |res|
              assert res.success?, "the get operation must be successful"
              assert_equal "bar", res.value
              assert_equal cas, res.cas
              EM.stop
            end
          end
        end
      end
    end

    def test_integration_with_em_synchrony
      EM.epoll
      EM.synchrony do
        Couchbase::Bucket.new(:engine => :eventmachine, bucket: "default", :node_list => ["localhost:8091"])
        EventMachine.stop
      end
    end
  end

end
