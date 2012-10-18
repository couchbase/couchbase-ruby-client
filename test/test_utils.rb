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

class TestUtils < MiniTest::Unit::TestCase

  def setup
    @mock = start_mock
  end

  def teardown
    stop_mock(@mock)
  end

  def test_complex_startkey
    assert_equal "all_docs?startkey=%5B%22Deadmau5%22%2C%22%22%5D", Couchbase::Utils.build_query("all_docs", :startkey =>  ["Deadmau5", ""])
  end

  def test_it_provides_enough_info_with_value_error
    class << MultiJson
      alias dump_good dump
      def dump(obj)
        raise ArgumentError, "cannot accept your object"
      end
    end
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)
    assert_raises(Couchbase::Error::ValueFormat) do
      connection.set(uniq_id, "foo")
    end
    begin
      connection.set(uniq_id, "foo")
    rescue Couchbase::Error::ValueFormat => ex
      assert_match /cannot accept your object/, ex.to_s
      assert_instance_of ArgumentError, ex.inner_exception
    end
  ensure
    class << MultiJson
      undef dump
      alias dump dump_good
    end
  end

end
