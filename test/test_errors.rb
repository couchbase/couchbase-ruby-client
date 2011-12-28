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
require 'digest/md5'

class TestErrors < MiniTest::Unit::TestCase

  def setup
    @mock = start_mock
  end

  def teardown
    stop_mock(@mock)
  end

  def genkey(item)
    tuple = [item["author"], item["message"]]
    Digest::MD5.hexdigest(tuple.join('-'))
  end

  def test_graceful_add_with_collision
    connection = Couchbase.new(:port => @mock.port)
    msg1 = {"author" => "foo", "message" => "hi all", "time" => "2012-01-12 11:29:09"}
    key1 = test_id(genkey(msg1))
    msg2 = {"author" => "foo", "message" => "hi all", "time" => "2012-01-12 11:29:30"}
    key2 = test_id(genkey(msg2))

    connection.add(key1, msg1)
    begin
      connection.add(key2, msg2)
    rescue Couchbase::Error::KeyExists => ex
      # using info from exception
      # it could be done with cas operation, but we can save one request
      # here (in real world cas operation will be more consistent because it
      # fetch fresh version from the cluster)
      #
      # connection.cas(key2) do |msg|
      #   msg.merge("time" => [msg["time"], msg2["time"]])
      # end
      msg2 = msg1.merge("time" => [msg1["time"], msg2["time"]])
      connection.set(key2, msg2, :cas => ex.cas)
    end

    msg3 = {"author" => "foo", "message" => "hi all",
                "time" => ["2012-01-12 11:29:09", "2012-01-12 11:29:30"]}
    key3 = test_id(genkey(msg3))
    assert_equal msg3, connection.get(key3)

    connection.async = true
    msg4 = {"author" => "foo", "message" => "hi all", "time" => "2012-01-12 11:45:34"}
    key4 = test_id(genkey(msg4))

    connection.on_error do |op, key, err|
      assert_equal :add, op
      assert_equal key4, key
      msg4 = msg3.merge("time" => msg3["time"] + [msg4["time"]])
      connection.set(key, msg4, :cas => err.cas)
    end

    connection.add(key4, msg4)
    connection.run

    connection.async = false
    msg5 = {"author" => "foo", "message" => "hi all",
                "time" => ["2012-01-12 11:29:09", "2012-01-12 11:29:30", "2012-01-12 11:45:34"]}
    key5 = test_id(genkey(msg5))
    assert_equal msg5, connection.get(key5)
  end

end
