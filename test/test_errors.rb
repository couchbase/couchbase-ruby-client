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
require 'digest/md5'

class TestErrors < MiniTest::Test
  def genkey(item)
    tuple = [item["author"], item["message"]]
    Digest::MD5.hexdigest(tuple.join('-'))
  end

  def test_graceful_add_with_collision
    connection = Couchbase.new(mock.connstr)

    msg1 = {"author" => "foo", "message" => "hi all", "time" => "2012-01-12 11:29:09"}
    key1 = uniq_id(genkey(msg1))
    msg2 = {"author" => "foo", "message" => "hi all", "time" => "2012-01-12 11:29:30"}
    key2 = uniq_id(genkey(msg2))

    res = connection.add(key1, msg1)
    refute res.error
    res = connection.add(key2, msg2)
    if res.error && res.error.code == Couchbase::LibraryError::LCB_KEY_EEXISTS
      # using info from exception
      # it could be done with cas operation, but we can save one request
      # here (in real world cas operation will be more consistent because it
      # fetch fresh version from the cluster)
      #
      # connection.cas(key2) do |msg|
      #   msg.merge("time" => [msg["time"], msg2["time"]])
      # end
      msg2 = msg1.merge("time" => [msg1["time"], msg2["time"]])
      refute connection.set(key2, msg2, :cas => res.cas).error
    end

    msg3 = {"author" => "foo", "message" => "hi all",
            "time" => ["2012-01-12 11:29:09", "2012-01-12 11:29:30"]}
    key3 = uniq_id(genkey(msg3))
    res = connection.get(key3)
    refute res.error
    assert_equal msg3, res.value

    msg4 = {"author" => "foo", "message" => "hi all", "time" => "2012-01-12 11:45:34"}
    key4 = uniq_id(genkey(msg4))

    res = connection.add(key4, msg4)
    assert_equal :add, res.operation
    assert_equal key4, res.key
    msg4 = msg3.merge("time" => msg3["time"] + [msg4["time"]])
    refute connection.set(res.key, msg4, :cas => res.cas).error

    msg5 = {"author" => "foo", "message" => "hi all",
            "time" => ["2012-01-12 11:29:09", "2012-01-12 11:29:30", "2012-01-12 11:45:34"]}
    key5 = uniq_id(genkey(msg5))
    res = connection.get(key5)
    refute res.error
    assert_equal msg5, res.value
  end
end
