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

class TestTouch < MiniTest::Test

  def setup
    @mock = start_mock
  end

  def teardown
    stop_mock(@mock)
  end

  def test_trivial_unlock
    if @mock.real?
      connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)
      connection.set(uniq_id, "foo")
      _, _, cas = connection.get(uniq_id, :lock => true, :extended => true)
      assert_raises Couchbase::Error::KeyExists do
        connection.set(uniq_id, "bar")
      end
      assert connection.unlock(uniq_id, :cas => cas)
      connection.set(uniq_id, "bar")
    else
      skip("GETL and UNL aren't implemented in CouchbaseMock.jar yet")
    end
  end

  def test_alternative_syntax_for_single_key
    if @mock.real?
      connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)
      connection.set(uniq_id, "foo")
      _, _, cas = connection.get(uniq_id, :lock => true, :extended => true)
      assert_raises Couchbase::Error::KeyExists do
        connection.set(uniq_id, "bar")
      end
      assert connection.unlock(uniq_id, cas)
      connection.set(uniq_id, "bar")
    else
      skip("GETL and UNL aren't implemented in CouchbaseMock.jar yet")
    end
  end

  def test_multiple_unlock
    if @mock.real?
      connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)
      connection.set(uniq_id(1), "foo")
      connection.set(uniq_id(2), "foo")
      info = connection.get(uniq_id(1), uniq_id(2), :lock => true, :extended => true)
      assert_raises Couchbase::Error::KeyExists do
        connection.set(uniq_id(1), "bar")
      end
      assert_raises Couchbase::Error::KeyExists do
        connection.set(uniq_id(2), "bar")
      end
      ret = connection.unlock(uniq_id(1) => info[uniq_id(1)][2],
                              uniq_id(2) => info[uniq_id(2)][2])
      assert ret[uniq_id(1)]
      assert ret[uniq_id(2)]
      connection.set(uniq_id(1), "bar")
      connection.set(uniq_id(2), "bar")
    else
      skip("GETL and UNL aren't implemented in CouchbaseMock.jar yet")
    end
  end

  def test_quiet_mode
    if @mock.real?
      connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)
      connection.set(uniq_id, "foo")
      _, _, cas = connection.get(uniq_id, :lock => true, :extended => true)
      assert_raises Couchbase::Error::NotFound do
        connection.unlock(uniq_id(:missing), :cas => 0xdeadbeef)
      end
      keys = {
        uniq_id => cas,
        uniq_id(:missing) => 0xdeadbeef
      }
      ret = connection.unlock(keys, :quiet => true)
      assert ret[uniq_id]
      refute ret[uniq_id(:missing)]
    else
      skip("GETL and UNL aren't implemented in CouchbaseMock.jar yet")
    end
  end

  def test_tmp_failure
    if @mock.real?
      connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)
      cas1 = connection.set(uniq_id(1), "foo")
      cas2 = connection.set(uniq_id(2), "foo")
      connection.get(uniq_id(1), :lock => true) # get with lock will update CAS
      assert_raises Couchbase::Error::TemporaryFail do
        connection.unlock(uniq_id(1), cas1)
      end
      assert_raises Couchbase::Error::TemporaryFail do
        connection.unlock(uniq_id(2), cas2)
      end
    else
      skip("GETL and UNL aren't implemented in CouchbaseMock.jar yet")
    end
  end
end
