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
require 'minitest/mock'

class TestCouchbase < MiniTest::Unit::TestCase

  def teardown
    Couchbase.reset_thread_storage!
  end

  def test_that_it_create_instance_of_bucket
    with_mock do |mock|
      assert_instance_of Couchbase::Bucket, Couchbase.new("http://#{mock.host}:#{mock.port}/pools/default")
    end
  end

  def test_verify_connection
    pid = Process.pid
    assert_equal pid, Couchbase.thread_storage[:pid]
    Couchbase.verify_connection!
    assert_equal pid, Couchbase.thread_storage[:pid]
  end

  def test_verify_connection_when_process_forks
    pid = Process.pid
    assert_equal pid, Couchbase.thread_storage[:pid]

    # stub a simulated Kernel#fork
    Process.stub(:pid, Process.pid + 1) do
      Couchbase.verify_connection!
      refute_equal pid, Couchbase.thread_storage[:pid]
    end
  end

  def test_new_connection_when_process_forks
    with_mock do |mock|
      connection_options = "http://#{mock.host}:#{mock.port}/pools/default"
      Couchbase.connection_options = connection_options
      old_bucket_id = Couchbase.bucket.object_id

      Process.stub(:pid, Process.pid + 1) do
        refute_equal old_bucket_id, Couchbase.bucket.object_id
      end
    end
  end

  def test_new_connection_has_same_configuration_options
    with_mock do |mock|
      connection_options = "http://#{mock.host}:#{mock.port}/pools/default"
      Couchbase.connection_options = connection_options
      old_bucket = Couchbase.bucket

      Process.stub(:pid, Process.pid + 1) do
        new_bucket = Couchbase.bucket
        assert_equal old_bucket.name, new_bucket.name
        assert_equal old_bucket.hostname, new_bucket.hostname
      end
    end
  end

end
