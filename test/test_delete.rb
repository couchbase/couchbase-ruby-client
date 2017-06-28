# Author:: Couchbase <info@couchbase.com>
# Copyright:: 2011-2017 Couchbase, Inc.
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

class TestStore < MiniTest::Test
  def setup
    @mock = start_mock
  end

  def teardown
    stop_mock(@mock)
  end

  def test_trivial_delete
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)
    connection.set(uniq_id, "bar")
    assert connection.delete(uniq_id)
    assert_raises(Couchbase::Error::NotFound) do
      connection.delete(uniq_id)
    end
  end

  def test_delete_missing
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)
    assert_raises(Couchbase::Error::NotFound) do
      connection.delete(uniq_id(:missing))
    end
    refute connection.delete(uniq_id(:missing), :quiet => true)
    refute connection.quiet?
    connection.quiet = true
    refute connection.delete(uniq_id(:missing))
  end

  def test_delete_with_cas
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)
    cas = connection.set(uniq_id, "bar")
    missing_cas = cas - 1
    assert_raises(Couchbase::Error::KeyExists) do
      connection.delete(uniq_id, :cas => missing_cas)
    end
    assert connection.delete(uniq_id, :cas => cas)
  end

  def test_allow_fixnum_as_cas_parameter
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port)
    cas = connection.set(uniq_id, "bar")
    assert connection.delete(uniq_id, cas)
  end

  def test_simple_multi_delete
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port, :quiet => true)
    connection.set(uniq_id(1) => "bar", uniq_id(2) => "foo")
    res = connection.delete(uniq_id(1), uniq_id(2))
    assert res.is_a?(Hash)
    assert res[uniq_id(1)]
    assert res[uniq_id(2)]
  end

  def test_simple_multi_delete_missing
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port, :quiet => true)
    connection.set(uniq_id(1) => "bar", uniq_id(2) => "foo")
    res = connection.delete(uniq_id(1), uniq_id(:missing), :quiet => true)
    assert res.is_a?(Hash)
    assert res[uniq_id(1)]
    refute res[uniq_id(:missing)]
  end

  def test_multi_delete_with_cas_check
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port, :quiet => true)
    cas = connection.set(uniq_id(1) => "bar", uniq_id(2) => "foo")
    res = connection.delete(uniq_id(1) => cas[uniq_id(1)], uniq_id(2) => cas[uniq_id(2)])
    assert res.is_a?(Hash)
    assert res[uniq_id(1)]
    assert res[uniq_id(2)]
  end

  def test_multi_delete_missing_with_cas_check
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port, :quiet => true)
    cas = connection.set(uniq_id(1) => "bar", uniq_id(2) => "foo")
    res = connection.delete(uniq_id(1) => cas[uniq_id(1)], uniq_id(:missing) => cas[uniq_id(2)])
    assert res.is_a?(Hash)
    assert res[uniq_id(1)]
    refute res[uniq_id(:missing)]
  end

  def test_multi_delete_with_cas_check_mismatch
    connection = Couchbase.new(:hostname => @mock.host, :port => @mock.port, :quiet => true)
    cas = connection.set(uniq_id(1) => "bar", uniq_id(2) => "foo")

    assert_raises(Couchbase::Error::KeyExists) do
      connection.delete(uniq_id(1) => cas[uniq_id(1)] + 1,
                        uniq_id(2) => cas[uniq_id(2)])
    end
  end
end
