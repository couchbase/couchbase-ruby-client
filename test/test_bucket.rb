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

class TestBucket < MiniTest::Test
  def test_it_raises_connect_error_if_server_not_found
    skip('ss tool is not found') unless system('which ss > /dev/null 2>&1')
    refute(`ss -tnl` =~ /12345/)
    assert_raises Couchbase::Error::Connect do
      Couchbase.new("couchbase://127.0.0.1:12345")
    end
  end

  def test_it_raises_argument_error_for_illegal_url
    illegal = [
      "ftp://localhost:8091/",
      "http:/localhost:8091/"
    ]
    illegal.each do |url|
      assert_raises Couchbase::Error::Invalid do
        Couchbase.new(url)
      end
    end
  end

  def test_it_able_to_connect_to_protected_buckets
    with_mock(:buckets_spec => 'protected:secret') do |mock|
      connection = Couchbase.new("#{mock.connstr}/protected",
                                 :username => 'protected',
                                 :password => 'secret')
      assert connection.connected?
      assert_equal "protected", connection.bucket
    end
  end

  def test_it_allows_to_specify_credentials_in_url
    with_mock(:buckets_spec => 'protected:secret') do |mock|
      connection = Couchbase.new("#{mock.connstr}/protected?username=protected&password=secret")
      assert connection.connected?
      assert_equal "protected", connection.bucket
    end
  end

  def test_it_raises_error_with_wrong_credentials
    with_mock do |mock|
      assert_raises Couchbase::Error::Auth do
        Couchbase.new(mock.connstr,
                      :password => 'wrong_password')
      end
      assert_raises Couchbase::Error::Auth do
        Couchbase.new(mock.connstr,
                      :username => 'wrong.username',
                      :password => 'wrong_password')
      end
    end
  end

  def test_it_unable_to_connect_to_protected_buckets_with_wrong_credentials
    with_mock(:buckets_spec => 'protected:secret') do |mock|
      assert_raises Couchbase::Error::Auth do
        Couchbase.new("#{mock.connstr}/protected",
                      :username => 'wrong',
                      :password => 'secret')
      end
      assert_raises Couchbase::Error::Auth do
        Couchbase.new("#{mock.connstr}/protected",
                      :username => 'protected',
                      :password => 'wrong')
      end
    end
  end

  def test_it_is_connected
    connection = Couchbase.new(mock.connstr)
    assert connection.connected?
  end

  def test_it_is_possible_to_disconnect_instance
    connection = Couchbase.new(mock.connstr)
    connection.disconnect
    refute connection.connected?
  end

  def test_it_raises_error_on_double_disconnect
    connection = Couchbase.new(mock.connstr)
    connection.disconnect
    assert_raises Couchbase::Error::Connect do
      connection.disconnect
    end
  end

  def test_it_allows_to_reconnect_the_instance
    connection = Couchbase.new(mock.connstr)
    assert connection.disconnect
    refute connection.connected?
    connection.reconnect
    assert connection.connected?
    refute connection.set(uniq_id, "foo").error
  end

  def test_it_allows_to_change_configuration_during_reconnect
    with_mock(:buckets_spec => 'protected:secret') do |mock|
      connection = Couchbase.new("#{mock.connstr}/protected",
                                 :username => 'protected',
                                 :password => 'secret')
      connection.disconnect
      assert_raises Couchbase::Error::Auth do
        connection.reconnect("#{mock.connstr}/protected",
                             :username => 'protected',
                             :password => 'incorrect')
      end
      refute connection.connected?

      connection.reconnect("#{mock.connstr}/protected",
                           :username => 'protected',
                           :password => 'secret')
      assert connection.connected?
    end
  end

  def test_it_doesnt_try_to_destroy_handle_in_case_of_lcb_create_failure
    assert_raises(Couchbase::Error::Invalid) do
      Couchbase.connect("foobar:baz")
    end
    GC.start # make sure it won't touch handle in finalizer
  end

  def test_it_accepts_environment_option
    connection = Couchbase.new(mock.connstr, :environment => :development)
    assert_equal :development, connection.environment
  end

  def test_it_defaults_to_production_environment
    connection = Couchbase.new(mock.connstr)
    assert_equal :production, connection.environment
  end

  def test_it_uses_default_environment_if_unknown_was_specified
    connection = Couchbase.new(mock.connstr, :environment => :foobar)
    assert_equal :production, connection.environment
  end
end
