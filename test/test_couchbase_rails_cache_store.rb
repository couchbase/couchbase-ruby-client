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
require 'active_support/cache/couchbase_store'
require 'active_support/notifications'
require 'ostruct'

class TestCouchbaseRailsCacheStore < MiniTest::Test

  def setup
    @mock = start_mock
    @foo = OpenStruct.new :payload => "foo"
    @foobar = OpenStruct.new :payload => "foobar"
  end

  def teardown
    stop_mock(@mock)
  end

  def store
    @store ||= ActiveSupport::Cache::CouchbaseStore.new(:hostname => @mock.host,
                                                        :port => @mock.port)
  end

  def pool_store
    @pool_store ||= ActiveSupport::Cache::CouchbaseStore.new(:hostname => @mock.host,
                                                             :port => @mock.port,
                                                             :connection_pool => 5)
  end

  def prefixed_store
    @prefixed_store ||= ActiveSupport::Cache::CouchbaseStore.new(:hostname => @mock.host,
                                                                 :port => @mock.port,
                                                                 :namespace => 'v1')
  end

  def test_it_supported_methods
    supported_methods = store.public_methods(false).map(&:to_sym)
    assert supported_methods.include?(:fetch)
    assert supported_methods.include?(:write)
    assert supported_methods.include?(:read)
    assert supported_methods.include?(:read_multi)
    assert supported_methods.include?(:increment)
    assert supported_methods.include?(:decrement)
    assert supported_methods.include?(:exists?)
    assert supported_methods.include?(:delete)
    assert supported_methods.include?(:stats)
    refute supported_methods.include?(:clear)
    assert_raises(NotImplementedError) do
      store.clear
    end
    refute supported_methods.include?(:cleanup)
    assert_raises(NotImplementedError) do
      store.cleanup
    end
  end

  def test_it_writes_and_reads_the_data
    store.write uniq_id, @foobar
    assert_equal @foobar, store.read(uniq_id)
  end

  def test_it_writes_the_data_with_expiration_time
    store.write(uniq_id, @foobar, :expires_in => 1.second)
    assert_equal @foobar, store.read(uniq_id)
    sleep 2
    refute store.read(uniq_id)
  end

  def test_it_doest_write_data_if_unless_exist_option_is_true
    store.write uniq_id, @foo
    [:unless_exist, :unless_exists].each do |unless_exists|
      store.write uniq_id, @foobar, unless_exists => true
      assert_equal @foo, store.read(uniq_id)
    end
  end

  def test_it_reads_raw_data
    store.write uniq_id, @foo
    expected = case RUBY_VERSION
               when /^2\.0/
                 "\x04\bU:\x0FOpenStruct{\x06:\fpayloadI\"\bfoo\x06:\x06ET"
               when /^1\.9/
                 "\x04\bU:\x0FOpenStruct{\x06:\fpayloadI\"\bfoo\x06:\x06EF"
               else
                 "\004\bU:\017OpenStruct{\006:\fpayload\"\bfoo"
               end
    assert_equal expected, store.read(uniq_id, :raw => true)
  end

  def test_it_writes_raw_data
    store.write uniq_id, @foobar, :raw => true
    assert_equal '#<OpenStruct payload="foobar">', store.read(uniq_id, :raw => true)
  end

  def test_it_deletes_data
    store.write uniq_id, @foo
    store.delete uniq_id
    refute store.read(uniq_id)
  end

  def test_it_verifies_existence_of_an_object_in_the_store
    store.write uniq_id, @foo
    assert store.exist?(uniq_id)
    refute store.exist?(uniq_id(:missing))
  end

  def test_it_initializes_key_on_first_increment_with_zero
    store.increment(uniq_id)
    assert_equal 0, store.read(uniq_id)
    assert_equal "0", store.read(uniq_id, :raw => true)
  end

  def test_it_initializes_key_on_first_decrement_with_zero
    store.decrement(uniq_id)
    assert_equal 0, store.read(uniq_id)
    assert_equal "0", store.read(uniq_id, :raw => true)
  end

  def test_it_initializes_key_with_given_value_on_increment
    store.increment(uniq_id, 1, :initial => 5)
    assert_equal 5, store.read(uniq_id)
    assert_equal "5", store.read(uniq_id, :raw => true)
  end

  def test_it_initializes_key_with_given_value_on_decrement
    store.decrement(uniq_id, 1, :initial => 5)
    assert_equal 5, store.read(uniq_id)
    assert_equal "5", store.read(uniq_id, :raw => true)
  end

  def test_it_increments_a_key
    3.times { store.increment uniq_id }
    assert_equal 2, store.read(uniq_id)
    assert_equal "2", store.read(uniq_id, :raw => true)
  end

  def test_it_decrements_a_key
    4.times { store.increment uniq_id }
    2.times { store.decrement uniq_id }
    assert_equal 1, store.read(uniq_id)
    assert_equal "1", store.read(uniq_id, :raw => true)
  end

  def test_it_increments_a_raw_key
    assert store.write(uniq_id, 1, :raw => true)
    store.increment(uniq_id, 2)
    assert_equal 3, store.read(uniq_id, :raw => true).to_i
  end

  def test_it_decrements_a_raw_key
    assert store.write(uniq_id, 3, :raw => true)
    store.decrement(uniq_id, 2)
    assert_equal 1, store.read(uniq_id, :raw => true).to_i
  end

  def test_it_increments_a_key_by_given_value
    store.write(uniq_id, 0, :raw => true)
    store.increment uniq_id, 3
    assert_equal 3, store.read(uniq_id, :raw => true).to_i
  end

  def test_it_decrements_a_key_by_given_value
    store.write(uniq_id, 0, :raw => true)
    3.times { store.increment uniq_id }
    store.decrement uniq_id, 2
    assert_equal 1, store.read(uniq_id, :raw => true).to_i
  end

  def test_it_provides_store_stats
    refute store.stats.empty?
  end

  def test_it_fetches_data
    assert store.write(uniq_id, @foo)
    assert_equal @foo, store.fetch(uniq_id)
    refute store.fetch("rub-a-dub")
    store.fetch("rub-a-dub") { "Flora de Cana" }
    assert_equal "Flora de Cana", store.fetch("rub-a-dub")
    store.fetch(uniq_id, :force => true) # force cache miss
    store.fetch(uniq_id, :force => true, :expires_in => 1.second) { @foobar }
    assert_equal @foobar, store.fetch(uniq_id)
    sleep 2
    refute store.fetch(uniq_id)
  end

  def test_it_reads_multiple_keys
    assert store.write(uniq_id(1), @foo)
    assert store.write(uniq_id(2), "foo")
    result = store.read_multi uniq_id(1), uniq_id(2)
    assert_equal @foo, result[uniq_id(1)]
    assert_equal "foo", result[uniq_id(2)]
  end

  def test_it_reads_multiple_keys_and_returns_only_the_matched_ones
    assert store.write(uniq_id, @foo)
    result = store.read_multi uniq_id, uniq_id(:missing)
    assert result[uniq_id]
    refute result[uniq_id(:missing)]
  end

  def test_it_notifies_on_fetch
    collect_notifications do
      store.fetch(uniq_id) { "foo" }
    end

    read, generate, write = @events

    assert_equal 'cache_read.active_support', read.name
    assert_equal({:key => uniq_id, :super_operation => :fetch}, read.payload)

    assert_equal 'cache_generate.active_support', generate.name
    assert_equal({:key => uniq_id}, generate.payload)

    assert_equal 'cache_write.active_support', write.name
    assert_equal({:key => uniq_id}, write.payload)
  end

  def test_it_notifies_on_read
    collect_notifications do
      store.read uniq_id
    end

    read = @events.first
    assert_equal 'cache_read.active_support', read.name
    assert_equal({:key => uniq_id, :hit => false}, read.payload)
  end

  def test_it_notifies_on_write
    collect_notifications do
      store.write uniq_id, "foo"
    end

    write = @events.first
    assert_equal 'cache_write.active_support', write.name
    assert_equal({:key => uniq_id}, write.payload)
  end

  def test_it_notifies_on_delete
    collect_notifications do
      store.delete uniq_id
    end

    delete = @events.first
    assert_equal 'cache_delete.active_support', delete.name
    assert_equal({:key => uniq_id}, delete.payload)
  end

  def test_it_notifies_on_exist?
    collect_notifications do
      store.exist? uniq_id
    end

    exist = @events.first
    assert_equal 'cache_exists?.active_support', exist.name
    assert_equal({:key => uniq_id}, exist.payload)
  end

  def test_it_notifies_on_increment
    collect_notifications do
      store.increment uniq_id
    end

    increment = @events.first
    assert_equal 'cache_increment.active_support', increment.name
    assert_equal({:key => uniq_id, :amount => 1, :create => true}, increment.payload)
  end

  def test_it_notifies_on_decrement
    collect_notifications do
      store.decrement uniq_id
    end

    decrement = @events.first
    assert_equal 'cache_decrement.active_support', decrement.name
    assert_equal({:key => uniq_id, :amount => 1, :create => true}, decrement.payload)
  end

  # Inspiration: https://github.com/mperham/dalli/blob/master/test/test_dalli.rb#L416
  def test_it_is_threadsafe
    workers = []

    # Have a bunch of threads perform a bunch of operations at the same time.
    # Verify the result of each operation to ensure the request and response
    # are not intermingled between threads.
    10.times do
      workers << Thread.new do
        100.times do
          store.write('a', 9)
          store.write('b', 11)
          assert_equal 9, store.read('a')
          assert_equal({ 'a' => 9, 'b' => 11 }, store.read_multi('a', 'b'))
          assert_equal 11, store.read('b')
          assert_equal %w(a b), store.read_multi('a', 'b', 'c').keys.sort
        end
      end
    end

    workers.each { |w| w.join }
  end

  def test_it_can_use_connection_pool_for_thread_safety
    workers = []

    10.times do
      workers << Thread.new do
        100.times do
          pool_store.write('a', 9)
          pool_store.write('b', 11)
          assert_equal 9, pool_store.read('a')
          assert_equal({ 'a' => 9, 'b' => 11 }, pool_store.read_multi('a', 'b'))
          assert_equal 11, pool_store.read('b')
          assert_equal %w(a b), pool_store.read_multi('a', 'b', 'c').keys.sort
        end
      end
    end

    workers.each { |w| w.join }
  end

  # These tests are only relevant against a real server,
  # CouchbaseMock seems to accept long keys.
  def test_it_can_handle_keys_longer_than_250_characters
    long_key = 'a' * 260
    assert store.write(long_key, 123)
    assert_equal 123, store.read(long_key)
  end

  def test_it_can_handle_keys_longer_than_250_characters_with_a_prefix
    long_key = 'a' * 249
    assert prefixed_store.write(long_key, 123)
    assert_equal 123, prefixed_store.read(long_key)
  end

  private

  def collect_notifications
    @events = [ ]
    ActiveSupport::Cache::CouchbaseStore.instrument = true
    ActiveSupport::Notifications.subscribe(/^cache_(.*)\.active_support$/) do |*args|
      @events << ActiveSupport::Notifications::Event.new(*args)
    end
    yield
    ActiveSupport::Cache::CouchbaseStore.instrument = false
  end
end
