require 'minitest/autorun'
require 'couchbase'

class TestConnection < MiniTest::Unit::TestCase
  def test_initialization_low_level_drivers
    memcached_params = {:servers => ['web1:11211', 'web2:11211:3'], :prefix_key => 'app_'}
    couchdb_params = {:uri => 'http://db1:5984/'}
    connection = Couchbase.new(:memcached => memcached_params, :couchdb => couchdb_params)
    default_weight = Couchbase::Memcached::DEFAULTS[:default_weight]
    assert_equal ["web1:11211:#{default_weight}", 'web2:11211:3'], connection.memcached.servers
    assert_equal 'app_', connection.memcached.prefix_key
    assert_equal 'http://db1:5984/', connection.couchdb.uri
  end
end
