require 'minitest/autorun'
require 'couchbase'
require 'ruby-debug'

class TestCouchdb < MiniTest::Unit::TestCase
  def setup
    @couchbase = Couchbase.new
  end

  def test_that_it_could_connect_to_couchdb
    debugger
    @couchbase
    true
  end
end


