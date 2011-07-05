require 'minitest/autorun'
require 'couchbase'

class TestVersion < MiniTest::Unit::TestCase
  def test_that_it_defines_version
    assert_match /\d+\.\d+(\.\d*)/, Couchbase::VERSION
  end
end
