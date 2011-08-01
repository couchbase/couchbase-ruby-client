require 'minitest/autorun'
require 'couchbase'

class TestRestClient < MiniTest::Unit::TestCase
  class RestClientMock
    extend Couchbase::RestClient
  end

  def test_that_build_uri_duplicates_string
    uri = "http://example.com/"
    assert_equal "http://example.com/?foo=bar", RestClientMock.send(:build_query, uri, {'foo' => 'bar'})
    assert_equal "http://example.com/", uri
  end
end
