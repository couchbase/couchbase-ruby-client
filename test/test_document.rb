require 'minitest/autorun'
require 'couchbase'

class TestView < MiniTest::Unit::TestCase
  def test_that_it_defines_views_for_design_documents_only
    design_doc = Couchbase::Document.new(:mock, {'_id' => '_design/foo', 'views' => {'all' => {'map' => 'function(doc){}'}}})
    assert_respond_to(design_doc, :all)
    regular_doc = Couchbase::Document.new(:mock, {'_id' => 'foo', 'views' => {'all' => {'map' => 'function(doc){}'}}})
    refute_respond_to(regular_doc, :all)
  end
end
