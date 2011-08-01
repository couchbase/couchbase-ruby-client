require 'minitest/autorun'
require 'couchbase'

class TestCouchdb < MiniTest::Unit::TestCase
  def setup
    @bucket = Couchbase.new('http://localhost:8091/pools/default')
  end

  def test_that_it_could_connect_to_couchdb
    server_uri = @bucket.next_node.couch_api_base[/http:\/\/[^\/]+/]
    assert_equal %w(couchdb version), @bucket.http_get(server_uri).keys
  end

  def test_that_it_raises_error_if_couch_api_isnt_implemented
    bucket = Couchbase.new('http://localhost:8091/pools/default')
    bucket.nodes.each{|node| node.couch_api_base = nil}
    assert_raises(Couchbase::NotImplemented) do
      bucket.design_docs
    end
  end

  def test_that_it_could_create_doc_using_memcached_api
    # cleanup
    @bucket.delete(test_id) rescue Memcached::NotFound
    @bucket.delete_design_doc(test_id)

    @bucket[test_id] = {'msg' => 'hello world'}
    @bucket.save_design_doc(test_id, 'all' => {'map' => 'function(doc){if(doc.msg){emit(doc._id, doc.msg)}}'})
    ddoc = @bucket.design_docs[test_id]
    assert ddoc, "design document '_design/#{test_id}' not found"
    doc = ddoc.all.detect{|doc| doc['id'] == test_id && doc['value'] == 'hello world'}
    assert doc, "object '#{test_id}' not found"
  end

  protected

  def test_id
    name = caller.first[/.*[` ](.*)'/, 1]
  end
end
