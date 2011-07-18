require 'minitest/autorun'
require 'couchbase'

class TestCouchdb < MiniTest::Unit::TestCase
  def setup
    @connection = Couchbase.new('http://localhost:8091/pools/default')
  end

  def test_that_it_could_connect_to_couchdb
    server_uri = @connection.bucket.next_node.couch_api_base[/http:\/\/[^\/]+/]
    assert_equal %w(couchdb version), @connection.http_get(server_uri).keys
  end

  def test_that_it_could_create_doc_using_memcached_api
    # cleanup
    @connection.delete(test_id) rescue Memcached::NotFound
    @connection.delete_design_doc(test_id)

    @connection[test_id] = {'msg' => 'hello world'}
    @connection.save_design_doc(test_id, 'all' => {'map' => 'function(doc){if(doc.msg){emit(doc._id, doc.msg)}}'})
    ddoc = @connection.design_docs[test_id]
    assert ddoc, "design document '_design/#{test_id}' not found"
    doc = ddoc.all.detect{|doc| doc['id'] == test_id && doc['value'] == 'hello world'}
    assert doc, "object '#{test_id}' not found"
  end

  protected

  def test_id
    name = caller.first[/.*[` ](.*)'/, 1]
  end
end
