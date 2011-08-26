require File.join(File.dirname(__FILE__), 'setup')

class TestCouchdb < MiniTest::Unit::TestCase
  def setup
    @bucket = Couchbase.new('http://localhost:8091/pools/default')
  end

  def test_that_it_could_connect_to_couchdb
    server_uri = @bucket.next_node.couch_api_base[/http:\/\/[^\/]+/]
    assert_equal %w(couchdb version couchbase), @bucket.http_get(server_uri).keys
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
    @bucket.save_design_doc('_id' => "_design/#{test_id}",
                            'views' => {'all' => {'map' => 'function(doc){if(doc.msg){emit(doc._id, doc.msg)}}'}})
    assert_operation_completed { database_ready(@bucket) }
    ddoc = @bucket.design_docs[test_id]
    assert ddoc, "design document '_design/#{test_id}' not found"
    doc = ddoc.all.detect{|doc| doc['id'] == test_id && doc['value'] == 'hello world'}
    assert doc, "object '#{test_id}' not found"
  end

  def test_it_creates_design_doc_from_string
    doc = <<-EOD
      {
        "_id": "_design/#{test_id}",
        "language": "javascript",
        "views": {
          "all": {
            "map": "function(doc){ if(doc.msg){ emit(doc._id, doc.msg) } }"
          }
        }
      }
    EOD

    @bucket.delete_design_doc(test_id)
    @bucket.save_design_doc(doc)
    assert_operation_completed { database_ready(@bucket) }
    assert @bucket.design_docs[test_id], "design document '_design/#{test_id}' not found"
  end

  def test_it_creates_design_doc_from_io
    doc = File.open(File.join(File.dirname(__FILE__), 'support', 'sample_design_doc.json'))
    @bucket.delete_design_doc(test_id)
    @bucket.save_design_doc(doc)
    assert_operation_completed { database_ready(@bucket) }
    assert @bucket.design_docs[test_id], "design document '_design/#{test_id}' not found"
  end

  def test_it_creates_design_doc_from_hash
    doc = {
      "_id" => "_design/#{test_id}",
      "language" => "javascript",
      "views" => {
        "all" => {
          "map" => "function(doc){ if(doc.msg){ emit(doc._id, doc.msg) } }"
        }
      }
    }

    @bucket.delete_design_doc(test_id)
    @bucket.save_design_doc(doc)
    assert_operation_completed { database_ready(@bucket) }
    assert @bucket.design_docs[test_id], "design document '_design/#{test_id}' not found"
  end

  protected

  def test_id
    name = caller.first[/.*[` ](.*)'/, 1]
  end
end
