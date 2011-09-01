require File.join(File.dirname(__FILE__), 'setup')

class TestView < MiniTest::Unit::TestCase
  def test_pagination
    connection = populate_database
    view = connection.design_docs['test_view'].all

    # fetch all records
    collection = view.fetch(:limit => 10)
    assert_equal 10, collection.size
    assert_equal 100, collection.total_entries

    # fetch with reduce
    view = connection.design_docs['test_view'].sum
    docs = view.fetch(:limit => 2, :group => true)
    assert_equal 2, docs.size
    assert_equal nil, docs.total_entries

    assert_instance_of Couchbase::View, connection.all_docs
  end

  def test_when_stream_has_errors_it_raises_error_by_default
    response = <<-EOR
      {
        "total_rows": 0,
        "rows": [ ],
        "errors": [
          {
            "from": "127.0.0.1:5984",
            "reason": "Design document `_design/testfoobar` missing in database `test_db_b`."
          },
          {
            "from": "http:// localhost:5984/_view_merge/",
            "reason": "Design document `_design/testfoobar` missing in database `test_db_c`."
          }
        ]
      }
    EOR
    view = Couchbase::View.new(stub(:curl_easy => response),
                               "http://localhost:5984/default/_design/test/_view/all")
    assert_raises(Couchbase::ViewError) do
      view.fetch
    end
  end

  def test_when_stream_has_errors_and_error_callback_provided_it_executes_the_callback
    response = <<-EOR
      {
        "total_rows": 0,
        "rows": [ ],
        "errors": [
          {
            "from": "127.0.0.1:5984",
            "reason": "Design document `_design/testfoobar` missing in database `test_db_b`."
          },
          {
            "from": "http:// localhost:5984/_view_merge/",
            "reason": "Design document `_design/testfoobar` missing in database `test_db_c`."
          }
        ]
      }
    EOR
    view = Couchbase::View.new(stub(:curl_easy => response),
                               "http://localhost:5984/default/_design/test/_view/all")
    errcount = 0
    view.on_error{|from, reason| errcount += 1}
    view.fetch
    assert 2, errcount
  end

  protected

  def populate_database
    connection = Couchbase.new('http://localhost:8091/pools/default')
    # clear storage
    connection.flush
    # save 100 documents
    toys = %w(buzz rex bo hamm slink potato)
    100.times do |t|
      connection["test_view_#{t}"]= {:counter => t+1, :toy => toys[t%6]}
    end

    connection.delete_design_doc('test_view')
    connection.save_design_doc('_id' => '_design/test_view',
                               'views' => {
                                 'all' => {'map' => 'function(doc){if(doc.counter){emit(doc._id, doc)}}'},
                                 'sum' => {'map' => 'function(doc){if(doc.counter){emit(doc.toy, doc.counter)}}',
                                           'reduce' => 'function(keys,values,rereduce){return sum(values)}'}})

    assert connection.design_docs['test_view']

    assert_operation_completed do
      database_ready(connection) && connection.all_docs.count > 100
    end
    connection
  end

end
