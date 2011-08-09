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
    connection.save_design_doc('test_view',
                               'all' => {'map' => 'function(doc){if(doc.counter){emit(doc._id, doc)}}'},
                               'sum' => {'map' => 'function(doc){if(doc.counter){emit(doc.toy, doc.counter)}}',
                                         'reduce' => 'function(keys,values,rereduce){return sum(values)}'})
    assert connection.design_docs['test_view']

    assert_operation_completed do
      database_ready(connection) && connection.all_docs.count > 100
    end
    connection
  end

end
