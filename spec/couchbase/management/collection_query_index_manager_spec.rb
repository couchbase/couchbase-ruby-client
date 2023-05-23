# frozen_string_literal: true

require "rspec"

RSpec.describe Couchbase::Management::CollectionQueryIndexManager do
  subject(:manager) { @collection.query_indexes }

  # rubocop:disable RSpec/BeforeAfterAll
  before(:all) do
    bucket = test_bucket(connect_with_classic)
    scope_name = uniq_id(:scope)
    coll_name = uniq_id(:collection)
    coll_mgr = bucket.collections
    coll_mgr.create_scope(scope_name)

    coll_spec = Couchbase::Management::CollectionSpec.new do |s|
      s.name = coll_name
      s.scope_name = scope_name
      s.max_expiry = nil
    end

    success = retry_operation(duration: 5, error: StandardError) { coll_mgr.create_collection(coll_spec) }
    raise "Failed to create scope/collection" unless success

    @collection = bucket.scope(scope_name).collection(coll_name)

    # Upsert something in the collection to make sure it's been created
    success = retry_operation(duration: 5, error: StandardError) { @collection.upsert("foo", {"test" => 10}) }
    raise "Failed to create collection" unless success
  end
  # rubocop:enable RSpec/BeforeAfterAll

  it_behaves_like "a collection query index manager"
end
