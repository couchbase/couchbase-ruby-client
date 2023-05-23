# frozen_string_literal: true

#  Copyright 2023. Couchbase, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

require 'rspec'

RSpec.describe Couchbase::Protostellar::Management::CollectionQueryIndexManager do
  subject(:manager) { collection.query_indexes }

  let(:collection) { @collection }

  # rubocop:disable RSpec/BeforeAfterAll
  before(:all) do
    bucket = test_bucket(connect_with_protostellar)
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
    sleep(1) # TODO: Need a delay because if the first attempt fails all of them fail indefinitely (Remove when fixed)
    success = retry_operation(duration: 5) { @collection.upsert("foo", {"test" => 10}) }
    raise "Failed to create collection" unless success
  end
  # rubocop:enable RSpec/BeforeAfterAll

  it_behaves_like "a collection query index manager"
end
