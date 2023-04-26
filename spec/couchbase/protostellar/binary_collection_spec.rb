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

require "rspec"
require "couchbase/protostellar"

RSpec.describe Couchbase::Protostellar::BinaryCollection do
  subject(:binary_collection) { @binary_collection }

  let(:collection) { @collection }

  # rubocop:disable RSpec/BeforeAfterAll
  before(:all) do
    @cluster = connect("protostellar")
    @collection = default_collection(@cluster)
    @binary_collection = @collection.binary
  end
  # rubocop:enable RSpec/BeforeAfterAll

  it_behaves_like "a binary collection"
end
