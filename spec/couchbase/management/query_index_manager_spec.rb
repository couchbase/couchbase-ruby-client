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

RSpec.describe Couchbase::Management::QueryIndexManager do
  subject(:manager) { @cluster.query_indexes }

  # rubocop:disable RSpec/BeforeAfterAll
  before(:all) do
    @cluster = connect_with_classic
  end
  # rubocop:enable RSpec/BeforeAfterAll

  it_behaves_like "a query index manager"
end
