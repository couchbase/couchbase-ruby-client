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
require "couchbase"

RSpec.describe Couchbase::Bucket do
  subject(:bucket) { @bucket }

  # rubocop:disable RSpec/BeforeAfterAll
  before(:all) do
    @cluster = connect_with_classic
    @bucket = test_bucket(@cluster)
  end
  # rubocop:enable RSpec/BeforeAfterAll

  it_behaves_like "a bucket"
end
