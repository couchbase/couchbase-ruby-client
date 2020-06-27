#    Copyright 2020 Couchbase, Inc.
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

require_relative "test_helper"

module Couchbase
  class SubdocTest < BaseTest
    def setup
      options = Cluster::ClusterOptions.new
      options.authenticate(TEST_USERNAME, TEST_PASSWORD)
      @cluster = Cluster.connect(TEST_CONNECTION_STRING, options)
      @bucket = @cluster.bucket(TEST_BUCKET)
      @collection = @bucket.default_collection
    end

    def teardown
      @cluster.disconnect
    end

    def uniq_id(name)
      "#{name}_#{Time.now.to_f}"
    end

    def test_mutate_in_increment
      doc_id = uniq_id(:foo)
      document = {"value" => 42}
      @collection.upsert(doc_id, document)

      @collection.mutate_in(
          doc_id,
          [
              MutateInSpec.increment("value", 4)
          ])

      expected = {"value" => 46}
      res = @collection.get(doc_id)
      assert_equal expected, res.content
    end
  end
end
