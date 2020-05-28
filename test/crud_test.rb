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

require "test_helper"

module Couchbase
  class CrudTest < Minitest::Test
    def setup
      options = Cluster::ClusterOptions.new
      options.authenticate(TEST_USERNAME, TEST_PASSWORD)
      @cluster = Cluster.connect(TEST_CONNECTION_STRING, options)
      @collection = @cluster.bucket("default").default_collection
    end

    def teardown
      @cluster.disconnect
    end

    def doc_id(name)
      @ids_ ||= {}
      test_name = caller_locations[0].label
      @ids_[test_name] ||= {}
      @ids_[test_name][name] ||= "#{name}_#{Time.now.to_i}"
    end

    def test_that_it_create_documents
      document = {"value" => 42}
      @collection.upsert(doc_id(:foo), document)
      res = @collection.get(doc_id(:foo))
      assert_equal document, res.content
    end

    def test_that_it_removes_documents
      document = {"value" => 42}
      @collection.upsert(doc_id(:foo), document)

      @collection.remove(doc_id(:foo))

      assert_raises(Couchbase::Error::DocumentNotFound) do
        @collection.get(doc_id(:foo))
      end
    end
  end
end
