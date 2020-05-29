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

    def uniq_id(name)
      "#{name}_#{Time.now.to_f}"
    end

    def test_that_it_create_documents
      doc_id = uniq_id(:foo)
      document = {"value" => 42}
      @collection.upsert(doc_id, document)
      res = @collection.get(doc_id)
      assert_equal document, res.content
    end

    def test_that_it_removes_documents
      doc_id = uniq_id(:foo)
      document = {"value" => 42}
      @collection.upsert(doc_id, document)

      @collection.remove(doc_id)

      assert_raises(Couchbase::Error::DocumentNotFound) do
        @collection.get(doc_id)
      end
    end

    def test_that_touch_sets_expiration
      document = {"value" => 42}
      doc_id = uniq_id(:foo)

      @collection.upsert(doc_id, document)

      @collection.touch(doc_id, 1)

      sleep(2)

      assert_raises(Couchbase::Error::DocumentNotFound) do
        @collection.get(doc_id)
      end
    end

    def test_that_get_can_also_set_expiration
      document = {"value" => 42}
      doc_id = uniq_id(:foo)

      res = @collection.upsert(doc_id, document)
      cas = res.cas

      res = @collection.get_and_touch(doc_id, 1)
      assert_equal 42, res.content["value"]

      sleep(2)

      assert_raises(Couchbase::Error::DocumentNotFound) do
        @collection.get(doc_id)
      end
    end

    def test_that_exists_allows_to_check_document_existence
      doc_id = uniq_id(:foo)

      res = @collection.exists(doc_id)
      refute res.exists?

      document = {"value" => 42}
      res = @collection.upsert(doc_id, document)
      cas = res.cas

      res = @collection.exists(doc_id)
      assert res.exists?
      assert_equal cas, res.cas
    end

    def test_that_get_and_lock_protects_document_from_mutations
      doc_id = uniq_id(:foo)
      document = {"value" => 42}
      @collection.upsert(doc_id, document)

      res = @collection.get_and_lock(doc_id, 1)
      cas = res.cas

      document["value"] += 1
      assert_raises(Couchbase::Error::DocumentLocked) do
        @collection.upsert(doc_id, document)
      end

      @collection.unlock(doc_id, cas)

      @collection.upsert(doc_id, document)
    end
  end
end
