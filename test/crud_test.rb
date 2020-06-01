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

      @collection.upsert(doc_id, document)

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

    def test_that_insert_fails_when_document_exists_already
      doc_id = uniq_id(:foo)
      document = {"value" => 42}
      @collection.insert(doc_id, document)

      assert_raises(Couchbase::Error::DocumentExists) do
        @collection.insert(doc_id, document)
      end
    end

    def test_that_replace_fails_when_document_does_not_exist_yet
      doc_id = uniq_id(:foo)
      document = {"value" => 42}

      assert_raises(Couchbase::Error::DocumentNotFound) do
        @collection.replace(doc_id, document)
      end

      @collection.upsert(doc_id, document)

      document["value"] = 43
      @collection.replace(doc_id, document)

      res = @collection.get(doc_id)

      assert_equal 43, res.content["value"]
    end

    def test_that_replace_supports_optimistic_locking
      doc_id = uniq_id(:foo)
      document = {"value" => 42}

      res = @collection.upsert(doc_id, document)
      cas = res.cas

      options = Collection::ReplaceOptions.new
      options.cas = cas + 1 # incorrect CAS

      document["value"] = 43
      assert_raises(Couchbase::Error::CasMismatch) do
        @collection.replace(doc_id, document, options)
      end

      options.cas = cas # correct CAS
      @collection.replace(doc_id, document, options)

      res = @collection.get(doc_id)

      assert_equal 43, res.content["value"]
    end

    class BinaryTranscoder
      def encode(blob)
        [blob, 0]
      end

      def decode(blob, _flags)
        blob
      end
    end

    def test_that_it_increments_and_decrements_existing_binary_document
      doc_id = uniq_id(:foo)
      document = "42"

      options = Collection::UpsertOptions.new
      options.transcoder = BinaryTranscoder.new
      @collection.upsert(doc_id, document, options)

      res = @collection.binary.increment(doc_id)
      assert_equal 43, res.content

      options = Collection::GetOptions.new
      options.transcoder = BinaryTranscoder.new
      res = @collection.get(doc_id, options)
      assert_equal "43", res.content

      res = @collection.binary.decrement(doc_id)
      assert_equal 42, res.content

      options = Collection::GetOptions.new
      options.transcoder = BinaryTranscoder.new
      res = @collection.get(doc_id, options)
      assert_equal "42", res.content
    end

    def test_that_it_fails_to_increment_and_decrement_missing_document
      doc_id = uniq_id(:foo)

      assert_raises(Couchbase::Error::DocumentNotFound) do
        @collection.binary.increment(doc_id)
      end

      assert_raises(Couchbase::Error::DocumentNotFound) do
        @collection.binary.decrement(doc_id)
      end
    end

    def test_that_it_increment_and_decrement_can_initialize_document
      doc_id = uniq_id(:foo)

      options = BinaryCollection::IncrementOptions.new
      options.initial = 42
      res = @collection.binary.increment(doc_id, options)
      assert_equal 42, res.content
      res = @collection.binary.increment(doc_id, options)
      assert_equal 43, res.content

      options = Collection::GetOptions.new
      options.transcoder = BinaryTranscoder.new
      res = @collection.get(doc_id, options)
      assert_equal "43", res.content

      doc_id = uniq_id(:bar)
      options = BinaryCollection::DecrementOptions.new
      options.initial = 142
      res = @collection.binary.decrement(doc_id, options)
      assert_equal 142, res.content
      res = @collection.binary.decrement(doc_id, options)
      assert_equal 141, res.content

      options = Collection::GetOptions.new
      options.transcoder = BinaryTranscoder.new
      res = @collection.get(doc_id, options)
      assert_equal "141", res.content
    end

    def test_that_it_increment_and_decrement_can_use_custom_delta
      doc_id = uniq_id(:foo)

      options = BinaryCollection::IncrementOptions.new
      options.initial = 42
      options.delta = 50
      res = @collection.binary.increment(doc_id, options)
      assert_equal 42, res.content
      res = @collection.binary.increment(doc_id, options)
      assert_equal 92, res.content

      options = Collection::GetOptions.new
      options.transcoder = BinaryTranscoder.new
      res = @collection.get(doc_id, options)
      assert_equal "92", res.content

      doc_id = uniq_id(:bar)
      options = BinaryCollection::DecrementOptions.new
      options.initial = 142
      options.delta = 20
      res = @collection.binary.decrement(doc_id, options)
      assert_equal 142, res.content
      res = @collection.binary.decrement(doc_id, options)
      assert_equal 122, res.content

      options = Collection::GetOptions.new
      options.transcoder = BinaryTranscoder.new
      res = @collection.get(doc_id, options)
      assert_equal "122", res.content
    end
  end
end
