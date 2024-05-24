#  Copyright 2020-2021 Couchbase, Inc.
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

require "couchbase/datastructures/couchbase_map"

module Couchbase
  module Datastructures
    class CouchbaseMapTest < Minitest::Test
      include TestUtilities

      def setup
        connect
        @bucket = @cluster.bucket(env.bucket)
        @collection = @bucket.default_collection
      end

      def teardown
        disconnect
      end

      def test_new_map_empty
        doc_id = uniq_id(:foo)
        map = CouchbaseMap.new(doc_id, @collection)

        assert_equal 0, map.size
        assert_empty map
      end

      def test_new_map_yields_no_elements
        doc_id = uniq_id(:foo)
        map = CouchbaseMap.new(doc_id, @collection)
        actual = map.map do |key, value|
          [key, value]
        end

        assert_empty actual
      end

      def test_existing_document_could_be_loaded_into_map
        doc_id = uniq_id(:foo)
        @collection.upsert(doc_id, {"foo" => "bar"})

        map = CouchbaseMap.new(doc_id, @collection)

        refute_empty map
        assert_equal 1, map.size
        assert_equal "bar", map["foo"]
      end

      def test_map_returns_nil_if_key_is_not_found
        doc_id = uniq_id(:foo)
        map = CouchbaseMap.new(doc_id, @collection)

        assert_empty map
        assert_nil map["foo"]
      end

      def test_map_allow_to_specify_default_on_fetch
        doc_id = uniq_id(:foo)
        map = CouchbaseMap.new(doc_id, @collection)

        assert_empty map
        assert_equal("baz", map.fetch("foo", "baz"))
        assert_equal("bar", map.fetch("foo") { "bar" })
      end

      def test_fetch_raises_key_error_when_default_not_specified
        doc_id = uniq_id(:foo)
        map = CouchbaseMap.new(doc_id, @collection)

        assert_empty map
        assert_raises KeyError do
          map.fetch("foo")
        end
      end

      def test_map_allows_to_check_if_key_is_present
        doc_id = uniq_id(:foo)
        @collection.upsert(doc_id, {"foo" => "bar"})

        map = CouchbaseMap.new(doc_id, @collection)

        refute_empty map
        assert map.key?("foo")
        refute map.key?("baz")
      end

      def test_map_returns_keys
        doc_id = uniq_id(:foo)
        @collection.upsert(doc_id, {"foo" => 100, "baz" => 42})

        map = CouchbaseMap.new(doc_id, @collection)

        refute_empty map
        assert_equal %w[baz foo], map.keys.sort
      end

      def test_map_returns_values
        doc_id = uniq_id(:foo)
        @collection.upsert(doc_id, {"foo" => 100, "baz" => 42})

        map = CouchbaseMap.new(doc_id, @collection)

        refute_empty map
        assert_equal [42, 100], map.values.sort
      end

      def test_changes_the_value_by_key
        doc_id = uniq_id(:foo)
        @collection.upsert(doc_id, {"foo" => 100, "baz" => 42})

        map = CouchbaseMap.new(doc_id, @collection)

        refute_empty map
        map["baz"] = "bar"

        pairs = map.map do |*pair|
          pair
        end

        assert_equal [%w[baz bar], ["foo", 100]], pairs.sort
      end

      def test_removes_key
        doc_id = uniq_id(:foo)
        @collection.upsert(doc_id, {"foo" => 100, "baz" => 42})

        map = CouchbaseMap.new(doc_id, @collection)

        refute_empty map

        map.delete("foo")

        assert_equal({"baz" => 42}, @collection.get(doc_id).content)
      end
    end
  end
end
