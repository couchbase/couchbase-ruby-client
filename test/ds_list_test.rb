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

require "couchbase/datastructures/couchbase_list"

module Couchbase
  module Datastructures
    class CouchbaseListTest < Minitest::Test
      include TestUtilities

      def setup
        connect
        @bucket = @cluster.bucket(env.bucket)
        @collection = @bucket.default_collection
      end

      def teardown
        disconnect
      end

      def test_new_list_empty
        doc_id = uniq_id(:foo)
        list = CouchbaseList.new(doc_id, @collection)
        assert_equal 0, list.size
        assert_empty list
      end

      def test_new_list_yields_no_elements
        doc_id = uniq_id(:foo)
        list = CouchbaseList.new(doc_id, @collection)
        actual = []
        list.each do |element|
          actual << element
        end
        assert_empty actual
      end

      def test_at_returns_nil_for_new_list
        doc_id = uniq_id(:foo)
        list = CouchbaseList.new(doc_id, @collection)
        refute list.at(0)
      end

      def test_push_creates_new_list
        doc_id = uniq_id(:foo)
        list = CouchbaseList.new(doc_id, @collection)
        list.push(1, 2, 3)
        assert_equal 3, list.size
        refute_empty list

        list = CouchbaseList.new(doc_id, @collection)
        assert_equal 3, list.size
        refute_empty list
      end

      def test_unshift_creates_new_list
        doc_id = uniq_id(:foo)
        list = CouchbaseList.new(doc_id, @collection)
        list.unshift(1, 2, 3)
        assert_equal 3, list.size
        refute_empty list

        list = CouchbaseList.new(doc_id, @collection)
        assert_equal 3, list.size
        refute_empty list
      end

      def test_clear_drops_the_list
        doc_id = uniq_id(:foo)
        list = CouchbaseList.new(doc_id, @collection)
        list.push(1, 2, 3)
        assert_equal 3, list.size

        list.clear
        assert_raises(Error::DocumentNotFound) do
          @collection.get(doc_id)
        end
        assert_empty list
      end

      def test_at_returns_last_entry
        doc_id = uniq_id(:foo)
        list = CouchbaseList.new(doc_id, @collection)
        list.push(1, 2, 3)
        assert_equal 3, list.size
        assert_equal 3, list.at(-1)
      end

      def test_removes_last_entry
        doc_id = uniq_id(:foo)
        list = CouchbaseList.new(doc_id, @collection)
        list.push(1, 2, 3)
        assert_equal 3, list.size

        list.delete_at(-1)

        result = @collection.get(doc_id)
        assert_equal [1, 2], result.content
      end

      def test_inserts_into_center_of_the_list
        doc_id = uniq_id(:foo)
        list = CouchbaseList.new(doc_id, @collection)
        list.push(1, 2, 3, 4)

        list.insert(2, "hello", "world")

        result = @collection.get(doc_id)
        assert_equal [1, 2, "hello", "world", 3, 4], result.content
      end
    end
  end
end
