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

require "couchbase/datastructures/couchbase_set"

module Couchbase
  module Datastructures
    class CouchbaseSetTest < Minitest::Test
      include TestUtilities

      def setup
        connect
        @bucket = @cluster.bucket(env.bucket)
        @collection = @bucket.default_collection
      end

      def teardown
        disconnect
      end

      def test_new_set_empty
        doc_id = uniq_id(:foo)
        set = CouchbaseSet.new(doc_id, @collection)
        assert_equal 0, set.size
        assert_empty set
      end

      def test_new_set_yields_no_elements
        doc_id = uniq_id(:foo)
        set = CouchbaseSet.new(doc_id, @collection)
        actual = []
        set.each do |element|
          actual << element
        end
        assert_empty actual
      end

      def test_add_does_not_create_duplicates
        doc_id = uniq_id(:foo)
        set = CouchbaseSet.new(doc_id, @collection)

        set.add("foo")
        set.add("foo")

        actual = []
        set.each do |element|
          actual << element
        end
        assert_equal %w[foo], actual
      end

      def test_has_methods_to_check_inclusivity
        doc_id = uniq_id(:foo)
        set = CouchbaseSet.new(doc_id, @collection)

        set.add("foo").add("bar")

        refute_empty set
        assert_equal 2, set.size

        assert_includes set, "foo"
        assert_includes set, "bar"
        refute_includes set, "baz"
      end

      def test_removes_the_item
        doc_id = uniq_id(:foo)
        set = CouchbaseSet.new(doc_id, @collection)

        set.add("foo").add("bar")
        assert_equal 2, set.size

        set.delete("bar")
        assert_equal 1, set.size

        assert_includes set, "foo"
        refute_includes set, "bar"
      end
    end
  end
end
