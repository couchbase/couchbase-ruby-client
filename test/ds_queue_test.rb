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

require "couchbase/datastructures/couchbase_queue"

module Couchbase
  module Datastructures
    class CouchbaseQueueTest < Minitest::Test
      include TestUtilities

      def setup
        connect
        @bucket = @cluster.bucket(env.bucket)
        @collection = @bucket.default_collection
      end

      def teardown
        disconnect
      end

      def test_new_queue_empty
        doc_id = uniq_id(:foo)
        queue = CouchbaseQueue.new(doc_id, @collection)
        assert_equal 0, queue.size
        assert_empty queue
      end

      def test_new_queue_yields_no_elements
        doc_id = uniq_id(:foo)
        queue = CouchbaseQueue.new(doc_id, @collection)
        actual = []
        queue.each do |element|
          actual << element
        end
        assert_equal [], actual
      end

      def test_implements_fifo
        skip("#{name}: CAVES does not support array indexes for Queue#pop yet") if use_caves?

        doc_id = uniq_id(:foo)
        queue = CouchbaseQueue.new(doc_id, @collection)

        queue.push("foo")
        queue.push("bar")
        queue.push(42)

        actual = []
        3.times do
          actual << queue.pop
        end
        assert_equal ["foo", "bar", 42], actual
      end

      def test_pop_from_empty_queue_returns_nil
        doc_id = uniq_id(:foo)
        queue = CouchbaseQueue.new(doc_id, @collection)

        assert_empty queue
        assert_nil queue.pop
      end
    end
  end
end
