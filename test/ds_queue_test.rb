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
    class CouchbaseQueueTest < BaseTest
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

      def test_new_queue_empty
        doc_id = uniq_id(:foo)
        queue = CouchbaseQueue.new(doc_id, @collection)
        assert_equal 0, queue.size
        assert queue.empty?
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

        assert queue.empty?
        assert_nil queue.pop
      end
    end
  end
end
