# frozen_string_literal: true

#  Copyright 2025-Present Couchbase, Inc.
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
require_relative "utils/tracing"

module Couchbase
  class TracingTest < Minitest::Test
    include TestUtilities

    def setup
      @tracer = TestTracer.new
      connect(Options::Cluster.new(tracer: @tracer))
      @bucket = @cluster.bucket(env.bucket)
      @collection = @bucket.default_collection
    end

    def teardown
      disconnect
    end

    def test_get
      parent = @tracer.request_span("parent_span")
      doc_id = uniq_id(:foo)
      assert_raises(Couchbase::Error::DocumentNotFound) do
        @collection.get(doc_id, Options::Get.new(parent_span: parent))
      end

      spans = @tracer.spans("get")

      assert_equal 1, spans.size
      assert_kv_span spans[0], "get", parent
    end

    def test_upsert
      parent = @tracer.request_span("parent_span")
      doc_id = uniq_id(:foo)
      @collection.upsert(doc_id, {foo: "bar"}, Options::Upsert.new(parent_span: parent))

      spans = @tracer.spans("upsert")

      assert_equal 1, spans.size
      assert_kv_span spans[0], "upsert", parent
      assert_has_request_encoding_span spans[0]
    end

    def test_replace
      parent = @tracer.request_span("parent_span")
      doc_id = uniq_id(:foo)
      assert_raises(Couchbase::Error::DocumentNotFound) do
        @collection.replace(doc_id, {foo: "bar"}, Options::Upsert.new(parent_span: parent))
      end

      spans = @tracer.spans("replace")

      assert_equal 1, spans.size
      assert_kv_span spans[0], "replace", parent
      assert_has_request_encoding_span spans[0]
    end

    def test_replace_durable
      parent = @tracer.request_span("parent_span")
      doc_id = uniq_id(:foo)
      assert_raises(Couchbase::Error::DocumentNotFound) do
        @collection.replace(doc_id, {foo: "bar"}, Options::Upsert.new(
                                                    parent_span: parent,
                                                    durability_level: :persist_to_majority,
                                                  ))
      end

      spans = @tracer.spans("replace")

      assert_equal 1, spans.size
      assert_kv_span spans[0], "replace", parent
      assert_has_request_encoding_span spans[0]
      assert_equal "persist_majority", spans[0].attributes["couchbase.durability"]
    end
  end
end
