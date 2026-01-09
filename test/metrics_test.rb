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
require_relative "utils/metrics"

module Couchbase
  class MetricsTest < Minitest::Test
    include TestUtilities

    EXISTING_DOC_ID = "metrics-test-doc"

    def setup
      @meter = TestMeter.new
      connect(Options::Cluster.new(meter: @meter))
      @bucket = @cluster.bucket(env.bucket)
      @collection = @bucket.default_collection
      @collection.upsert(EXISTING_DOC_ID, {foo: "bar"})
      @meter.reset
    end

    def teardown
      disconnect
    end

    def test_get_and_replace
      10.times do
        res = @collection.get(EXISTING_DOC_ID)
        @collection.replace(EXISTING_DOC_ID, {foo: uniq_id("content")}, Options::Replace.new(cas: res.cas))
      end

      assert_operation_metrics(
        10,
        operation_name: "get",
        service: "kv",
        bucket_name: env.bucket,
        scope_name: "_default",
        collection_name: "_default",
      )
      assert_operation_metrics(
        10,
        operation_name: "replace",
        service: "kv",
        bucket_name: env.bucket,
        scope_name: "_default",
        collection_name: "_default",
      )
    end

    def test_get_document_not_found
      assert_raises(Couchbase::Error::DocumentNotFound) do
        @collection.get(uniq_id(:does_not_exist))
      end

      assert_operation_metrics(
        1,
        operation_name: "get",
        service: "kv",
        bucket_name: env.bucket,
        scope_name: "_default",
        collection_name: "_default",
        error: "DocumentNotFound",
      )
    end

    def test_upsert
      @collection.upsert(uniq_id(:foo), {foo: "bar"})

      assert_operation_metrics(
        1,
        operation_name: "upsert",
        service: "kv",
        bucket_name: env.bucket,
        scope_name: "_default",
        collection_name: "_default",
      )
    end

    def test_cluster_level_query
      skip("#{name}: CAVES does not support query service") if use_caves?

      @cluster.query("SELECT 1=1")

      assert_operation_metrics(
        1,
        operation_name: "query",
        service: "query",
      )
    end

    def test_scope_level_query
      skip("#{name}: CAVES does not support query service") if use_caves?

      @bucket.default_scope.query("SELECT 1=1")

      assert_operation_metrics(
        1,
        operation_name: "query",
        service: "query",
        bucket_name: env.bucket,
        scope_name: "_default",
      )
    end

    def test_query_parsing_failure
      skip("#{name}: CAVES does not support query service") if use_caves?

      assert_raises(Couchbase::Error::ParsingFailure) do
        @bucket.default_scope.query("SEEEELECT 1=1")
      end

      assert_operation_metrics(
        1,
        operation_name: "query",
        service: "query",
        bucket_name: env.bucket,
        scope_name: "_default",
        error: "ParsingFailure",
      )
    end
  end
end
