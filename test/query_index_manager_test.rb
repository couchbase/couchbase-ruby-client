# frozen_string_literal: true

#  Copyright 2023 Couchbase, Inc.
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

require "securerandom"
require_relative "test_helper"
require_relative "utils/tracing"

module Couchbase
  class QueryIndexManagerTest < Minitest::Test
    include TestUtilities

    def setup
      return if use_caves?

      @tracer = TestTracer.new
      connect(Couchbase::Options::Cluster.new(tracer: @tracer))
      @bucket_name = "query-idx-#{SecureRandom.uuid[0..5]}"
      @cluster.buckets.create_bucket(
        Couchbase::Management::BucketSettings.new do |s|
          s.name = @bucket_name
          s.ram_quota_mb = 256
        end,
      )
      env.consistency.wait_until_bucket_present(@bucket_name)
      env.consistency.wait_until_bucket_present_in_indexes(@bucket_name)
      @idx_mgr = @cluster.query_indexes
      @parent_span = @tracer.request_span("parent_span")
      @tracer.reset
    end

    def teardown
      return if use_caves?

      @cluster.buckets.drop_bucket(@bucket_name)
      disconnect
    end

    def test_get_all_indexes
      skip("#{name}: CAVES does not support query service yet") if use_caves?

      index_names = [uniq_id(:foo), uniq_id(:bar)]
      index_names.each do |idx_name|
        @idx_mgr.create_index(@bucket_name, idx_name, ["foo"], Management::Options::Query::CreateIndex.new(parent_span: @parent_span))
      end

      res = @idx_mgr.get_all_indexes(@bucket_name, Management::Options::Query::GetAllIndexes.new(parent_span: @parent_span))

      assert_predicate res.length, :positive?
      index_names.each do |idx_name|
        assert_includes(res.map(&:name), idx_name)
      end

      get_all_indexes_spans = @tracer.spans("manager_query_get_all_indexes")

      assert_equal 1, get_all_indexes_spans.size
      assert_http_span(
        get_all_indexes_spans.first,
        "manager_query_get_all_indexes",
        parent: @parent_span,
        service: "query",
        bucket_name: @bucket_name,
      )

      create_index_spans = @tracer.spans("manager_query_create_index")

      assert_equal 2, create_index_spans.size
      create_index_spans.each do |span|
        assert_http_span(
          span,
          "manager_query_create_index",
          parent: @parent_span,
          service: "query",
          bucket_name: @bucket_name,
        )
      end
    end

    def test_query_indexes
      skip("#{name}: CAVES does not support query service yet") if use_caves?

      @idx_mgr.create_primary_index(@bucket_name, Management::Options::Query::CreatePrimaryIndex.new(parent_span: @parent_span))
      @idx_mgr.create_index(@bucket_name, "test_index", ["test"], Management::Options::Query::CreateIndex.new(parent_span: @parent_span))
      res1 = @idx_mgr.get_all_indexes(@bucket_name, Management::Options::Query::GetAllIndexes.new(parent_span: @parent_span))

      assert_empty %w[test_index #primary] - res1.map(&:name)
      assert_equal 2, res1.size

      @idx_mgr.create_index(@bucket_name, "test_index_deferred", ["something"],
                            Management::Options::Query::CreateIndex.new(deferred: true, parent_span: @parent_span))
      res2 = @idx_mgr.get_all_indexes(@bucket_name, Management::Options::Query::GetAllIndexes.new(parent_span: @parent_span))

      assert_equal 3, res2.size
      assert_equal :deferred, res2.find { |idx| idx.name == "test_index_deferred" }.state

      @idx_mgr.build_deferred_indexes(@bucket_name, Management::Options::Query::BuildDeferredIndexes.new(parent_span: @parent_span))
      @idx_mgr.watch_indexes(@bucket_name, ["test_index_deferred"], 50000,
                             Management::Options::Query::WatchIndexes.new(parent_span: @parent_span))
      res3 = @idx_mgr.get_all_indexes(@bucket_name, Management::Options::Query::GetAllIndexes.new(parent_span: @parent_span))

      assert_equal 3, res3.size
      assert_equal :online, res3.find { |idx| idx.name == "test_index_deferred" }.state

      assert_raises(Error::IndexNotFound) do
        @idx_mgr.watch_indexes(@bucket_name, ["non_existent_index"], 50000,
                               Management::Options::Query::WatchIndexes.new(parent_span: @parent_span))
      end

      @idx_mgr.drop_primary_index(@bucket_name, Management::Options::Query::DropPrimaryIndex.new(parent_span: @parent_span))
      @idx_mgr.drop_index(@bucket_name, "test_index_deferred", Management::Options::Query::DropIndex.new(parent_span: @parent_span))
      res4 = @idx_mgr.get_all_indexes(@bucket_name, Management::Options::Query::GetAllIndexes.new(parent_span: @parent_span))

      assert_equal 1, res4.size

      get_all_indexes_root_spans = @tracer.spans("manager_query_get_all_indexes", parent: @parent_span)

      assert_equal 4, get_all_indexes_root_spans.size
      get_all_indexes_root_spans.each do |span|
        assert_http_span(
          span,
          "manager_query_get_all_indexes",
          parent: @parent_span,
          service: "query",
          bucket_name: @bucket_name,
        )
      end

      create_primary_index_spans = @tracer.spans("manager_query_create_primary_index", parent: @parent_span)

      assert_equal 1, create_primary_index_spans.size
      assert_http_span(
        create_primary_index_spans.first,
        "manager_query_create_primary_index",
        parent: @parent_span,
        service: "query",
        bucket_name: @bucket_name,
      )

      create_index_spans = @tracer.spans("manager_query_create_index", parent: @parent_span)

      assert_equal 2, create_index_spans.size
      create_index_spans.each do |span|
        assert_http_span(
          span,
          "manager_query_create_index",
          parent: @parent_span,
          service: "query",
          bucket_name: @bucket_name,
        )
      end

      build_deferred_indexes_spans = @tracer.spans("manager_query_build_deferred_indexes", parent: @parent_span)

      assert_equal 1, build_deferred_indexes_spans.size
      assert_http_span(
        build_deferred_indexes_spans.first,
        "manager_query_build_deferred_indexes",
        parent: @parent_span,
        service: "query",
        bucket_name: @bucket_name,
      )

      watch_indexes_spans = @tracer.spans("manager_query_watch_indexes", parent: @parent_span)

      assert_equal 2, watch_indexes_spans.size
      watch_indexes_spans.each do |span|
        assert_http_span(
          span,
          "manager_query_watch_indexes",
          parent: @parent_span,
          service: "query",
          bucket_name: @bucket_name,
        )

        assert_predicate span.children.size, :positive?
        span.children.each do |child_span|
          assert_http_span(
            child_span,
            "manager_query_get_all_indexes",
            parent: span,
            service: "query",
            bucket_name: @bucket_name,
          )
        end
      end
    end
  end
end
