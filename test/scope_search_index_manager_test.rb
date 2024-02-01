#  Copyright 2024. Couchbase, Inc.
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
  class ScopeSearchIndexManagerTest < Minitest::Test
    include TestUtilities

    def setup
      unless env.server_version.supports_scoped_search_indexes?
        skip("skipped for (#{env.server_version}) as the ScopeSearchIndexManagerTest requires scoped search index support")
      end

      @index_names = []

      connect
      @bucket = @cluster.bucket(env.bucket)
      @scope = @bucket.default_scope
      @mgr = @scope.search_indexes
    end

    def teardown
      @index_names.each do |name|
        @mgr.drop_index(name)
      rescue StandardError
        Error::IndexNotFound
      end
      disconnect
    end

    def get_search_index_name
      name = uniq_id(:scope_idx)
      @index_names << name
      name
    end

    def test_index_not_found
      [
        [:drop_index, ["test-index"]],
        [:get_indexed_documents_count, ["test-index"]],
        [:pause_ingest, ["test-index"]],
        [:resume_ingest, ["test-index"]],
        [:allow_querying, ["test-index"]],
        [:disallow_querying, ["test-index"]],
        [:freeze_plan, ["test-index"]],
        [:unfreeze_plan, ["test-index"]],
      ].each do |method_name, args|
        assert_raises(Error::IndexNotFound, "Expected #{method_name} to raise IndexNotFound") do
          @mgr.public_send(method_name, *args)
        end
      end
    end

    def test_index_crud
      index_name = uniq_id(:scope_idx)

      @mgr.upsert_index(
        Management::SearchIndex.new do |idx|
          idx.name = index_name
          idx.source_name = env.bucket
        end
      )

      # Upsert requires a UUID
      assert_raises(Error::IndexExists) do
        @mgr.upsert_index(
          Management::SearchIndex.new do |idx|
            idx.name = index_name
            idx.source_name = env.bucket
          end
        )
      end

      idx = @mgr.get_index(index_name)

      assert_equal index_name, idx.name
      assert_equal "default", idx.source_name

      @mgr.upsert_index(idx)

      indexes = @mgr.get_all_indexes

      refute_empty indexes

      @mgr.drop_index(index_name)

      assert_raises(Error::IndexNotFound) do
        @mgr.drop_index(index_name)
      end
    end
  end

  class ScopeSearchIndexManagerNotSupportedTest < Minitest::Test
    include TestUtilities

    def setup
      skip("Test requires not having scoped search index support") if env.server_version.supports_scoped_search_indexes?

      connect
      @bucket = @cluster.bucket(env.bucket)
      @scope = @bucket.default_scope
      @mgr = @scope.search_indexes
    end

    def teardown
      disconnect
    end

    def test_feature_not_available
      [
        [:get_index, ["test-index"]],
        [:get_all_indexes, []],
        [:upsert_index, [Management::SearchIndex.new { |idx| idx.name = "test-index" }]],
        [:drop_index, ["test-index"]],
        [:get_indexed_documents_count, ["test-index"]],
        [:pause_ingest, ["test-index"]],
        [:resume_ingest, ["test-index"]],
        [:allow_querying, ["test-index"]],
        [:disallow_querying, ["test-index"]],
        [:freeze_plan, ["test-index"]],
        [:unfreeze_plan, ["test-index"]],
        [:analyze_document, ["test-index", {foo: "bar"}]],
      ].each do |method_name, args|
        assert_raises(Error::FeatureNotAvailable, "Expected #{method_name} to raise FeatureNotAvailable") do
          @mgr.public_send(method_name, *args)
        end
      end
    end
  end
end
