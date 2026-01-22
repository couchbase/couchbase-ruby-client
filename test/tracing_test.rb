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
      @scope = @bucket.default_scope
      @collection = @bucket.default_collection

      @parent_span = @tracer.request_span("parent_span")
    end

    def teardown
      disconnect
    end

    def test_get
      doc_id = uniq_id(:foo)
      assert_raises(Couchbase::Error::DocumentNotFound) do
        @collection.get(doc_id, Options::Get.new(parent_span: @parent_span))
      end

      spans = @tracer.spans("get")

      assert_equal 1, spans.size
      assert_kv_span env, spans[0], "get", @collection, @parent_span
    end

    def test_upsert
      doc_id = uniq_id(:foo)
      @collection.upsert(doc_id, {foo: "bar"}, Options::Upsert.new(parent_span: @parent_span))

      spans = @tracer.spans("upsert")

      assert_equal 1, spans.size
      assert_kv_span env, spans[0], "upsert", @collection, @parent_span
      assert_has_request_encoding_span env, spans[0]
    end

    def test_replace
      doc_id = uniq_id(:foo)
      assert_raises(Couchbase::Error::DocumentNotFound) do
        @collection.replace(doc_id, {foo: "bar"}, Options::Upsert.new(parent_span: @parent_span))
      end

      spans = @tracer.spans("replace")

      assert_equal 1, spans.size
      assert_kv_span env, spans[0], "replace", @collection, @parent_span
      assert_has_request_encoding_span env, spans[0]
    end

    def test_replace_durable
      doc_id = uniq_id(:foo)
      assert_raises(Couchbase::Error::DocumentNotFound) do
        @collection.replace(doc_id, {foo: "bar"}, Options::Upsert.new(
                                                    parent_span: @parent_span,
                                                    durability_level: :persist_to_majority,
                                                  ))
      end

      spans = @tracer.spans("replace")

      assert_equal 1, spans.size
      assert_kv_span env, spans[0], "replace", @collection, @parent_span
      assert_has_request_encoding_span env, spans[0]
      assert_equal "persist_majority", spans[0].attributes["couchbase.durability"]
    end

    def test_append
      doc_id = uniq_id(:foo)
      @collection.upsert(doc_id, "foo", Options::Upsert.new(
                                          transcoder: RawBinaryTranscoder.new,
                                        ))
      @collection.binary.append(doc_id, "bar", Options::Append.new(
                                                 parent_span: @parent_span,
                                               ))

      spans = @tracer.spans("append")

      assert_equal 1, spans.size
      assert_kv_span env, spans[0], "append", @collection, @parent_span
    end

    def test_increment
      doc_id = uniq_id(:foo)
      res = @collection.binary.increment(doc_id, Options::Increment.new(
                                                   delta: 10,
                                                   initial: 0,
                                                   parent_span: @parent_span,
                                                 ))

      assert_equal 0, res.content

      res = @collection.binary.increment(doc_id, Options::Increment.new(
                                                   delta: 10,
                                                   parent_span: @parent_span,
                                                 ))

      assert_equal 10, res.content

      spans = @tracer.spans("increment")

      assert_equal 2, spans.size
      spans.each do |span|
        assert_kv_span env, span, "increment", @collection, @parent_span
      end
    end

    def test_get_all_replicas
      num_replicas = @cluster.buckets.get_bucket(env.bucket).num_replicas

      doc_id = uniq_id(:foo)
      @collection.upsert(doc_id, {"foo" => "bar"}, Options::Upsert.new(
                                                     durability_level: :majority,
                                                   ))

      res = @collection.get_all_replicas(doc_id, Options::GetAllReplicas.new(
                                                   parent_span: @parent_span,
                                                 ))

      res.each do |r|
        assert_equal({"foo" => "bar"}, r.content)
      end

      spans = @tracer.spans("get_all_replicas")

      assert_equal 1, spans.size
      assert_compound_kv_span(
        env, spans[0], "get_all_replicas", @collection, @parent_span,
        child_op_names: Array.new(1, "get") + Array.new(num_replicas, "get_replica"),
        enforce_child_ordering: false
      )
    end

    def test_analytics_query
      skip("#{name}: CAVES does not support analytics service yet") if use_caves?

      res = @cluster.analytics_query(
        "SELECT 1=1 AS result",
        Options::Analytics.new(
          parent_span: @parent_span,
        ),
      )

      assert_equal({"result" => true}, res.rows.first)

      spans = @tracer.spans("analytics")

      assert_equal 1, spans.size
      assert_http_span(
        env, spans.first, "analytics", @parent_span,
        service: "analytics",
        statement: nil # No db.query.text attribute if no parameters are used
      )
    end

    def test_analytics_query_with_parameters
      skip("#{name}: CAVES does not support analytics service yet") if use_caves?

      res = @cluster.analytics_query(
        "SELECT $value AS result",
        Options::Analytics.new(
          named_parameters: {value: 10},
          parent_span: @parent_span,
        ),
      )

      assert_equal({"result" => 10}, res.rows.first)

      spans = @tracer.spans("analytics")

      assert_equal 1, spans.size
      assert_http_span(
        env, spans.first, "analytics", @parent_span,
        service: "analytics",
        statement: "SELECT $value AS result"
      )
    end

    def test_analytics_manager_get_all_datasets
      skip("#{name}: CAVES does not support analytics service yet") if use_caves?

      res = @cluster.analytics_indexes.get_all_datasets(
        Management::Options::Analytics::GetAllDatasets.new(
          parent_span: @parent_span,
        ),
      )

      assert_kind_of Array, res

      spans = @tracer.spans("manager_analytics_get_all_datasets")

      assert_equal 1, spans.size
      assert_http_span(
        env, spans.first, "manager_analytics_get_all_datasets", @parent_span,
        service: "analytics"
      )
    end

    def test_get_all_buckets
      res = @cluster.buckets.get_all_buckets(
        Management::Options::Bucket::GetAllBuckets.new(
          parent_span: @parent_span,
        ),
      )

      assert(res.any? { |b| b.name == env.bucket })

      spans = @tracer.spans("manager_buckets_get_all_buckets")

      assert_equal 1, spans.size
      assert_http_span(
        env, spans.first, "manager_buckets_get_all_buckets", @parent_span,
        service: "management"
      )
    end

    def test_get_all_scopes
      res = @bucket.collections.get_all_scopes(
        Management::Options::Collection::GetAllScopes.new(
          parent_span: @parent_span,
        ),
      )

      assert(res.any? { |b| b.name == @scope.name })

      spans = @tracer.spans("manager_collections_get_all_scopes")

      assert_equal 1, spans.size
      assert_http_span(
        env, spans.first, "manager_collections_get_all_scopes", @parent_span,
        bucket_name: @bucket.name,
        service: "management"
      )
    end

    def test_get_scope
      res = @bucket.collections.get_scope(
        @scope.name,
        Management::Options::Collection::GetAllScopes.new(
          parent_span: @parent_span,
        ),
      )

      assert_equal @scope.name, res.name

      spans = @tracer.spans("manager_collections_get_scope")

      assert_equal 1, spans.size
      assert_compound_http_span(
        env, spans.first, "manager_collections_get_scope", @parent_span,
        bucket_name: @bucket.name,
        scope_name: @scope.name,
        service: "management",
        child_count: 1
      ) do |child_span|
        assert_http_span(
          env, child_span, "manager_collections_get_all_scopes", spans.first,
          bucket_name: @bucket.name,
          service: "management"
        )
      end
    end

    def test_drop_collection
      collection_name = uniq_id(:non_existent_collection)
      assert_raises Couchbase::Error::CollectionNotFound do
        @bucket.collections.drop_collection(
          @scope.name,
          collection_name,
          Management::Options::Collection::DropCollection.new(
            parent_span: @parent_span,
          ),
        )
      end

      spans = @tracer.spans("manager_collections_drop_collection")

      assert_equal 1, spans.size
      assert_http_span(
        env, spans.first, "manager_collections_drop_collection", @parent_span,
        bucket_name: @bucket.name,
        scope_name: @scope.name,
        collection_name: collection_name,
        service: "management"
      )
    end

    def test_query_indexes_get_all
      skip("#{name}: CAVES does not support query service") if use_caves?

      res = @cluster.query_indexes.get_all_indexes(
        env.bucket,
        Management::Options::Query::GetAllIndexes.new(
          parent_span: @parent_span,
        ),
      )

      assert_kind_of Array, res

      spans = @tracer.spans("manager_query_get_all_indexes")

      assert_equal 1, spans.size
      assert_http_span(
        env, spans.first, "manager_query_get_all_indexes", @parent_span,
        service: "query",
        bucket_name: env.bucket
      )
    end

    def test_collection_query_indexes_get_all
      skip("#{name}: CAVES does not support query service") if use_caves?

      res = @collection.query_indexes.get_all_indexes(
        Management::Options::Query::GetAllIndexes.new(
          parent_span: @parent_span,
        ),
      )

      assert_kind_of Array, res

      spans = @tracer.spans("manager_query_get_all_indexes")

      assert_equal 1, spans.size
      assert_http_span(
        env, spans.first, "manager_query_get_all_indexes", @parent_span,
        service: "query",
        bucket_name: @collection.bucket_name,
        scope_name: @collection.scope_name,
        collection_name: @collection.name
      )
    end

    def test_cluster_level_query
      skip("#{name}: CAVES does not support query service") if use_caves?

      res = @cluster.query(
        "SELECT 1=1 AS result",
        Options::Query.new(
          parent_span: @parent_span,
        ),
      )

      assert_equal({"result" => true}, res.rows.first)

      spans = @tracer.spans("query")

      assert_equal 1, spans.size
      assert_http_span(
        env, spans.first, "query", @parent_span,
        service: "query",
        statement: nil # No db.query.text attribute if no parameters are used
      )
    end

    def test_cluster_level_query_with_parameters
      skip("#{name}: CAVES does not support query service") if use_caves?

      res = @cluster.query(
        "SELECT $1 AS result",
        Options::Query.new(
          parent_span: @parent_span,
          positional_parameters: [10],
        ),
      )

      assert_equal({"result" => 10}, res.rows.first)

      spans = @tracer.spans("query")

      assert_equal 1, spans.size
      assert_http_span(
        env, spans.first, "query", @parent_span,
        service: "query",
        statement: "SELECT $1 AS result"
      )
    end

    def test_scope_level_query
      skip("#{name}: CAVES does not support query service") if use_caves?

      res = @scope.query(
        "SELECT 1=1 AS result",
        Options::Query.new(
          parent_span: @parent_span,
        ),
      )

      assert_equal({"result" => true}, res.rows.first)

      spans = @tracer.spans("query")

      assert_equal 1, spans.size
      assert_http_span(
        env, spans.first, "query", @parent_span,
        service: "query",
        statement: nil, # No db.query.text attribute if no parameters are used
        bucket_name: @scope.bucket_name,
        scope_name: @scope.name
      )
    end

    def test_scope_level_query_with_parameters
      skip("#{name}: CAVES does not support query service") if use_caves?

      res = @scope.query(
        "SELECT $1 AS result",
        Options::Query.new(
          parent_span: @parent_span,
          positional_parameters: [10],
        ),
      )

      assert_equal({"result" => 10}, res.rows.first)

      spans = @tracer.spans("query")

      assert_equal 1, spans.size
      assert_http_span(
        env, spans.first, "query", @parent_span,
        service: "query",
        statement: "SELECT $1 AS result",
        bucket_name: @scope.bucket_name,
        scope_name: @scope.name
      )
    end

    def test_get_all_users
      res = @cluster.users.get_all_users(
        Management::Options::User::GetAllUsers.new(
          parent_span: @parent_span,
        ),
      )

      assert_kind_of Array, res

      spans = @tracer.spans("manager_users_get_all_users")

      assert_equal 1, spans.size
      assert_http_span(
        env, spans.first, "manager_users_get_all_users", @parent_span,
        service: "management"
      )
    end

    def test_cluster_level_search_get_all_indexes
      skip("#{name}: CAVES does not support FTS") if use_caves?

      res = @cluster.search_indexes.get_all_indexes(
        Management::Options::Search::GetAllIndexes.new(
          parent_span: @parent_span,
        ),
      )

      assert_kind_of Array, res

      spans = @tracer.spans("manager_search_get_all_indexes")

      assert_equal 1, spans.size
      assert_http_span(
        env, spans.first, "manager_search_get_all_indexes", @parent_span,
        service: "search"
      )
    end

    def test_scope_level_search_get_all_indexes
      skip("#{name}: CAVES does not support FTS") if use_caves?
      skip("#{name}: Server does not support scoped FTS indexes") unless env.server_version.supports_scoped_search_indexes?

      res = @scope.search_indexes.get_all_indexes(
        Management::Options::Search::GetAllIndexes.new(
          parent_span: @parent_span,
        ),
      )

      assert_kind_of Array, res

      spans = @tracer.spans("manager_search_get_all_indexes")

      assert_equal 1, spans.size
      assert_http_span(
        env, spans.first, "manager_search_get_all_indexes", @parent_span,
        bucket_name: @scope.bucket_name,
        scope_name: @scope.name,
        service: "search"
      )
    end

    def test_cluster_level_search_query
      skip("#{name}: CAVES does not support FTS") if use_caves?

      assert_raises Couchbase::Error::IndexNotFound do
        @cluster.search(
          uniq_id(:non_existent_index),
          Couchbase::SearchRequest.new(Couchbase::SearchQuery.match_none),
          Options::Search.new(
            parent_span: @parent_span,
          ),
        )
      end

      spans = @tracer.spans("search")

      assert_equal 1, spans.size
      assert_http_span(
        env, spans.first, "search", @parent_span,
        service: "search"
      )
    end

    def test_scope_level_search_query
      skip("#{name}: CAVES does not support FTS") if use_caves?
      skip("#{name}: Server does not support scoped FTS indexes") unless env.server_version.supports_scoped_search_indexes?

      assert_raises Couchbase::Error::IndexNotFound do
        @scope.search(
          uniq_id(:non_existent_index),
          Couchbase::SearchRequest.new(Couchbase::SearchQuery.match_none),
          Options::Search.new(
            parent_span: @parent_span,
          ),
        )
      end

      spans = @tracer.spans("search")

      assert_equal 1, spans.size
      assert_http_span(
        env, spans.first, "search", @parent_span,
        bucket_name: @scope.bucket_name,
        scope_name: @scope.name,
        service: "search"
      )
    end
  end
end
