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

module Couchbase
  class QueryTest < Minitest::Test
    include TestUtilities

    def setup
      if env.server_version.is_rcbc_408_applicable?
        skip("skipped for (#{env.server_version}) because of query_context known issue, see RCBC-408")
      end
      connect
      skip("#{name}: CAVES does not support query service yet") if use_caves?
      @bucket = @cluster.bucket(env.bucket)
      @collection = @bucket.default_collection
      options = Management::QueryIndexManager::CreatePrimaryIndexOptions.new
      options.ignore_if_exists = true
      options.timeout = 300_000 # give it up to 5 minutes
      @cluster.query_indexes.create_primary_index(@bucket.name, options)
    end

    def teardown
      disconnect
    end

    def test_simple_query
      res = @cluster.query('SELECT "ruby rules" AS greeting')

      assert_equal "ruby rules", res.rows.first["greeting"]
    end

    def test_cas_representation_consistency
      doc_id = uniq_id(:foo)
      res = @collection.insert(doc_id, {"self_id" => doc_id})
      cas = res.cas

      res = @cluster.query("SELECT META() AS meta FROM `#{@bucket.name}` WHERE self_id = $doc_id",
                           Options::Query(scan_consistency: :request_plus, named_parameters: {doc_id: doc_id}))

      assert_equal doc_id, res.rows.first["meta"]["id"]
      assert_equal cas, res.rows.first["meta"]["cas"]
    end

    def test_query_with_metrics
      options = Cluster::QueryOptions.new
      options.metrics = true
      options.timeout = 200_000 # 200 seconds
      res = @cluster.query('SELECT "ruby rules" AS greeting', options)

      assert_equal "ruby rules", res.rows.first["greeting"]

      metrics = res.meta_data.metrics

      assert_predicate metrics.elapsed_time, :positive?
      assert_predicate metrics.execution_time, :positive?
      assert_equal 0, metrics.error_count
      assert_equal 0, metrics.warning_count
      assert_equal 1, metrics.result_count
    end

    def test_query_with_all_options
      doc_id = uniq_id(:foo)
      @collection.insert(doc_id, {"foo" => "bar"})

      options = Cluster::QueryOptions.new
      options.adhoc = true
      options.client_context_id = "123"
      options.max_parallelism = 3
      options.metrics = true
      options.pipeline_batch = 1
      options.pipeline_cap = 1
      options.readonly = true
      options.scan_cap = 10
      options.scan_consistency = :request_plus
      options.scan_wait = 50
      options.timeout = 200_000 # 200 seconds

      res = @cluster.query("SELECT * FROM `#{@bucket.name}` WHERE META().id = \"#{doc_id}\"", options)

      assert_equal :success, res.meta_data.status
      assert_equal "123", res.meta_data.client_context_id
      assert res.meta_data.metrics
    end

    def test_readonly_violation
      options = Cluster::QueryOptions.new
      options.readonly = true

      assert_raises Error::InternalServerFailure do
        @cluster.query("INSERT INTO `#{@bucket.name}` (key, value) VALUES (\"foo\", \"bar\")", options)
      end
    end

    def test_select
      doc_id = uniq_id(:foo)
      @collection.insert(doc_id, {"foo" => "bar"})

      options = Cluster::QueryOptions.new
      options.scan_consistency = :request_plus
      options.timeout = 200_000 # 200 seconds

      res = @cluster.query("SELECT * FROM `#{@bucket.name}` AS doc WHERE META().id = \"#{doc_id}\"", options)

      assert res.meta_data.request_id
      assert res.meta_data.client_context_id
      assert_equal :success, res.meta_data.status
      refute res.meta_data.warnings
      assert res.meta_data.signature

      rows = res.rows

      assert_equal 1, rows.size
      assert_equal({"foo" => "bar"}, rows.first["doc"])
    end

    def test_select_with_profile
      options = Cluster::QueryOptions.new
      options.timeout = 200_000 # 200 seconds

      options.profile = :off
      res = @cluster.query('SELECT "ruby rules" AS greeting', options)

      refute res.meta_data.profile

      options.profile = :timings
      res = @cluster.query('SELECT "ruby rules" AS greeting', options)

      assert_kind_of Hash, res.meta_data.profile

      options.profile = :phases
      res = @cluster.query('SELECT "ruby rules" AS greeting', options)

      assert_kind_of Hash, res.meta_data.profile
    end

    def test_parsing_error_on_bad_query
      assert_raises(Error::ParsingFailure) do
        @cluster.query('BAD QUERY')
      end
    end

    def test_query_with_named_parameters
      doc_id = uniq_id(:foo)
      @collection.insert(doc_id, {"foo" => "bar"})

      options = Cluster::QueryOptions.new
      options.scan_consistency = :request_plus
      options.named_parameters("id" => doc_id)
      options.timeout = 200_000 # 200 seconds

      res = @cluster.query("SELECT `#{@bucket.name}`.* FROM `#{@bucket.name}` WHERE META().id = $id", options)

      assert_equal 1, res.rows.size
      assert_equal({"foo" => "bar"}, res.rows.first)
    end

    def test_query_with_positional_parameters
      doc_id = uniq_id(:foo)
      @collection.insert(doc_id, {"foo" => "bar"})

      options = Cluster::QueryOptions.new
      options.scan_consistency = :request_plus
      options.positional_parameters([doc_id])
      options.timeout = 200_000 # 200 seconds

      res = @cluster.query("SELECT `#{@bucket.name}`.* FROM `#{@bucket.name}` WHERE META().id = $1", options)

      assert_equal 1, res.rows.size
      assert_equal({"foo" => "bar"}, res.rows.first)
    end

    def test_consistent_with
      doc_id = uniq_id(:foo)
      res = @collection.insert(doc_id, {"foo" => "bar"})

      options = Cluster::QueryOptions.new
      options.consistent_with(MutationState.new(res.mutation_token))
      options.positional_parameters([doc_id])
      options.timeout = 200_000 # 200 seconds

      res = @cluster.query("SELECT `#{@bucket.name}`.* FROM `#{@bucket.name}` WHERE META().id = $1", options)

      assert_equal 1, res.rows.size
      assert_equal({"foo" => "bar"}, res.rows.first)
    end

    def __wait_for_collections_manifest(uid)
      hits = 5 # make sure that the manifest has distributed well enough
      while hits.positive?
        backend = @cluster.instance_variable_get(:@backend)
        manifest = backend.collections_manifest_get(@bucket.name, 10_000)
        if manifest[:uid] < uid
          time_travel(0.1)
          next
        end
        hits -= 1
      end
    end

    def retry_on(exception)
      attempts = 5
      begin
        yield
      rescue exception => e
        attempts -= 1
        retry if attempts.positive?
        raise e
      end
    end

    def test_scoped_query
      skip("The server does not support scoped queries (#{env.server_version})") unless env.server_version.supports_scoped_queries?

      scope_name = uniq_id(:scope).delete("-")[0, 30]
      collection_name = uniq_id(:collection).delete("-")[0, 30]

      manager = @bucket.collections
      ns_uid = manager.create_scope(scope_name)
      __wait_for_collections_manifest(ns_uid)
      spec = Management::CollectionSpec.new
      spec.scope_name = scope_name
      spec.name = collection_name
      ns_uid = manager.create_collection(spec)
      __wait_for_collections_manifest(ns_uid)
      time_travel(3)

      manager = @cluster.query_indexes
      options = Management::QueryIndexManager::CreatePrimaryIndexOptions.new
      options.collection_name = collection_name
      options.scope_name = scope_name
      retry_on(Couchbase::Error::ScopeNotFound) do
        manager.create_primary_index(@bucket.name, options)
        time_travel(1)
      end

      scope = @bucket.scope(scope_name)
      collection = scope.collection(collection_name)

      local_doc = uniq_id(:foo)
      collection.insert(local_doc, {"location" => "local"})

      global_doc = uniq_id(:foo)
      @collection.insert(global_doc, {"location" => "global"})

      options = Cluster::QueryOptions.new
      options.scan_consistency = :request_plus
      options.positional_parameters([[global_doc, local_doc]])
      options.timeout = 200_000 # 200 seconds

      res = @cluster.query("SELECT location FROM `#{@bucket.name}` WHERE META().id IN $1", options)

      assert_equal 1, res.rows.size
      assert_equal({"location" => "global"}, res.rows.first)

      res = @cluster.query("SELECT location FROM `#{@bucket.name}`.#{scope_name}.#{collection_name} WHERE META().id IN $1", options)

      assert_equal 1, res.rows.size
      assert_equal({"location" => "local"}, res.rows.first)

      res = scope.query("SELECT location FROM #{collection_name} WHERE META().id IN $1", options)

      assert_equal 1, res.rows.size
      assert_equal({"location" => "local"}, res.rows.first)
    end
  end
end
