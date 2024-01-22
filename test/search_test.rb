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
  class SearchTest < Minitest::Test
    include TestUtilities

    def setup
      skip("#{name}: The #{Couchbase::Protostellar::NAME} protocol does not support search index management yet") if env.protostellar?

      connect
      skip("#{name}: CAVES does not support query service yet") if use_caves?
      @bucket = @cluster.bucket(env.bucket)
      @collection = @bucket.default_collection
      @index_name = "idx-#{@bucket.name}-#{rand(0..100_000)}"

      index = Management::SearchIndex.new
      index.type = "fulltext-index"
      index.name = @index_name
      index.source_type = "couchbase"
      index.source_name = @bucket.name
      index.params = {
        mapping: {
          types: {
            "character" => {
              properties: {
                "name" => {
                  fields: [
                    {
                      name: "name",
                      type: "text",
                      include_in_all: true,
                      include_term_vectors: true,
                      index: true,
                      store: true,
                      docvalues: true,
                    },
                  ],
                },
              },
            },
          },
        },
      }
      @cluster.search_indexes.upsert_index(index)
      loop do
        stats = @cluster.search_indexes.get_stats
        if stats["#{@bucket.name}:#{@index_name}:num_mutations_to_index"].to_i.zero? &&
           stats["#{@bucket.name}:#{@index_name}:num_pindexes_target"].to_i.positive? &&
           stats["#{@bucket.name}:#{@index_name}:num_pindexes_actual"] == stats["#{@bucket.name}:#{@index_name}:num_pindexes_target"]
          break
        end

        time_travel(0.1)
      end
    end

    def teardown
      disconnect
    end

    def test_simple_search
      doc_id = uniq_id(:foo)
      res = @collection.insert(doc_id, {"type" => "character", "name" => "Arthur"})
      mutation_state = MutationState.new(res.mutation_token)
      options = Cluster::SearchOptions.new
      options.consistent_with(mutation_state)
      options.limit = 100
      attempts = 0
      loop do
        begin
          break if attempts >= 30

          attempts += 1
          res = @cluster.search_query(@index_name, Cluster::SearchQuery.query_string("arthur"), options)
        rescue Error::ConsistencyMismatch
          sleep(0.5)
          retry
        end
        if res.rows.empty?
          sleep(0.5)
          next
        end

        assert_predicate res, :success?, "res=#{res.inspect}"
        refute_empty res.rows, "expected non empty result"
        assert res.rows.find { |row| row.id == doc_id }, "result expected to include #{doc_id}"
        break
      end
      warn "search with at_plus took #{attempts} attempts, probably server bug" if attempts > 1

      assert_operator attempts, :<, 20, "it is very suspicious that search with at_plus took more than 20 attempts (#{attempts})"
    end

    def test_doc_id_search_query
      doc_ids = [uniq_id(:foo), uniq_id(:bar)]
      res1 = @collection.insert(doc_ids[0], {"type" => "character", "name" => "Arthur"})
      res2 = @collection.insert(doc_ids[1], {"type" => "character", "name" => "Brodie"})
      mutation_state = MutationState.new(res1.mutation_token, res2.mutation_token)
      options = Cluster::SearchOptions.new
      options.consistent_with(mutation_state)
      options.limit = 100
      attempts = 0
      loop do
        begin
          break if attempts >= 30

          attempts += 1
          res = @cluster.search_query(@index_name, Cluster::SearchQuery.doc_id(*doc_ids), options)
        rescue Error::ConsistencyMismatch
          sleep(0.5)
          retry
        end
        if res.rows.empty?
          sleep(0.5)
          next
        end

        assert_predicate res, :success?, "res=#{res.inspect}"
        refute_empty res.rows, "expected non empty result"
        doc_ids.each do |doc_id|
          assert res.rows.find { |row| row.id == doc_id }, "result expected to include #{doc_id}"
        end
        break
      end
      warn "search took #{attempts} attempts, probably a server bug" if attempts > 1

      assert_operator attempts, :<, 20, "it is very suspicious that search took more than 20 attempts (#{attempts})"
    end

    def test_search_request_backend_encoding
      vec1 = [-0.00810323283, 0.0727998167, 0.0211895034, -0.0254271757]
      vec2 = [-0.005610323283, 0.023427998167, 0.0132511895034, 0.03466271757]

      expected_query = {
        prefix: "S",
        field: "cityName",
        boost: 1.0,
      }
      expected_vector_queries = [
        {
          field: "cityVector",
          vector: vec1,
          k: 3,
          boost: 0.7,
        },
        {
          field: "cityVector",
          vector: vec2,
          k: 2,
          boost: 0.3,
        },
      ]

      query = Cluster::SearchQuery.prefix("S") do |q|
        q.field = "cityName"
        q.boost = 1.0
      end

      vector_queries = [
        Cluster::VectorQuery.new("cityVector", vec1) do |q|
          q.num_candidates = 3
          q.boost = 0.7
        end,
        Cluster::VectorQuery.new("cityVector", vec2) do |q|
          q.num_candidates = 2
          q.boost = 0.3
        end,
      ]
      vector_search = Cluster::VectorSearch.new(vector_queries, Options::VectorSearch.new(vector_query_combination: :or))

      # Both requests should be equivalent
      requests = [
        Cluster::SearchRequest.new(vector_search).search_query(query),
        Cluster::SearchRequest.new(query).vector_search(vector_search),
      ]
      requests.each do |request|
        enc_query, enc_request = request.to_backend

        assert_equal expected_query, JSON.parse(enc_query, symbolize_names: true)
        assert_equal expected_vector_queries, JSON.parse(enc_request[:vector_search][:vector_queries], symbolize_names: true)
        assert_equal :or, enc_request[:vector_search][:vector_query_combination]
      end
    end

    def test_search_request_invalid_argument
      vec1 = [-0.00810323283, 0.0727998167, 0.0211895034, -0.0254271757]
      vec2 = [-0.005610323283, 0.023427998167, 0.0132511895034, 0.03466271757]

      query = Cluster::SearchQuery.prefix("S")
      vector_queries = [
        Cluster::VectorQuery.new("cityVector", vec1),
        Cluster::VectorQuery.new("cityVector", vec2),
      ]
      vector_search = Cluster::VectorSearch.new(vector_queries, Options::VectorSearch.new(vector_query_combination: :or))

      invalid_initializations = [
        proc { Cluster::SearchRequest.new(vector_search).vector_search(vector_search) },
        proc { Cluster::SearchRequest.new(query).vector_search(vector_search).vector_search(vector_search) },
        proc { Cluster::SearchRequest.new(query).search_query(query) },
        proc { Cluster::SearchRequest.new(vector_search).search_query(query).search_query(query) },
      ]

      invalid_initializations.each do |it|
        assert_raises(Error::InvalidArgument) { it.call }
      end
    end

    def test_vector_query_invalid_candidate_number
      vector_query = Cluster::VectorQuery.new("foo", [-1.1, 1.2]) do |q|
        q.num_candidates = 0
      end

      assert_raises(Error::InvalidArgument) do
        vector_query.to_json
      end
    end

    def test_vector_search_query_defaults_to_match_none
      vector_search = Cluster::VectorSearch.new(Cluster::VectorQuery.new("foo", [-1.1, 1.2]))
      enc_query, = Cluster::SearchRequest.new(vector_search).to_backend

      assert_equal Cluster::SearchQuery.match_none.to_json, enc_query
    end
  end
end
