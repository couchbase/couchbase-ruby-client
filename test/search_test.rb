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
          time_travel(0.5)
          retry
        end
        if res.rows.empty?
          time_travel(0.5)
          next
        end
        assert res.success?, "res=#{res.inspect}"
        refute_empty res.rows, "expected non empty result"
        assert res.rows.find { |row| row.id == doc_id }, "result expected to include #{doc_id}"
        break
      end
      warn "search with at_plus took #{attempts} attempts, probably server bug" if attempts > 1
      assert attempts < 20, "it is very suspicious that search with at_plus took more than 20 attempts (#{attempts})"
    end
  end
end
