# frozen_string_literal: true

#  Copyright 2026-present Couchbase, Inc.
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

require "couchbase/protostellar/request_generator/search"

module Couchbase
  class SearchCustomQueryTest < Minitest::Test
    include TestUtilities

    def setup
      # Do nothing
    end

    def teardown
      # Do nothing
    end

    def test_custom_score_query
      query = Cluster::SearchQuery.custom_score(Cluster::SearchQuery.match("hotel"), "function(doc, params) { return doc.score; }") do |q|
        q.fields = %w[price rating]
        q.parameters = {boost: 2.5, nested: {a: [1, "b"]}}
      end

      assert_search_query_json(
        {
          "custom_score" => {
            "query" => {"match" => "hotel"},
            "source" => "function(doc, params) { return doc.score; }",
            "fields" => %w[price rating],
            "params" => {"boost" => 2.5, "nested" => {"a" => [1, "b"]}},
          },
        },
        query,
      )
    end

    def test_custom_filter_query
      query = Cluster::SearchQuery.custom_filter(Cluster::SearchQuery.match("hotel"), "function(doc, params) { return true; }") do |q|
        q.fields = ["*"]
        q.parameters = {budget: 100}
      end

      assert_search_query_json(
        {
          "custom_filter" => {
            "query" => {"match" => "hotel"},
            "source" => "function(doc, params) { return true; }",
            "fields" => ["*"],
            "params" => {"budget" => 100},
          },
        },
        query,
      )
    end

    def test_custom_score_query_omits_unset_options
      assert_search_query_json(
        {"custom_score" => {"query" => {"match" => "hotel"}, "source" => "src"}},
        Cluster::SearchQuery.custom_score(Cluster::SearchQuery.match("hotel"), "src"),
      )
    end

    def test_custom_filter_query_omits_unset_options
      assert_search_query_json(
        {"custom_filter" => {"query" => {"match" => "hotel"}, "source" => "src"}},
        Cluster::SearchQuery.custom_filter(Cluster::SearchQuery.match("hotel"), "src"),
      )
    end

    def test_custom_queries_in_compound_query
      query = Cluster::SearchQuery.conjuncts(
        Cluster::SearchQuery.custom_filter(Cluster::SearchQuery.match("hotel"), "filter"),
        Cluster::SearchQuery.custom_score(Cluster::SearchQuery.custom_filter(Cluster::SearchQuery.match("motel"), "inner-filter"), "score"),
      )

      assert_search_query_json(
        {
          "conjuncts" => [
            {"custom_filter" => {"query" => {"match" => "hotel"}, "source" => "filter"}},
            {
              "custom_score" => {
                "query" => {"custom_filter" => {"query" => {"match" => "motel"}, "source" => "inner-filter"}},
                "source" => "score",
              },
            },
          ],
        },
        query,
      )
    end

    def test_custom_queries_protostellar_feature_not_available
      queries = {
        score: Cluster::SearchQuery.custom_score(Cluster::SearchQuery.match("hotel"), "src"),
        filter: Cluster::SearchQuery.custom_filter(Cluster::SearchQuery.match("hotel"), "src"),
        conjunction: Cluster::SearchQuery.conjuncts(Cluster::SearchQuery.custom_score(Cluster::SearchQuery.match("hotel"), "src")),
        disjunction: Cluster::SearchQuery.disjuncts(Cluster::SearchQuery.custom_filter(Cluster::SearchQuery.match("hotel"), "src")),
        boolean: Cluster::SearchQuery.booleans do |q|
          q.must(Cluster::SearchQuery.custom_filter(Cluster::SearchQuery.match("hotel"), "src"))
        end,
      }

      generator = Protostellar::RequestGenerator::Search.new

      queries.each do |query_name, query|
        assert_raises(Error::FeatureNotAvailable, "expected FeatureNotAvailable for #{query_name} query") do
          generator.search_query_request("testindex", query, Options::Search::DEFAULT)
        end
      end
    end

    private

    def assert_search_query_json(expected, query)
      assert_equal expected, JSON.parse(JSON.generate(query))
    end
  end
end
