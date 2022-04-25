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

require "couchbase/collection"
require "couchbase/query_options"
require "couchbase/analytics_options"

module Couchbase
  # The scope identifies a group of collections and allows high application density as a result.
  class Scope
    attr_reader :bucket_name
    attr_reader :name

    alias inspect to_s

    # @param [Couchbase::Backend] backend
    # @param [String] bucket_name name of the bucket
    # @param [String] scope_name name of the scope
    def initialize(backend, bucket_name, scope_name)
      @backend = backend
      @bucket_name = bucket_name
      @name = scope_name
    end

    # Opens the default collection for this scope
    #
    # @param [String] collection_name name of the collection
    #
    # @return [Collection]
    def collection(collection_name)
      Collection.new(@backend, @bucket_name, @name, collection_name)
    end

    # Performs a query against the query (N1QL) services.
    #
    # The query will be implicitly scoped using current bucket and scope names.
    #
    # @see Options::Query#scope_qualifier
    # @see Cluster#query
    #
    # @param [String] statement the N1QL query statement
    # @param [Options::Query] options the custom options for this query
    #
    # @example Select first ten hotels from travel sample dataset
    #   scope.query("SELECT * FROM `travel-sample` WHERE type = $type LIMIT 10",
    #                Options::Query(named_parameters: {type: "hotel"}, metrics: true))
    #
    # @return [QueryResult]
    def query(statement, options = Options::Query.new)
      resp = @backend.document_query(statement, options.to_backend(scope_name: @name, bucket_name: @bucket_name))

      Cluster::QueryResult.new do |res|
        res.meta_data = Cluster::QueryMetaData.new do |meta|
          meta.status = resp[:meta][:status]
          meta.request_id = resp[:meta][:request_id]
          meta.client_context_id = resp[:meta][:client_context_id]
          meta.signature = JSON.parse(resp[:meta][:signature]) if resp[:meta][:signature]
          meta.profile = JSON.parse(resp[:meta][:profile]) if resp[:meta][:profile]
          meta.metrics = Cluster::QueryMetrics.new do |metrics|
            if resp[:meta][:metrics]
              metrics.elapsed_time = resp[:meta][:metrics][:elapsed_time]
              metrics.execution_time = resp[:meta][:metrics][:execution_time]
              metrics.sort_count = resp[:meta][:metrics][:sort_count]
              metrics.result_count = resp[:meta][:metrics][:result_count]
              metrics.result_size = resp[:meta][:metrics][:result_size]
              metrics.mutation_count = resp[:meta][:metrics][:mutation_count]
              metrics.error_count = resp[:meta][:metrics][:error_count]
              metrics.warning_count = resp[:meta][:metrics][:warning_count]
            end
          end
          res[:warnings] = resp[:warnings].map { |warn| Cluster::QueryWarning.new(warn[:code], warn[:message]) } if resp[:warnings]
        end
        res.instance_variable_set(:@rows, resp[:rows])
      end
    end

    # Performs an analytics query
    #
    # The query will be implicitly scoped using current bucket and scope names.
    #
    # @param [String] statement the N1QL query statement
    # @param [Options::Analytics] options the custom options for this query
    #
    # @example Select name of the given user
    #   scope.analytics_query("SELECT u.name AS uname FROM GleambookUsers u WHERE u.id = $user_id ",
    #                          Options::Analytics(named_parameters: {user_id: 2}))
    #
    # @return [AnalyticsResult]
    def analytics_query(statement, options = Options::Analytics.new)
      resp = @backend.document_analytics(statement, options.to_backend(scope_name: @name, bucket_name: @bucket_name))

      Cluster::AnalyticsResult.new do |res|
        res.transcoder = options.transcoder
        res.meta_data = Cluster::AnalyticsMetaData.new do |meta|
          meta.status = resp[:meta][:status]
          meta.request_id = resp[:meta][:request_id]
          meta.client_context_id = resp[:meta][:client_context_id]
          meta.signature = JSON.parse(resp[:meta][:signature]) if resp[:meta][:signature]
          meta.profile = JSON.parse(resp[:meta][:profile]) if resp[:meta][:profile]
          meta.metrics = Cluster::AnalyticsMetrics.new do |metrics|
            if resp[:meta][:metrics]
              metrics.elapsed_time = resp[:meta][:metrics][:elapsed_time]
              metrics.execution_time = resp[:meta][:metrics][:execution_time]
              metrics.result_count = resp[:meta][:metrics][:result_count]
              metrics.result_size = resp[:meta][:metrics][:result_size]
              metrics.error_count = resp[:meta][:metrics][:error_count]
              metrics.warning_count = resp[:meta][:metrics][:warning_count]
              metrics.processed_objects = resp[:meta][:metrics][:processed_objects]
            end
          end
          res[:warnings] = resp[:warnings].map { |warn| Cluster::AnalyticsWarning.new(warn[:code], warn[:message]) } if resp[:warnings]
        end
        res.instance_variable_set(:@rows, resp[:rows])
      end
    end

    # Performs a Full Text Search (FTS) query
    #
    # @param [String] index_name the name of the search index
    # @param [SearchQuery] query the query tree
    # @param [Options::Search] options the query tree
    #
    # @example Return first 10 results of "hop beer" query and request highlighting
    #   cluster.search_query("travel_index", Cluster::SearchQuery.match_phrase("green"),
    #                        Options::Search(
    #                          limit: 10,
    #                          collections: ["landmark", "hotel"]
    #                          fields: %w[name],
    #                          highlight_style: :html,
    #                          highlight_fields: %w[name description]
    #                        ))
    #
    # @return [SearchResult]
    def search_query(index_name, query, options = Options::Search.new)
      resp = @backend.document_search(index_name, JSON.generate(query), options.to_backend(scope_name: @name))

      SearchResult.new do |res|
        res.meta_data = SearchMetaData.new do |meta|
          meta.metrics.max_score = resp[:meta_data][:metrics][:max_score]
          meta.metrics.error_partition_count = resp[:meta_data][:metrics][:error_partition_count]
          meta.metrics.success_partition_count = resp[:meta_data][:metrics][:success_partition_count]
          meta.metrics.took = resp[:meta_data][:metrics][:took]
          meta.metrics.total_rows = resp[:meta_data][:metrics][:total_rows]
          meta.errors = resp[:meta_data][:errors]
        end
        res.rows = resp[:rows].map do |r|
          SearchRow.new do |row|
            row.transcoder = options.transcoder
            row.index = r[:index]
            row.id = r[:id]
            row.score = r[:score]
            row.fragments = r[:fragments]
            row.locations = SearchRowLocations.new(
              r[:locations].map do |loc|
                SearchRowLocation.new do |location|
                  location.field = loc[:field]
                  location.term = loc[:term]
                  location.position = loc[:position]
                  location.start_offset = loc[:start_offset]
                  location.end_offset = loc[:end_offset]
                  location.array_positions = loc[:array_positions]
                end
              end
            ) unless r[:locations].empty?
            row.instance_variable_set(:@fields, r[:fields])
            row.explanation = JSON.parse(r[:explanation]) if r[:explanation]
          end
        end
        if resp[:facets]
          res.facets = resp[:facets].each_with_object({}) do |(k, v), o|
            facet = case options.facets[k]
                    when SearchFacet::SearchFacetTerm
                      SearchFacetResult::TermFacetResult.new do |f|
                        f.terms =
                          if v[:terms]
                            v[:terms].map do |t|
                              SearchFacetResult::TermFacetResult::TermFacet.new(t[:term], t[:count])
                            end
                          else
                            []
                          end
                      end
                    when SearchFacet::SearchFacetDateRange
                      SearchFacetResult::DateRangeFacetResult.new do |f|
                        f.date_ranges =
                          if v[:date_ranges]
                            v[:date_ranges].map do |r|
                              SearchFacetResult::DateRangeFacetResult::DateRangeFacet.new(r[:name], r[:count], r[:start_time], r[:end_time])
                            end
                          else
                            []
                          end
                      end
                    when SearchFacet::SearchFacetNumericRange
                      SearchFacetResult::NumericRangeFacetResult.new do |f|
                        f.numeric_ranges =
                          if v[:numeric_ranges]
                            v[:numeric_ranges].map do |r|
                              SearchFacetResult::NumericRangeFacetResult::NumericRangeFacet.new(r[:name], r[:count], r[:min], r[:max])
                            end
                          else
                            []
                          end
                      end
                    else
                      next # ignore unknown facet result
                    end
            facet.name = v[:name]
            facet.field = v[:field]
            facet.total = v[:total]
            facet.missing = v[:missing]
            facet.other = v[:other]
            o[k] = facet
          end
        end
      end
    end
  end
end
