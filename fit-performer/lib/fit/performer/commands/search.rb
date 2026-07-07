# frozen_string_literal: true

#  Copyright 2026-Present Couchbase, Inc.
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

require_relative 'search/search_query_command'
require_relative 'search/search_command'
require_relative 'search/options_builder'

module FIT
  module Performer
    module Commands
      module Search # rubocop:disable Metrics/ModuleLength
        MATCH_OPERATOR_MAP = {
          :SEARCH_MATCH_OPERATOR_OR => :or,
          :SEARCH_MATCH_OPERATOR_AND => :and,
        }.freeze

        def self.build_cluster_level_command(raw_cmd, cluster, cmd_kwargs)
          cmd_kwargs[:cluster] = cluster
          build_command(raw_cmd, cmd_kwargs)
        end

        def self.build_scope_level_command(raw_cmd, scope, cmd_kwargs)
          cmd_kwargs[:scope] = scope
          build_command(raw_cmd, cmd_kwargs)
        end

        def self.build_command(raw_cmd, cmd_kwargs)
          case raw_cmd
          when Protocol::SDK::Search::Search
            cmd_kwargs.update({
              index_name: raw_cmd.indexName,
              search_query: get_search_query(raw_cmd.query),
              stream_config: raw_cmd.stream_config,
            })
            cmd_kwargs[:raw_options] = raw_cmd.options if raw_cmd.has_options?
            cmd_kwargs[:fields_as] = raw_cmd.fields_as if raw_cmd.has_fields_as?
            SearchQueryCommand.create_command(**cmd_kwargs)
          when Protocol::SDK::Search::SearchWrapper
            cmd_kwargs.update({
              index_name: raw_cmd.search.indexName,
              search_request: get_search_request(raw_cmd.search.request),
              stream_config: raw_cmd.stream_config,
            })
            cmd_kwargs[:raw_options] = raw_cmd.search.options if raw_cmd.search.has_options?
            cmd_kwargs[:fields_as] = raw_cmd.fields_as if raw_cmd.has_fields_as?
            SearchCommand.create_command(**cmd_kwargs)
          else
            raise PerformerError, "#{raw_cmd.class} is not a valid search command type"
          end
        end

        def self.get_search_query(raw_search_query)
          query = raw_search_query.public_send(raw_search_query.query)
          case raw_search_query.query
          when :match
            Couchbase::Cluster::SearchQuery.match(query.match) do |q|
              q.field = query.field if query.has_field?
              q.analyzer = query.analyzer if query.has_analyzer?
              q.prefix_length = query.prefix_length if query.has_prefix_length?
              q.fuzziness = query.fuzziness if query.has_fuzziness?
              q.boost = query.boost if query.has_boost?
              q.operator = MATCH_OPERATOR_MAP[query.operator] if query.has_operator?
            end
          when :match_phrase
            Couchbase::Cluster::SearchQuery.match_phrase(query.match_phrase) do |q|
              q.field = query.field if query.has_field?
              q.analyzer = query.analyzer if query.has_analyzer?
              q.boost = query.boost if query.has_boost?
            end
          when :regexp
            Couchbase::Cluster::SearchQuery.regexp(query.regexp) do |q|
              q.field = query.field if query.has_field?
              q.boost = query.boost if query.has_boost?
            end
          when :query_string
            Couchbase::Cluster::SearchQuery.query_string(query.query) do |q|
              q.boost = query.boost if query.has_boost?
            end
          when :wildcard
            Couchbase::Cluster::SearchQuery.wildcard(query.wildcard) do |q|
              q.field = query.field if query.has_field?
              q.boost = query.boost if query.has_boost?
            end
          when :doc_id
            Couchbase::Cluster::SearchQuery.doc_id(*query.ids) do |q|
              q.boost = query.boost if query.has_boost?
            end
          when :search_boolean_field
            Couchbase::Cluster::SearchQuery.boolean_field(query.bool) do |q|
              q.field = query.field if query.has_field?
              q.boost = query.boost if query.has_boost?
            end
          when :date_range
            Couchbase::Cluster::SearchQuery.date_range do |q|
              if query.has_start?
                if query.has_inclusive_start?
                  q.start_time(query.start, query.inclusive_start)
                else
                  q.start_time(query.start)
                end
              end
              if query.has_end?
                if query.has_inclusive_end?
                  q.end_time(query.end, query.inclusive_end)
                else
                  q.end_time(query.end)
                end
              end
              q.date_time_parser = query.datetime_parser if query.has_datetime_parser?
              q.field = query.field if query.has_field?
              q.boost = query.boost if query.has_boost?
            end
          when :numeric_range
            Couchbase::Cluster::SearchQuery.numeric_range do |q|
              if query.has_min?
                if query.has_inclusive_min?
                  q.min(query.min, query.inclusive_min)
                else
                  q.min(query.min)
                end
              end
              if query.has_max?
                if query.has_inclusive_max?
                  q.max(query.max, query.inclusive_max)
                else
                  q.max(query.max)
                end
              end
              q.field = query.field if query.has_field?
              q.boost = query.boost if query.has_boost?
            end
          when :term_range
            Couchbase::Cluster::SearchQuery.term_range do |q|
              if query.has_min?
                if query.has_inclusive_min?
                  q.min(query.min, query.inclusive_min)
                else
                  q.min(query.min)
                end
              end
              if query.has_max?
                if query.has_inclusive_max?
                  q.max(query.max, query.inclusive_max)
                else
                  q.max(query.max)
                end
              end
              q.field = query.field if query.has_field?
              q.boost = query.boost if query.has_boost?
            end
          when :geo_distance
            Couchbase::Cluster::SearchQuery.geo_distance(query.location.lon, query.location.lat, query.distance) do |q|
              q.field = query.field if query.has_field?
              q.boost = query.boost if query.has_boost?
            end
          when :geo_bounding_box
            Couchbase::Cluster::SearchQuery.geo_bounding_box(query.top_left.lon, query.top_left.lat, query.bottom_right.lon,
                                                             query.bottom_right.lat) do |q|
              q.field = query.field if query.has_field?
              q.boost = query.boost if query.has_boost?
            end
          when :conjunction
            queries = query.conjuncts.map { |c| get_search_query(c) }
            Couchbase::Cluster::SearchQuery.conjuncts(*queries) do |q|
              q.boost = query.boost if query.has_boost?
            end
          when :disjunction
            queries = query.disjuncts.map { |d| get_search_query(d) }
            Couchbase::Cluster::SearchQuery.disjuncts(*queries) do |q|
              q.min = query.min if query.has_min?
              q.boost = query.boost if query.has_boost?
            end
          when :boolean
            Couchbase::Cluster::SearchQuery.booleans do |q|
              q.must(*query.must.map { |proto_query| get_search_query(proto_query) }) unless query.must.empty?
              q.should(*query.should.map { |proto_query| get_search_query(proto_query) }) unless query.should.empty?
              q.must_not(*query.must_not.map { |proto_query| get_search_query(proto_query) }) unless query.must_not.empty?
              q.should_min(query.should_min) if query.has_should_min?
              q.boost = query.boost if query.has_boost?
            end
          when :term
            Couchbase::Cluster::SearchQuery.term(query.term) do |q|
              q.field = query.field if query.has_field?
              q.fuzziness = query.fuzziness if query.has_fuzziness?
              q.prefix_length = query.prefix_length if query.has_prefix_length?
              q.boost = query.boost if query.has_boost?
            end
          when :prefix
            Couchbase::Cluster::SearchQuery.prefix(query.prefix) do |q|
              q.field = query.field if query.has_field?
              q.boost = query.boost if query.has_boost?
            end
          when :phrase
            Couchbase::Cluster::SearchQuery.phrase(*query.terms) do |q|
              q.field = query.field if query.has_field?
              q.boost = query.boost if query.has_boost?
            end
          when :match_all
            Couchbase::Cluster::SearchQuery.match_all
          when :match_none
            Couchbase::Cluster::SearchQuery.match_none
          else
            raise PerformerError, "Query type #{raw_search_query.query} not supported"
          end
        end

        def self.get_search_request(raw_search_request)
          if raw_search_request.has_vector_search?
            vector = get_vector_search(raw_search_request.vector_search)
            if raw_search_request.has_search_query?
              Couchbase::SearchRequest.new(vector).search_query(get_search_query(raw_search_request.search_query))
            else
              Couchbase::SearchRequest.new(vector)
            end
          elsif raw_search_request.has_search_query?
            Couchbase::SearchRequest.new(get_search_query(raw_search_request.search_query))
          else
            raise PerformerError, "The SDK does not support creating an empty SearchRequest"
          end
        end

        def self.get_vector_search(raw_vector_search)
          queries = raw_vector_search.vector_query.map do |raw_query|
            query_args = [
              raw_query.vector_field_name,
              raw_query.has_base64_vector_query? ? raw_query.base64_vector_query : raw_query.vector_query.to_a,
            ]

            Couchbase::VectorQuery.new(*query_args) do |q|
              if raw_query.has_options?
                q.num_candidates = raw_query.options.num_candidates if raw_query.options.has_num_candidates?
                q.boost = raw_query.options.boost if raw_query.options.has_boost?
                q.prefilter = get_search_query(raw_query.options.prefilter) if raw_query.options.has_prefilter?
              end
            end
          end

          if raw_vector_search.has_options?
            builder = OptionsBuilder.new(options: Couchbase::Options::VectorSearch.new, raw_options: raw_vector_search.options)
            builder.set_vector_query_combination
            Couchbase::VectorSearch.new(queries, builder.options)
          else
            Couchbase::VectorSearch.new(queries)
          end
        end
      end
    end
  end
end
