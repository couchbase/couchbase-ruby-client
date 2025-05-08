#  Copyright 2023. Couchbase, Inc.
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

# frozen_string_literal: true

require "google/protobuf/well_known_types"

require "couchbase/search_options"

require "couchbase/protostellar/generated/search/v1/search_pb"
require "couchbase/protostellar/request"

module Couchbase
  module Protostellar
    module RequestGenerator
      class Search
        HIGHLIGHT_STYLE_MAP = {
          :html => :HIGHLIGHT_STYLE_HTML,
          :ansi => :HIGHLIGHT_STYLE_ANSI,
          nil => :HIGHLIGHT_STYLE_DEFAULT,
        }.freeze

        MATCH_QUERY_OPERATOR_MAP = {
          :or => :OPERATOR_OR,
          :and => :OPERATOR_AND,
        }.freeze

        SCAN_CONSISTENCY_MAP = {
          :not_bounded => :SCAN_CONSISTENCY_NOT_BOUNDED,
        }.freeze

        DEFAULT_LIMIT = 10
        DEFAULT_SKIP = 0

        def search_query_request(index_name, query, options)
          proto_opts = {
            scan_consistency: SCAN_CONSISTENCY_MAP[options.scan_consistency],
            include_explanation: options.explain,
            highlight_style: HIGHLIGHT_STYLE_MAP[options.highlight_style],
            disable_scoring: options.disable_scoring,
            include_locations: options.include_locations,
          }

          proto_opts[:highlight_fields] = options.highlight_fields unless options.highlight_fields.nil?
          proto_opts[:fields] = options.fields unless options.fields.nil?
          proto_opts[:sort] = get_sort(options) unless options.sort.nil?
          proto_opts[:limit] = options.limit || DEFAULT_LIMIT
          proto_opts[:skip] = options.skip || DEFAULT_SKIP
          proto_opts[:collections] = options.collections unless options.collections.nil?

          proto_req = Generated::Search::V1::SearchQueryRequest.new(
            query: create_query(query),
            index_name: index_name,
            **proto_opts,
          )

          # Set the search facets in the request
          get_facets(options).each do |key, facet|
            proto_req.facets[key] = facet
          end

          create_search_request(proto_req, :search_query, options)
        end

        private

        def create_search_request(proto_request, rpc, options, idempotent: false)
          Request.new(
            service: :search,
            rpc: rpc,
            proto_request: proto_request,
            timeout: options.timeout,
            idempotent: idempotent,
          )
        end

        def create_query(query)
          sq = Couchbase::Cluster::SearchQuery
          res = Generated::Search::V1::Query.new

          case query
          when sq::BooleanFieldQuery
            res.boolean_field_query = create_boolean_field_query(query)
          when sq::BooleanQuery
            res.boolean_query = create_boolean_query(query)
          when sq::ConjunctionQuery
            res.conjunction_query = create_conjunction_query(query)
          when sq::DateRangeQuery
            res.date_range_query = create_date_range_query(query)
          when sq::DisjunctionQuery
            res.disjunction_query = create_disjunction_query(query)
          when sq::DocIdQuery
            res.doc_id_query = create_doc_id_query(query)
          when sq::GeoBoundingBoxQuery
            res.geo_bounding_box_query = create_geo_bounding_box_query(query)
          when sq::GeoDistanceQuery
            res.geo_distance_query = create_geo_distance_query(query)
          when sq::GeoPolygonQuery
            res.geo_polygon_query = create_geo_polygon_query(query)
          when sq::MatchAllQuery
            res.match_all_query = create_match_all_query(query)
          when sq::MatchNoneQuery
            res.match_none_query = create_match_none_query(query)
          when sq::MatchPhraseQuery
            res.match_phrase_query = create_match_phrase_query(query)
          when sq::MatchQuery
            res.match_query = create_match_query(query)
          when sq::NumericRangeQuery
            res.numeric_range_query = create_numeric_range_query(query)
          when sq::PhraseQuery
            res.phrase_query = create_phrase_query(query)
          when sq::PrefixQuery
            res.prefix_query = create_prefix_query(query)
          when sq::QueryStringQuery
            res.query_string_query = create_query_string_query(query)
          when sq::RegexpQuery
            res.regexp_query = create_regexp_query(query)
          when sq::TermQuery
            res.term_query = create_term_query(query)
          when sq::TermRangeQuery
            res.term_range_query = create_term_range_query(query)
          when sq::WildcardQuery
            res.wildcard_query = create_wildcard_query(query)
          else
            raise Couchbase::Error::CouchbaseError, "Unexpected search query type #{query.class}"
          end
          res
        end

        def create_boolean_field_query(query)
          query_attrs = query.to_h
          query_attrs[:value] = query_attrs.delete(:bool)

          Generated::Search::V1::BooleanFieldQuery.new(query_attrs)
        end

        def create_boolean_query(query)
          query_attrs = {
            boost: query.boost,
            must: create_conjunction_query(query.instance_variable_get(:@must)),
            must_not: create_disjunction_query(query.instance_variable_get(:@must_not)),
            should: create_disjunction_query(query.instance_variable_get(:@should)),
          }
          Generated::Search::V1::BooleanQuery.new(query_attrs)
        end

        def create_conjunction_query(query)
          query_attrs = {
            boost: query.boost,
            queries: query.instance_variable_get(:@queries).uniq.map(&:create_query),
          }
          Generated::Search::V1::ConjunctionQuery.new(query_attrs)
        end

        def create_date_range_query(query)
          query_attrs = query.to_h
          query_attrs[:start_date] = query_attrs.delete(:start)
          query_attrs[:end_date] = query_attrs.delete(:end)

          Generated::Search::V1::DateRangeQuery.new(query_attrs)
        end

        def create_disjunction_query(query)
          query_attrs = {
            boost: query.boost,
            queries: query.instance_variable_get(:@queries).uniq.map(&:create_query),
            minimum: query.min,
          }
          Generated::Search::V1::DisjunctionQuery.new(query_attrs)
        end

        def create_doc_id_query(query)
          Generated::Search::V1::DocIdQuery.new(query.to_h)
        end

        def create_geo_bounding_box_query(query)
          query_attrs = query.to_h
          query_attrs[:top_left] = Generated::Search::V1::LatLng.new(
            longitude: query_attrs[:top_left][0],
            latitude: query_attrs[:top_left][1],
          )
          query_attrs[:bottom_right] = Generated::Search::V1::LatLng.new(
            longitude: query_attrs[:bottom_right][0],
            latitude: query_attrs[:bottom_right][1],
          )
          Generated::Search::V1::GeoBoundingBoxQuery.new(query_attrs)
        end

        def create_geo_distance_query(query)
          query_attrs = query.to_h
          longitude, latitude = query_attrs.delete(:location)
          query_attrs[:center] = Generated::Search::V1::LatLng.new(
            longitude: longitude,
            latitude: latitude,
          )
          Generated::Search::V1::GeoDistanceQuery.new(query_attrs)
        end

        def create_geo_polygon_query(query)
          query_attrs = query.to_h
          polygon_points = query_attrs.delete(:polygon_points)
          query_attrs[:vertices] = polygon_points.map do |lon, lat|
            Generated::Search::V1::LatLng.new(
              longitude: lon,
              latitude: lat,
            )
          end
          Generated::Search::V1::GeoPolygonQuery.new(query_attrs)
        end

        def create_match_all_query(_query)
          Generated::Search::V1::MatchAllQuery.new
        end

        def create_match_none_query(_query)
          Generated::Search::V1::MatchNoneQuery.new
        end

        def create_match_phrase_query(query)
          query_attrs = query.to_h
          query_attrs[:phrase] = query_attrs.delete(:match_phrase)

          Generated::Search::V1::MatchPhraseQuery.new(query_attrs)
        end

        def create_match_query(query)
          query_attrs = query.to_h
          query_attrs[:value] = query_attrs.delete(:match)
          query_attrs[:operator] = MATCH_QUERY_OPERATOR_MAP[query_attrs[:operator]] if query_attrs.include?(:operator)

          Generated::Search::V1::MatchQuery.new(query_attrs)
        end

        def create_numeric_range_query(query)
          Generated::Search::V1::NumericRangeQuery.new(query.to_h)
        end

        def create_phrase_query(query)
          Generated::Search::V1::PhraseQuery.new(query.to_h)
        end

        def create_prefix_query(query)
          Generated::Search::V1::PrefixQuery.new(query.to_h)
        end

        def create_query_string_query(query)
          query_attrs = query.to_h
          query_attrs[:query_string] = query_attrs.delete(:query)

          Generated::Search::V1::QueryStringQuery.new(query_attrs)
        end

        def create_regexp_query(query)
          Generated::Search::V1::RegexpQuery.new(query.to_h)
        end

        def create_term_query(query)
          Generated::Search::V1::TermQuery.new(query.to_h)
        end

        def create_term_range_query(query)
          Generated::Search::V1::TermRangeQuery.new(query.to_h)
        end

        def create_wildcard_query(query)
          Generated::Search::V1::WildcardQuery.new(query.to_h)
        end

        def get_sort(options)
          options.sort.map do |s|
            case s
            when Couchbase::Cluster::SearchSort::SearchSortField
              # TODO: missing, type and mode could change to Enums
              Generated::Search::V1::Sorting.new(
                field_sorting: Generated::Search::V1::FieldSorting.new(
                  field: s.field,
                  descending: s.desc,
                  missing: s.missing.to_s,
                  mode: s.mode.to_s,
                  type: s.type.to_s,
                ),
              )
            when Couchbase::Cluster::SearchSort::SearchSortId
              Generated::Search::V1::Sorting.new(
                id_sorting: Generated::Search::V1::IdSorting.new(
                  descending: s.desc,
                ),
              )
            when Couchbase::Cluster::SearchSort::SearchSortScore
              Generated::Search::V1::Sorting.new(
                score_sorting: Generated::Search::V1::ScoreSorting.new(
                  descending: s.desc,
                ),
              )
            when Couchbase::Cluster::SearchSort::SearchSortGeoDistance
              Generated::Search::V1::Sorting.new(
                geo_distance_sorting: Generated::Search::V1::GeoDistanceSorting.new(
                  field: s.field,
                  descending: s.desc,
                  center: Generated::Search::V1::LatLng.new(
                    latitude: s.latitude,
                    longitude: s.longitude,
                  ),
                  unit: s.unit.to_s,
                ),
              )
            when String
              if s[0] == "-"
                desc = true
                field = s[1..]
              else
                desc = false
                field = s
              end
              if field == "_score"
                Generated::Search::V1::Sorting.new(
                  score_sorting: Generated::Search::V1::ScoreSorting.new(
                    descending: desc,
                  ),
                )
              else
                Generated::Search::V1::Sorting.new(
                  # TODO: What to do with the rest of the FieldSorting attributes?
                  core_sorting: Generated::Search::V1::FieldSorting.new(
                    field: field,
                    descending: desc,
                  ),
                )
              end
            else
              raise Couchbase::Error::CouchbaseError, "Unrecognised search sort type"
            end
          end
        end

        def get_facets(options)
          return {} if options.facets.nil?

          options.facets.transform_values do |facet|
            case facet
            when Couchbase::Cluster::SearchFacet::SearchFacetTerm
              Generated::Search::V1::Facet.new(
                term_facet: Generated::Search::V1::TermFacet.new(
                  field: facet.field,
                  size: facet.size,
                ),
              )
            when Couchbase::Cluster::SearchFacet::SearchFacetNumericRange
              Generated::Search::V1::Facet.new(
                numeric_range_facet: Generated::Search::V1::NumericRangeFacet.new(
                  field: facet.field,
                  size: facet.size,
                  numeric_ranges: facet.instance_variable_get(:@ranges).map do |r|
                    Generated::Search::V1::NumericRange(name: r[:name], min: r[:min], max: r[:max])
                  end,
                ),
              )
            when Couchbase::Cluster::SearchFacet::SearchFacetDateRange
              Generated::Search::V1::Facet.new(
                date_range_facet: Generated::Search::V1::DateRangeFacet.new(
                  field: facet.field,
                  size: facet.size,
                  date_ranges: facet.instance_variable_get(:@ranges).map do |r|
                    Generated::Search::V1::DateRange(name: r[:name], start: r[:start], end: r[:end])
                  end,
                ),
              )
            else
              raise Couchbase::Error::CouchbaseError, "Unrecognised search facet type"
            end
          end
        end
      end
    end
  end
end
