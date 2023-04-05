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

        def initialize(default_timeout: nil)
          @default_timeout = default_timeout.nil? ? TimeoutDefaults::SEARCH : default_timeout
        end

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
          proto_opts[:limit] = options.limit unless options.limit.nil?
          proto_opts[:skip] = options.skip unless options.skip.nil?
          proto_opts[:collections] = options.collections unless options.collections.nil?

          proto_req = Generated::Search::V1::SearchQueryRequest.new(
            query: create_query(query),
            index_name: index_name,
            **proto_opts
          )

          create_search_request(proto_req, :search_query, options)
        end

        private

        def create_search_request(proto_request, rpc, options, idempotent: false)
          Request.new(
            service: :search,
            rpc: rpc,
            proto_request: proto_request,
            timeout: get_timeout(options),
            idempotent: idempotent
          )
        end

        def create_query(query)
          sq = Couchbase::Cluster::SearchQuery

          case query
          when sq::BooleanFieldQuery
            [create_boolean_field_query(query), :boolean_field_query]
          when sq::BooleanQuery
            [create_boolean_query(query), :boolean_query]
          when sq::ConjunctionQuery
            [create_conjunction_query(query), :conjunction_query]
          when sq::DateRangeQuery
            [create_date_range_query(query), :date_range_query]
          when sq::DisjunctionQuery
            [create_disjunction_query(query), :disjunction_query]
          when sq::DocIdQuery
            [create_doc_id_query(query), :doc_id_query]
          when sq::GeoBoundingBoxQuery
            [create_geo_bounding_box_query(query), :geo_bounding_box_query]
          when sq::GeoDistanceQuery
            [create_geo_distance_query(query), :geo_distance_query]
          when sq::GeoPolygonQuery
            [create_geo_polygon_query(query), :geo_polygon_query]
          when sq::MatchAllQuery
            [create_match_all_query(query), :match_all_query]
          when sq::MatchNoneQuery
            [create_match_none_query(query), :match_none_query]
          when sq::MatchPhraseQuery
            [create_match_phrase_query(query), :match_phrase_query]
          when sq::MatchQuery
            [create_match_query(query), :match_query]
          when sq::NumericRangeQuery
            [create_numeric_range_query(query), :numeric_range_query]
          when sq::PhraseQuery
            [create_phrase_query(query), :phrase_query]
          when sq::PrefixQuery
            [create_prefix_query(query), :prefix_query]
          when sq::QueryStringQuery
            [create_query_string_query(query), :query_string_query]
          when sq::RegexpQuery
            [create_regexp_query(query), :regexp_query]
          when sq::TermQuery
            [create_term_query(query), :term_query]
          when sq::TermRangeQuery
            [create_term_range_query(query), :term_range_query]
          when sq::WildcardQuery
            [create_wildcard_query(query), :wildcard_query]
          else
            raise Protostellar::Error::UnexpectedSearchQueryType
          end
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
            latitude: query_attrs[:top_left][1]
          )
          query_attrs[:bottom_right] = Generated::Search::V1::LatLng.new(
            longitude: query_attrs[:bottom_right][0],
            latitude: query_attrs[:bottom_right][1]
          )
          Generated::Search::V1::GeoBoundingBoxQuery.new(query_attrs)
        end

        def create_geo_distance_query(query)
          query_attrs = query.to_h
          longitude, latitude = query_attrs.delete(:location)
          query_attrs[:center] = Generated::Search::V1::LatLng.new(
            longitude: longitude,
            latitude: latitude
          )
          Generated::Search::V1::GeoDistanceQuery.new(query_attrs)
        end

        def create_geo_polygon_query(query)
          query_attrs = query.to_h
          polygon_points = query_attrs.delete(:polygon_points)
          query_attrs[:vertices] = polygon_points.map do |lon, lat|
            Generated::Search::V1::LatLng.new(
              longitude: lon,
              latitude: lat
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
                  type: s.type.to_s
                )
              )
            when Couchbase::Cluster::SearchSort::SearchSortId
              Generated::Search::V1::Sorting.new(
                id_sorting: Generated::Search::V1::IdSorting.new(
                  descending: s.desc
                )
              )
            when Couchbase::Cluster::SearchSort::SearchSortScore
              Generated::Search::V1::Sorting.new(
                score_sorting: Generated::Search::V1::ScoreSorting.new(
                  descending: s.desc
                )
              )
            when Couchbase::Cluster::SearchSort::SearchSortGeoDistance
              Generated::Search::V1::Sorting.new(
                geo_distance_sorting: Generated::Search::V1::GeoDistanceSorting.new(
                  field: s.field,
                  descending: s.desc,
                  center: Generated::Search::V1::LatLng.new(
                    latitude: s.latitude,
                    longitude: s.longitude
                  ),
                  unit: s.unit.to_s
                )
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
                    descending: desc
                  )
                )
              else
                Generated::Search::V1::Sorting.new(
                  # TODO: What to do with the rest of the FieldSorting attributes?
                  core_sorting: Generated::Search::V1::FieldSorting.new(
                    field: field,
                    descending: desc
                  )
                )
              end
            else
              raise Protostellar::Error::ProtostellarError, "Unrecognised search sort type"
            end
          end
        end

        def get_timeout(options)
          if options.timeout.nil?
            @default_timeout
          else
            options.timeout
          end
        end
      end
    end
  end
end
