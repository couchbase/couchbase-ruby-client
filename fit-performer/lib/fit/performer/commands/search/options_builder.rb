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

require 'fit/performer/commands/options_builder_base'

require 'google/protobuf/well_known_types'

module FIT
  module Performer
    module Commands
      module Search
        class OptionsBuilder < OptionsBuilderBase
          HIGHLIGHT_STYLE_MAP = {
            :HIGHLIGHT_STYLE_HTML => :html,
            :HIGHLIGHT_STYLE_ANSI => :ansi,
          }.freeze

          GEO_DISTANCE_UNIT_MAP = {
            :SEARCH_GEO_DISTANCE_UNITS_METERS => :meters,
            :SEARCH_GEO_DISTANCE_UNITS_MILES => :miles,
            :SEARCH_GEO_DISTANCE_UNITS_CENTIMETERS => :centimeters,
            :SEARCH_GEO_DISTANCE_UNITS_MILLIMETERS => :millimeters,
            :SEARCH_GEO_DISTANCE_UNITS_NAUTICAL_MILES => :nauticalmiles,
            :SEARCH_GEO_DISTANCE_UNITS_KILOMETERS => :kilometers,
            :SEARCH_GEO_DISTANCE_UNITS_FEET => :feet,
            :SEARCH_GEO_DISTANCE_UNITS_YARDS => :yards,
            :SEARCH_GEO_DISTANCE_UNITS_INCHES => :inch,
          }.freeze

          SCAN_CONSISTENCY_MAP = {
            :SEARCH_SCAN_CONSISTENCY_NOT_BOUNDED => :not_bounded,
          }.freeze

          def set_limit
            @options.limit = @raw_options.limit if @raw_options.has_limit?
            self
          end

          def set_skip
            @options.skip = @raw_options.skip if @raw_options.has_skip?
            self
          end

          def set_explain
            @options.explain = @raw_options.explain if @raw_options.has_explain?
            self
          end

          def set_highlight
            if @raw_options.has_highlight?
              @options.highlight_style = HIGHLIGHT_STYLE_MAP[@raw_options.highlight.style] if @raw_options.highlight.has_style?
              @options.highlight_fields = @raw_options.highlight.fields.to_a unless @raw_options.highlight.fields.empty?
            end
            self
          end

          def set_fields
            @options.fields = @raw_options.fields.to_a unless @raw_options.fields.empty?
            self
          end

          def set_scan_consistency
            @options.scan_consistency = SCAN_CONSISTENCY_MAP[@raw_options.scan_consistency] if @raw_options.has_scan_consistency?
            self
          end

          def set_sort
            return self if @raw_options.sort.empty?

            sort = @raw_options.sort.map do |proto_sort|
              case proto_sort.sort
              when :score
                Couchbase::Cluster::SearchSort.score do |s|
                  s.desc = proto_sort.score.desc if proto_sort.score.has_desc?
                end
              when :id
                Couchbase::Cluster::SearchSort.id do |s|
                  s.desc = proto_sort.id.desc if proto_sort.id.has_desc?
                end
              when :field
                Couchbase::Cluster::SearchSort.field(proto_sort.field.field) do |s|
                  s.desc = proto_sort.field.desc if proto_sort.field.has_desc?
                  s.type = proto_sort.field.type.to_sym if proto_sort.field.has_type?
                  s.mode = proto_sort.field.mode.to_sym if proto_sort.field.has_mode?
                  s.missing = proto_sort.field.missing.to_sym if proto_sort.field.has_missing?
                end
              when :geo_distance
                Couchbase::Cluster::SearchSort.geo_distance(
                  proto_sort.geo_distance.field,
                  proto_sort.geo_distance.location.lon,
                  proto_sort.geo_distance.location.lat,
                ) do |s|
                  s.unit = GEO_DISTANCE_UNIT_MAP[proto_sort.geo_distance.unit] if proto_sort.geo_distance.has_unit?
                  s.desc = proto_sort.geo_distance.sort if proto_sort.geo_distance.has_sort?
                end
              when :raw
                proto_sort.raw
              else
                raise PerformerError, "Search sort type #{proto_sort.sort} not supported"
              end
            end

            @options.sort = sort
            self
          end

          def set_facets
            return self unless @raw_options.facets.size.positive?

            facets = {}
            @raw_options.facets.each do |name, proto_facet|
              facets[name] =
                case proto_facet.facet
                when :term
                  Couchbase::Cluster::SearchFacet.term(proto_facet.term.field) do |f|
                    f.size = proto_facet.term.size if proto_facet.term.has_size?
                  end
                when :numeric_range
                  Couchbase::Cluster::SearchFacet.numeric_range(proto_facet.numeric_range.name) do |f|
                    f.size = proto_facet.numeric_range.size if proto_facet.numeric_range.has_size?
                    proto_facet.numeric_range.numeric_ranges.each do |proto_range|
                      min = proto_range.has_min? ? proto_range.min : nil
                      max = proto_range.has_max? ? proto_range.max : nil
                      f.add(proto_range.name, min, max)
                    end
                  end
                when :date_range
                  Couchbase::Cluster::SearchFacet.date_range(proto_facet.date_range) do |f|
                    f.size = proto_facet.date_range.size if proto_facet.date_range.has_size?
                    proto_facet.date_range.date_ranges.each do |proto_range|
                      start_time = proto_range.has_start? ? proto_range.start.to_time : nil
                      end_time = proto_range.has_end? ? proto_range.end.to_time : nil
                      f.add(proto_range.name, start_time, end_time)
                    end
                  end
                else
                  raise PerformerError, "Search facet type #{proto_facet.facet} not supported"
                end
            end
            @options.facets = facets
            self
          end

          def set_raw
            # TODO: Complete this if/when support for raw is added to the SDK
            raise PerformerError, "`raw` search option not supported in the Ruby SDK" if @raw_options.raw.size.positive?

            self
          end

          def set_include_locations
            @options.include_locations = @raw_options.include_locations if @raw_options.has_include_locations?
            self
          end

          def set_vector_query_combination
            @options.vector_query_combination = @raw_options.vector_query_combination.downcase if @raw_options.has_vector_query_combination?
            self
          end

          def set_serializer
            return self unless @raw_options.has_serialize?

            @options.transcoder = CustomJsonTranscoder.new if @raw_options.serialize.serializer == :custom_serializer

            self
          end
        end
      end
    end
  end
end
