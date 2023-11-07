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

require "couchbase/search_options"

module Couchbase
  module Protostellar
    module ResponseConverter
      class Search
        def self.to_search_result(resps, options)
          Couchbase::Cluster::SearchResult.new do |res|
            res.rows = []
            res.facets = {}
            resps.each do |resp|
              resp.hits.each do |hit|
                res.rows.append(convert_search_row(hit, options))
              end
              resp.facets.each do |k, facet_res|
                res.facets[k] = convert_facet_result(facet_res)
              end
              res.meta_data = convert_meta_data(resp.meta_data) if resp.has_meta_data?
            end
          end
        end

        def self.convert_search_row(proto_row, options)
          Couchbase::Cluster::SearchRow.new do |r|
            r.instance_variable_set(:@fields, (proto_row.fields.to_h.transform_values { |v| JSON.parse(v) }).to_json)
            r.transcoder = options.transcoder
            r.index = proto_row.index
            r.id = proto_row.id
            r.score = proto_row.score.to_f
            unless proto_row.locations.empty?
              r.locations = Couchbase::Cluster::SearchRowLocations.new(
                proto_row.locations.map { |loc| convert_search_row_location(loc) }
              )
            end
            r.fragments = convert_fragments(proto_row.fragments)
          end
        end

        def self.convert_fragments(proto_fragments)
          proto_fragments.to_h.transform_values { |f| f.content.to_a }
        end

        def self.convert_search_row_location(proto_location)
          Couchbase::Cluster::SearchRowLocation.new do |loc|
            loc.field = proto_location.field
            loc.term = proto_location.term
            loc.position = proto_location.position
            loc.start_offset = proto_location.start
            loc.end_offset = proto_location.end
            loc.array_positions = proto_location.array_positions.to_a
          end
        end

        def self.convert_facet_result(proto_facet_res)
          facet_type = proto_facet_res.search_facet
          case facet_type
          when :term_facet
            res = proto_facet_res.term_facet
            Couchbase::Cluster::SearchFacetResult::TermFacetResult.new do |f|
              f.name = res.name
              f.field = res.field
              f.total = res.total
              f.missing = res.missing
              f.other = res.other
              f.terms = res.terms.map do |term_res|
                Couchbase::Cluster::SearchFacetResult::TermFacetResult::TermFacet.new(term_res.name, term_res.size)
              end
            end
          when :date_range_facet
            res = proto_facet_res.date_range_facet
            Couchbase::Cluster::SearchFacetResult::DateRangeFacetResult.new do |f|
              f.name = res.name
              f.field = res.field
              f.total = res.total
              f.missing = res.missing
              f.other = res.other
              f.date_ranges = res.date_ranges.map do |date_range|
                start_time = Time.at(date_range.start.seconds).strftime("%Y-%m-%d")
                end_time = Time.at(date_range.end.seconds).strftime("%Y-%m-%d")
                Couchbase::Cluster::SearchFacetResult::DateRangeFacetResult::DateRangeFacet.new(
                  date_range.name, date_range.size, start_time, end_time
                )
              end
            end
          when :numeric_range_facet
            res = proto_facet_res.numeric_range_facet
            Couchbase::Cluster::SearchFacetResult::NumericRangeFacetResult.new do |f|
              f.name = res.name
              f.field = res.field
              f.total = res.total
              f.missing = res.missing
              f.other = res.other
              f.numeric_ranges = res.numeric_ranges.map do |numeric_range|
                Couchbase::Cluster::NumericRangeResult::NumericRangeFacetResult::NumericRangeFacet.new(
                  numeric_range.name, numeric_range.size, numeric_range.min, numeric_range.max
                )
              end
            end
          else
            raise ProtostellarError, "Unrecognised facet type"
          end
        end

        def self.convert_meta_data(proto_meta_data)
          Couchbase::Cluster::SearchMetaData.new do |meta|
            proto_metrics = proto_meta_data.metrics
            dur = proto_metrics.execution_time
            meta.metrics.took = (dur.seconds * 1000) + (dur.nanos / 1000.0).round  # `took` is in milliseconds
            meta.metrics.total_rows = proto_metrics.total_rows
            meta.metrics.max_score = proto_metrics.max_score
            meta.metrics.success_partition_count = proto_metrics.success_partition_count
            meta.metrics.error_partition_count = proto_metrics.error_partition_count

            meta.errors = proto_meta_data.errors.to_h
          end
        end
      end
    end
  end
end
