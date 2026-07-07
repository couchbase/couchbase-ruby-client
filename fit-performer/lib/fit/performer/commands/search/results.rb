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

module FIT
  module Performer
    module Commands
      module Search
        class Results < SharedResults
          def self.as_search_result_stream(initiated:, stream_id:, fields_as: nil)
            begin
              result = yield
            rescue StandardError => e
              return to_exception(error: e, initiated: initiated)
            end

            to_search_result_enumerator(result: result, initiated: initiated, stream_id: stream_id, fields_as: fields_as)
          end

          def self.to_search_result_enumerator(result:, initiated:, stream_id:, fields_as: nil)
            kwargs = {initiated: initiated, stream_id: stream_id}
            Enumerator.new do |y|
              result.rows.each do |row|
                y << to_search_result(to_search_row(row, fields_as: fields_as), **kwargs)
              end
              y << to_search_result(to_search_facets(result.facets), **kwargs) unless result.facets.nil?
              y << to_search_result(to_search_meta_data(result.meta_data), **kwargs) unless result.meta_data.nil?
            end
          end

          def self.to_search_result(item, initiated:, stream_id:)
            res = FIT::Protocol::SDK::Search::StreamingSearchResult.new(stream_id: stream_id)
            case item
            when FIT::Protocol::SDK::Search::SearchRow
              res.row = item
            when FIT::Protocol::SDK::Search::SearchFacets
              res.facets = item
            when FIT::Protocol::SDK::Search::SearchMetaData
              res.meta_data = item
            else
              raise PerformerError, "#{item.class} is not a valid search stream item"
            end
            create_run_result(
              sdk_result: FIT::Protocol::SDK::Result.new(search_streaming_result: res),
              initiated: initiated,
            )
          end

          def self.to_search_row(row, fields_as: nil)
            proto_row = FIT::Protocol::SDK::Search::SearchRow.new(
              index: row.index,
              id: row.id,
              score: row.score,
              fields: get_content(content: row.fields, content_as: fields_as),
            )
            row.locations&.get_all&.each do |loc|
              proto_row.locations << to_search_row_location(loc)
            end
            proto_row
          end

          def self.to_search_row_location(loc)
            FIT::Protocol::SDK::Search::SearchRowLocation.new(
              field: loc.field,
              term: loc.term,
              position: loc.position,
              start: loc.start_offset,
              end: loc.end_offset,
              array_positions: loc.array_positions,
            )
          end

          def self.to_search_facet_result(facet_res)
            FIT::Protocol::SDK::Search::SearchFacetResult.new(
              name: facet_res.name,
              field: facet_res.field,
              total: facet_res.total,
              missing: facet_res.missing,
              other: facet_res.other,
            )
          end

          def self.to_search_facets(facets)
            proto_facets = FIT::Protocol::SDK::Search::SearchFacets.new
            facets.each do |name, facet|
              proto_facets.facets[name] = to_search_facet_result(facet)
            end
            proto_facets
          end

          def self.to_search_meta_data(meta)
            proto_meta = FIT::Protocol::SDK::Search::SearchMetaData.new(metrics: to_search_metrics(meta.metrics))
            meta.errors&.each do |k, v|
              proto_meta.errors[k] = v
            end
            proto_meta
          end

          def self.to_search_metrics(metrics)
            FIT::Protocol::SDK::Search::SearchMetrics.new(
              took_msec: metrics.took,
              total_rows: metrics.total_rows,
              max_score: metrics.max_score,
              total_partition_count: metrics.total_partition_count,
              success_partition_count: metrics.success_partition_count,
              error_partition_count: metrics.error_partition_count,
            )
          end
        end
      end
    end
  end
end
