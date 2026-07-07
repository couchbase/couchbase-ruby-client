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

require_relative 'results'
require_relative 'options_builder'

module FIT
  module Performer
    module Commands
      module Search
        class SearchQueryCommand
          attr_reader :stream_config

          def initialize(
            index_name:, search_query:, return_result:, initiated:, stream_config:, get_span_fn:,
            cluster: nil, scope: nil, raw_options: nil, fields_as: nil
          )
            @cmd_args = [index_name, search_query]
            @receiver = cluster || scope
            @raw_options = raw_options
            @return_result = return_result
            @initiated = initiated
            @stream_config = stream_config
            @fields_as = fields_as
            @get_span_fn = get_span_fn
          end

          def stream_type
            :STREAM_FULL_TEXT_SEARCH
          end

          def set_options
            return if @raw_options.nil?

            builder = OptionsBuilder.new(options: Couchbase::Options::Search.new, raw_options: @raw_options)
            builder.set_timeout
                   .set_limit
                   .set_skip
                   .set_explain
                   .set_highlight
                   .set_fields
                   .set_scan_consistency
                   .set_consistent_with
                   .set_sort
                   .set_facets
                   .set_raw
                   .set_include_locations
                   .set_parent_span(@get_span_fn)
            @cmd_args.append(builder.options)
          end

          def execute_command
            Results.as_search_result_stream(initiated: @initiated, stream_id: @stream_config.stream_id, fields_as: @fields_as) do
              @receiver.search_query(*@cmd_args)
            end
          end

          def self.create_command(...)
            cmd = SearchQueryCommand.new(...)
            cmd.set_options
            cmd
          end
        end
      end
    end
  end
end
