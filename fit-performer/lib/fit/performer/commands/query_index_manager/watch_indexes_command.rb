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

require 'couchbase/management/query_index_manager'

require_relative 'results'
require_relative 'options_builder'

module FIT
  module Performer
    module Commands
      module QueryIndexManager
        class WatchIndexesCommand
          def initialize(manager:, index_names:, timeout:, initiated:, get_span_fn:, return_result: true, raw_options: nil,
                         bucket_name: nil)
            @manager = manager
            @cmd_args = bucket_name.nil? ? [] : [bucket_name]
            @cmd_args.append(index_names, timeout)
            @raw_options = raw_options
            @initiated = initiated
            @return_result = return_result
            @get_span_fn = get_span_fn
          end

          def set_options
            return if @raw_options.nil?

            builder = OptionsBuilder.new(
              options: Couchbase::Management::Options::Query::WatchIndexes.new,
              raw_options: @raw_options,
            )
            builder.set_watch_primary
                   .set_scope_name
                   .set_collection_name
                   .set_parent_span(@get_span_fn)

            @cmd_args.append(builder.options)
          end

          def execute_command
            Results.as_success(initiated: @initiated) do
              @manager.watch_indexes(*@cmd_args)
            end
          end

          def self.create_command(...)
            cmd = WatchIndexesCommand.new(...)
            cmd.set_options
            cmd
          end
        end
      end
    end
  end
end
