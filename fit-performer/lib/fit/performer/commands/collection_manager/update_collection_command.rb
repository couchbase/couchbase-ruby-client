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

require_relative 'options_builder'
require_relative 'results'

module FIT
  module Performer
    module Commands
      module CollectionManager
        class UpdateCollectionCommand
          def initialize(manager:, scope_name:, collection_name:, settings:, initiated:, get_span_fn:, return_result: true,
                         raw_options: nil)
            @manager = manager
            @cmd_args = [scope_name, collection_name, settings]
            @initiated = initiated
            @return_result = return_result
            @raw_options = raw_options
            @get_span_fn = get_span_fn
          end

          def set_options
            return if @raw_options.nil?

            builder = OptionsBuilder.new(
              options: Couchbase::Management::Options::Collection::UpdateCollection.new,
              raw_options: @raw_options,
            )
            builder.set_timeout
                   .set_parent_span(@get_span_fn)

            @cmd_args.append(builder.options)
          end

          def execute_command
            Results.as_success(initiated: @initiated) do
              @manager.update_collection(*@cmd_args)
            end
          end

          def self.create_command(...)
            cmd = UpdateCollectionCommand.new(...)
            cmd.set_options
            cmd
          end
        end
      end
    end
  end
end
