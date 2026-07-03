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

module FIT
  module Performer
    module Commands
      module QueryIndexManager
        class OptionsBuilder < OptionsBuilderBase
          def set_scope_name
            @options.scope_name = @raw_options.scope_name if @raw_options.has_scope_name?
            self
          end

          def set_collection_name
            @options.collection_name = @raw_options.collection_name if @raw_options.has_collection_name?
            self
          end

          def set_ignore_if_exists
            @options.ignore_if_exists = @raw_options.ignore_if_exists if @raw_options.has_ignore_if_exists?
            self
          end

          def set_ignore_if_does_not_exist
            @options.ignore_if_does_not_exist = @raw_options.ignore_if_not_exists if @raw_options.has_ignore_if_not_exists?
            self
          end

          def set_num_replicas
            @options.num_replicas = @raw_options.num_replicas if @raw_options.has_num_replicas?
            self
          end

          def set_deferred
            @options.deferred = @raw_options.deferred if @raw_options.has_deferred?
            self
          end

          def set_index_name
            @options.index_name = @raw_options.index_name if @raw_options.has_index_name?
            self
          end

          def set_watch_primary
            @options.watch_primary = @raw_options.watch_primary if @raw_options.has_watch_primary?
            self
          end
        end
      end
    end
  end
end
