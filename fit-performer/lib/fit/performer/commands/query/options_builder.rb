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
      module Query
        class OptionsBuilder < OptionsBuilderBase
          def set_scan_consistency
            @options.scan_consistency = @raw_options.scan_consistency.downcase if @raw_options.has_scan_consistency?
            self
          end

          def set_raw
            @raw_options.raw.each do |k, v|
              @options.raw_parameters[k] = v
            end
            self
          end

          def set_adhoc
            @options.adhoc = @raw_options.adhoc if @raw_options.has_adhoc?
            self
          end

          def set_profile
            @options.profile = @raw_options.profile.to_sym if @raw_options.has_profile?
            self
          end

          def set_readonly
            @options.readonly = @raw_options.readonly if @raw_options.has_readonly?
            self
          end

          def set_positional_parameters
            @options.positional_parameters(@raw_options.parameters_positional.to_a) unless @raw_options.parameters_positional.empty?
            self
          end

          def set_named_parameters
            # rubocop:disable Style/ZeroLengthPredicate
            # Protobuf maps have no #empty?
            @options.named_parameters(@raw_options.parameters_named.to_h) unless @raw_options.parameters_named.size.zero?
            # rubocop:enable Style/ZeroLengthPredicate
            self
          end

          def set_flex_index
            @options.flex_index = @raw_options.flex_index if @raw_options.has_flex_index?
            self
          end

          def set_pipeline_cap
            @options.pipeline_cap = @raw_options.pipeline_cap if @raw_options.has_pipeline_cap?
            self
          end

          def set_pipeline_batch
            @options.pipeline_batch = @raw_options.pipeline_batch if @raw_options.has_pipeline_batch?
            self
          end

          def set_scan_cap
            @options.scan_cap = @raw_options.scan_cap if @raw_options.has_scan_cap?
            self
          end

          def set_scan_wait
            @options.scan_wait = @raw_options.scan_wait_millis if @raw_options.has_scan_wait_millis?
            self
          end

          def set_timeout
            @options.timeout = @raw_options.timeout_millis if @raw_options.has_timeout_millis?
            self
          end

          def set_max_parallelism
            @options.max_parallelism = @raw_options.max_parallelism if @raw_options.has_max_parallelism?
            self
          end

          def set_metrics
            @options.metrics = @raw_options.metrics if @raw_options.has_metrics?
            self
          end

          def set_use_replica
            @options.use_replica = @raw_options.use_replica if @raw_options.has_use_replica?
            self
          end

          def set_client_context_id
            @options.client_context_id = @raw_options.client_context_id if @raw_options.has_client_context_id?
            self
          end
        end
      end
    end
  end
end
