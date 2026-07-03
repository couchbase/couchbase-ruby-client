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

require 'couchbase/json_transcoder'
require 'couchbase/raw_binary_transcoder'
require 'couchbase/raw_json_transcoder'
require 'couchbase/raw_string_transcoder'

require 'fit/performer/performer_error'
require 'fit/performer/commands/options_builder_base'

module FIT
  module Performer
    module Commands
      module KeyValue
        class OptionsBuilder < OptionsBuilderBase
          def set_with_expiry
            @options.with_expiry = @raw_options.with_expiry if @raw_options.has_with_expiry?
            self
          end

          def set_projection
            @options.project(*@raw_options.projection) unless @raw_options.projection.empty?
            self
          end

          def set_transcoder
            return self unless @raw_options.has_transcoder?

            transcoder_type = @raw_options.transcoder.transcoder
            @options.transcoder =
              case transcoder_type
              when :json
                Couchbase::JsonTranscoder.new
              when :raw_string
                begin
                  Couchbase::RawStringTranscoder.new  # Added in 3.4.3
                rescue NameError
                  nil
                end
              when :raw_binary
                begin
                  Couchbase::RawBinaryTranscoder.new  # Added in 3.4.3
                rescue NameError
                  nil
                end
              when :raw_json
                begin
                  Couchbase::RawJsonTranscoder.new # Added in 3.4.3
                rescue NameError
                  nil
                end
              else
                raise PerformerError, "Transcoder type `#{transcoder_type}` not supported"
              end
            self
          end

          def set_durability
            return self unless @raw_options.has_durability?

            durability_type = @raw_options.durability.durability
            case durability_type
            when :durabilityLevel
              @options.durability_level = @raw_options.durability.durabilityLevel.downcase
            when :observe
              obs = @raw_options.durability.observe
              @options.persist_to = obs.persistTo.to_s.delete_prefix('PERSIST_TO_').to_sym.downcase
              @options.replicate_to = obs.replicateTo.to_s.delete_prefix('REPLICATE_TO_').to_sym.downcase
            else
              raise PerformerError, "Durability type `#{durability_type}` not supported"
            end

            self
          end

          def set_cas
            @options.cas = @raw_options.cas if @raw_options.has_cas?
            self
          end

          def set_expiry
            return self unless @raw_options.has_expiry?

            @options.expiry = KeyValue.get_expiry(@raw_options)
            self
          end

          def set_batch_byte_limit
            @options.batch_byte_limit = @raw_options.batch_byte_limit if @raw_options.has_batch_byte_limit?
            self
          end

          def set_batch_item_limit
            @options.batch_item_limit = @raw_options.batch_item_limit if @raw_options.has_batch_item_limit?
            self
          end

          def set_concurrency
            @options.concurrency = @raw_options.concurrency if @raw_options.has_concurrency?
            self
          end

          def set_ids_only
            @options.ids_only = @raw_options.ids_only if @raw_options.has_ids_only?
            self
          end

          def set_access_deleted
            @options.access_deleted = @raw_options.access_deleted if @raw_options.has_access_deleted?
            self
          end

          def set_delta
            @options.delta = @raw_options.delta if @raw_options.has_delta?
            self
          end

          def set_initial
            @options.initial = @raw_options.initial if @raw_options.has_initial?
            self
          end

          def set_store_semantics
            @options.store_semantics = @raw_options.store_semantics.downcase if @raw_options.has_store_semantics?
            self
          end

          def set_create_as_deleted
            @options.create_as_deleted = @raw_options.create_as_deleted if @raw_options.has_create_as_deleted?
            self
          end

          def set_read_preference
            @options.read_preference = @raw_options.read_preference.downcase if @raw_options.has_read_preference?
            self
          end
        end
      end
    end
  end
end
