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
      class OptionsBuilderBase
        attr_reader :options

        def initialize(options:, raw_options:)
          @options = options
          @raw_options = raw_options
        end

        def set_timeout
          if @raw_options.respond_to?(:has_timeout_millis?)
            @options.timeout = @raw_options.timeout_millis if @raw_options.has_timeout_millis?
          elsif @raw_options.has_timeout_msecs?
            @options.timeout = @raw_options.timeout_msecs
          end
          self
        end

        def set_parent_span(get_span_fn)
          return self unless @raw_options.has_parent_span_id?

          @options.parent_span = get_span_fn.call(@raw_options.parent_span_id)
          self
        end

        def set_consistent_with
          return self unless @raw_options.has_consistent_with?

          mutation_state = Couchbase::MutationState.new
          @raw_options.consistent_with.tokens.each do |proto_token|
            mutation_state.add(
              Couchbase::MutationToken.new do |t|
                t.partition_id = proto_token.partition_id
                t.partition_uuid = proto_token.partition_uuid
                t.sequence_number = proto_token.sequence_number
                t.bucket_name = proto_token.bucket_name
              end,
            )
          end
          @options.consistent_with(mutation_state)
          self
        end

        def set_preserve_expiry
          @options.preserve_expiry = @raw_options.preserve_expiry if @raw_options.has_preserve_expiry?
          self
        end

        class CustomJsonTranscoder
          def initialize
            @json_transcoder = Couchbase::JsonTranscoder.new
          end

          def encode(document)
            document["Serialized"] = true
            @json_transcoder.encode(document)
          end

          def decode(blob, flags)
            document = @json_transcoder.decode(blob, flags)
            document["Serialized"] = false
            document
          end
        end
      end
    end
  end
end
