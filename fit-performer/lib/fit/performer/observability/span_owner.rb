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

require "fit/performer/performer_error"

module FIT
  module Performer
    module Observability
      class SpanOwner
        def initialize
          @spans = {}
          @mutex = Mutex.new
        end

        def get_span(span_id)
          @mutex.synchronize do
            raise PerformerError, "Span with ID #{span_id} not found" unless @spans.key?(span_id)

            @spans[span_id]
          end
        end

        def create_span(tracer, request)
          parent_span =
            (get_span(request.parent_span_id) if request.has_parent_span_id?)

          span = tracer.request_span(request.name, parent: parent_span)
          request.attributes.each do |key, value|
            span.set_attribute(key, value.public_send(value.value))
          end

          @mutex.synchronize do
            @spans[request.id] = span
          end
        end

        def finish_span(request)
          @mutex.synchronize do
            raise PerformerError, "Span with ID #{request.id} not found" unless @spans.key?(request.id)

            @spans.delete(request.id)
          end&.finish
        end
      end
    end
  end
end
