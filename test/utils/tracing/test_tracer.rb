# frozen_string_literal: true

#  Copyright 2025-Present Couchbase, Inc.
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

require "couchbase/tracing/request_tracer"

require_relative "test_span"

module Couchbase
  module TestUtilities
    class TestTracer < Couchbase::Tracing::RequestTracer
      def initialize
        super
        @spans = []
      end

      def reset
        @spans.clear
      end

      def request_span(name, parent: nil, start_timestamp: nil)
        span = TestSpan.new(name, parent: parent, start_timestamp: start_timestamp)
        @spans << span

        return span if parent.nil?

        raise "Parent span has unexpected type: #{parent.inspect}" unless parent.instance_of?(TestSpan)

        parent.children << span

        span
      end

      # Fetches spans. Optionally filters them by name and/or parent span.
      #
      # @param [String, nil] name name of the span to filter by. If nil, no filtering by name is done.
      # @param [TestSpan, :root, nil] parent parent span to filter by. If :root, only root spans are returned.
      #   If nil, no filtering by parent is done.
      #
      # @return [Array<TestSpan>] array of spans matching the criteria
      def spans(name = nil, parent: nil)
        return @spans if name.nil? && parent.nil?

        @spans.filter do |span|
          success = true
          unless parent.nil?
            success &&= if parent == :root
                          span.parent.nil?
                        else
                          span.parent == parent
                        end
          end
          success &&= span.name == name unless name.nil?
          success
        end
      end
    end
  end
end
