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

require "couchbase/metrics/meter"
require_relative "test_value_recorder"

module Couchbase
  module TestUtilities
    class TestMeter < Couchbase::Metrics::Meter
      def initialize
        super
        @value_recorders = {}
        @mutex = Mutex.new
      end

      def value_recorder(name, attributes)
        puts "Creating or retrieving ValueRecorder for name: #{name}, attributes: #{attributes}"
        @mutex.synchronize do
          @value_recorders[name] ||= {}
          @value_recorders[name][attributes] ||= TestValueRecorder.new(name, attributes)
        end
      end

      def values(name, attributes, attributes_filter_strategy = :subset)
        @mutex.synchronize do
          return [] unless @value_recorders.key?(name)

          matching_recorders = @value_recorders[name].select do |attrs, _|
            case attributes_filter_strategy
            when :subset
              attrs >= attributes
            when :exact
              attrs == attributes
            else
              raise ArgumentError, "Unknown attributes_filter_strategy: #{attributes_filter_strategy}"
            end
          end

          matching_recorders.values.flat_map(&:values)
        end
      end

      def reset
        @mutex.synchronize do
          @value_recorders.clear
        end
      end
    end
  end
end
