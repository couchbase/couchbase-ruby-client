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

require "couchbase/tracing/request_span"

module Couchbase
  module TestUtilities
    class TestSpan < Couchbase::Tracing::RequestSpan
      attr_accessor :name
      attr_accessor :start_time, :end_time
      attr_accessor :attributes
      attr_accessor :parent
      attr_accessor :children
      attr_accessor :status_code

      def initialize(name, parent: nil, start_timestamp: nil)
        super()
        @name = name
        @start_time = start_timestamp.nil? ? Time.now : start_timestamp
        @parent = parent
        @attributes = {}
        @children = []
      end

      def set_attribute(key, value)
        @attributes[key] = value
      end

      def status=(status_code)
        @status_code = status_code
      end

      def finish(end_timestamp: nil)
        @end_time = end_timestamp.nil? ? Time.now : end_timestamp
      end

      def to_h
        {
          name: @name,
          start_time: @start_time&.strftime('%H:%M:%S.%L'),
          end_time: @end_time&.strftime('%H:%M:%S.%L'),
          attributes: @attributes,
          children: @children.map(&:to_h),
        }
      end
    end
  end
end
