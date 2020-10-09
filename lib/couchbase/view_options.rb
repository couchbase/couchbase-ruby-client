#    Copyright 2020 Couchbase, Inc.
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

require "json"

module Couchbase
  class Bucket
    class ViewRow
      # @return [String]
      attr_accessor :id

      # @return [#to_json]
      attr_accessor :key

      # @return [#to_json]
      attr_accessor :value

      # @yieldparam [ViewRow] self
      def initialize
        yield self if block_given?
      end
    end

    class ViewMetaData
      # @return [Integer]
      attr_accessor :total_rows

      # @api private
      attr_writer :debug_info

      def debug
        JSON.parse(@debug_info)
      end

      # @yieldparam [ViewMetaData] self
      def initialize
        yield self if block_given?
      end
    end

    class ViewResult
      # @return [ViewMetaData] returns object representing additional metadata associated with this query
      attr_accessor :meta_data

      # @return [Array<ViewRow>]
      attr_accessor :rows

      # @yieldparam [ViewResult] self
      def initialize
        yield self if block_given?
      end
    end
  end
end
