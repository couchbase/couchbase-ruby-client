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

require 'json'

module Couchbase
  class Bucket
    class ViewOptions
      # @return [Integer] Timeout in milliseconds
      attr_accessor :timeout

      # Specifies the level of consistency for the query
      # @return [:not_bounded, :request_plus, :update_after]
      attr_accessor :scan_consistency

      # Specifies the number of results to skip from the start of the result set
      # @return [Integer]
      attr_accessor :skip

      # Specifies the maximum number of results to return
      # @return [Integer]
      attr_accessor :limit

      # Specifies the key, to which the engine has to skip before result generation
      attr_accessor :start_key

      # Specifies the key, at which the result generation has to be stopped
      # @return [#to_json]
      attr_accessor :end_key

      # Specifies the document id in case {#start_key} gives multiple results within the index
      # @return [String]
      attr_accessor :start_key_doc_id

      # Specifies the document id in case {#end_key} gives multiple results within the index
      # @return [String]
      attr_accessor :end_key_doc_id

      # Specifies whether the {#end_key}/#{#end_key_doc_id} values should be inclusive
      # @return [Boolean]
      attr_accessor :inclusive_end

      # Specifies whether to enable grouping of the results
      #
      # @return [Boolean]
      attr_accessor :group

      # Specifies the depth within the key to group the results
      #
      # @return [Integer]
      attr_accessor :group_level

      # Specifies the set of the keys to fetch from the index
      # @return [#to_json]
      attr_accessor :key

      # Specifies set of the keys to fetch from the index
      #
      # @return [Array<#to_json>]
      attr_accessor :keys

      # Specifies the order of the results that should be returned
      #
      # @return [:ascending, :descending]
      attr_accessor :order

      # Specifies whether to enable the reduction function associated with this particular view index
      # @return [Boolean]
      attr_accessor :reduce

      # Specifies the behaviour of the view engine should an error occur during the gathering of view index results
      # which would result in only partial results being available
      #
      # @return [:stop, :continue]
      attr_accessor :on_error

      # @return [Boolean] allows to return debug information as part of the view response
      attr_accessor :debug

      # @return [:production, :development]
      attr_accessor :namespace

      # @yieldparam [ViewOptions] self
      def initialize
        @namespace = :production
        yield self if block_given?
      end

      # Allows providing custom JSON key/value pairs for advanced usage
      #
      # @param [String] key the parameter name (key of the JSON property)
      # @param [Object] value the parameter value (value of the JSON property)
      def raw(key, value)
        @raw_parameters[key] = JSON.generate(value)
      end
    end

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
