# frozen_string_literal: true

#  Copyright 2022-Present. Couchbase, Inc.
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
require_relative "query_meta_data"
require_relative "json_transcoder"

module Couchbase
  module StellarNebula
    class QueryResult
      attr_accessor :meta_data
      attr_accessor :transcoder

      def rows(transcoder = self.transcoder)
        @rows.lazy.map do |row|
          if transcoder == :json
            JSON.parse(row)
          else
            transcoder.call(row)
          end
        end
      end

      def initialize(resps)
        @rows = []
        resps.each do |resp|
          @rows.push(*resp.rows)
          @meta_data = QueryMetaData.new(resp.meta_data) unless resp.meta_data.nil?
        end

        yield self if block_given?
      end
    end
  end
end
