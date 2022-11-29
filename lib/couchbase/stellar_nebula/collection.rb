# frozen_string_literal: true

#  Copyright 2022-Present Couchbase, Inc.
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

require_relative "get_options"

require_relative "generated/kv.v1_pb"

module Couchbase
  module StellarNebula
    class Collection
      attr_reader :name

      def initialize(client, bucket_name, scope_name, name)
        @client = client
        @bucket_name = bucket_name
        @scope_name = scope_name
        @name = name
      end

      def get(id, _options = GetOptions::DEFAULT)
        resp = @client.get(
          Generated::KV::V1::GetRequest.new(
            bucket_name: @bucket_name,
            scope_name: @scope_name,
            collection_name: @name,
            key: id
          )
        )
        pp resp
        resp
      end
    end
  end
end
