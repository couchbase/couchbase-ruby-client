#  Copyright 2023. Couchbase, Inc.
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

# frozen_string_literal: true

require "couchbase/errors"
require "couchbase/options"

require_relative "request_generator/query"
require_relative "response_converter/query"
require_relative "collection"

module Couchbase
  module Protostellar
    class Scope
      attr_reader :bucket_name
      attr_reader :name

      def initialize(client, bucket_name, name)
        @client = client
        @bucket_name = bucket_name
        @name = name

        @query_request_generator = RequestGenerator::Query.new(bucket_name: @bucket_name, scope_name: @name)
      end

      def collection(name)
        Collection.new(@client, @bucket_name, @name, name)
      end

      def query(statement, options = Couchbase::Options::Query::DEFAULT)
        req = @query_request_generator.query_request(statement, options)
        resps = @client.send_request(req)
        ResponseConverter::Query.to_query_result(resps)
      end
    end
  end
end
