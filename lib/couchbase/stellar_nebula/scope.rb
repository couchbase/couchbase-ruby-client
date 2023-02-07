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

require "couchbase/errors"
require "couchbase/options"

require_relative "request_generator/query"
require_relative "response_converter/query"
require_relative "collection"

module Couchbase
  module StellarNebula
    class Scope
      attr_reader :name

      def initialize(client, bucket_name, name)
        @client = client
        @bucket_name = bucket_name
        @name = name

        @query_request_generator = RequestGenerator::Query.new(bucket_name: @bucket_name)
      end

      def collection(name)
        Collection.new(@client, @bucket_name, @name, name)
      end

      def query(statement, options = Couchbase::Options::Query::DEFAULT)
        req = @query_request_generator.query_request(statement, options)
        begin
          resps = @client.query(req, timeout: options.timeout)
        rescue GRPC::DeadlineExceeded
          raise Couchbase::Error::Timeout
        end
        ResponseConverter::Query.from_query_responses(resps)
      end
    end
  end
end
