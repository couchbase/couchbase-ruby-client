# frozen_string_literal: true

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

require "couchbase/options"

require_relative "response_converter/kv"

module Couchbase
  module Protostellar
    class BinaryCollection
      def initialize(collection)
        @collection = collection
        @client = collection.instance_variable_get(:@client)
        @kv_request_generator = collection.instance_variable_get(:@kv_request_generator)
      end

      def append(id, content, options = Options::Append::DEFAULT)
        req = @kv_request_generator.append_request(id, content, options)
        resp = @client.send_request(req)
        ResponseConverter::KV.to_mutation_result(resp)
      end

      def prepend(id, content, options = Options::Prepend::DEFAULT)
        req = @kv_request_generator.prepend_request(id, content, options)
        resp = @client.send_request(req)
        ResponseConverter::KV.to_mutation_result(resp)
      end

      def increment(id, options = Options::Increment::DEFAULT)
        req = @kv_request_generator.increment_request(id, options)
        resp = @client.send_request(req)
        ResponseConverter::KV.to_counter_result(resp)
      end

      def decrement(id, options = Options::Decrement::DEFAULT)
        req = @kv_request_generator.decrement_request(id, options)
        resp = @client.send_request(req)
        ResponseConverter::KV.to_counter_result(resp)
      end
    end
  end
end
