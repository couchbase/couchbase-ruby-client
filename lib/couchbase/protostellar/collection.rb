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

require "couchbase/options"
require "couchbase/errors"
require "couchbase/collection_options"

require_relative "request_generator/kv"
require_relative "response_converter/kv"
require_relative "binary_collection"

require "google/protobuf/well_known_types"

module Couchbase
  module Protostellar
    class Collection
      attr_reader :bucket_name
      attr_reader :scope_name
      attr_reader :name

      def initialize(client, bucket_name, scope_name, name)
        @client = client
        @bucket_name = bucket_name
        @scope_name = scope_name
        @name = name
        @kv_request_generator = RequestGenerator::KV.new(@bucket_name, @scope_name, @name)
      end

      def binary
        BinaryCollection.new(self)
      end

      def get(id, options = Couchbase::Options::Get::DEFAULT)
        req = @kv_request_generator.get_request(id, options)
        resp = @client.send_request(req)
        ResponseConverter::KV.to_get_result(resp, options)
      end

      def get_and_touch(id, expiry, options = Couchbase::Options::GetAndTouch::DEFAULT)
        req = @kv_request_generator.get_and_touch_request(id, expiry, options)
        resp = @client.send_request(req)
        ResponseConverter::KV.to_get_result(resp, options)
      end

      def get_and_lock(id, lock_time, options = Couchbase::Options::GetAndLock::DEFAULT)
        req = @kv_request_generator.get_and_lock_request(id, lock_time, options)
        resp = @client.send_request(req)
        ResponseConverter::KV.to_get_result(resp, options)
      end

      def unlock(id, cas, options = Couchbase::Options::Unlock::DEFAULT)
        req = @kv_request_generator.unlock_request(id, cas, options)
        @client.send_request(req)
      end

      def touch(id, expiry, options = Couchbase::Options::Touch::DEFAULT)
        req = @kv_request_generator.touch_request(id, expiry, options)
        resp = @client.send_request(req)
        ResponseConverter::KV.to_mutation_result(resp)
      end

      def upsert(id, content, options = Couchbase::Options::Upsert::DEFAULT)
        req = @kv_request_generator.upsert_request(id, content, options)
        resp = @client.send_request(req)
        ResponseConverter::KV.to_mutation_result(resp)
      end

      def remove(id, options = Couchbase::Options::Remove::DEFAULT)
        req = @kv_request_generator.remove_request(id, options)
        resp = @client.send_request(req)
        ResponseConverter::KV.to_mutation_result(resp)
      end

      def insert(id, content, options = Couchbase::Options::Insert::DEFAULT)
        req = @kv_request_generator.insert_request(id, content, options)
        resp = @client.send_request(req)
        ResponseConverter::KV.to_mutation_result(resp)
      end

      def replace(id, content, options = Couchbase::Options::Replace::DEFAULT)
        req = @kv_request_generator.replace_request(id, content, options)
        resp = @client.send_request(req)
        ResponseConverter::KV.to_mutation_result(resp)
      end

      def exists(id, options = Couchbase::Options::Exists::DEFAULT)
        req = @kv_request_generator.exists_request(id, options)
        resp = @client.send_request(req)
        ResponseConverter::KV.to_exists_result(resp)
      end

      def lookup_in(id, specs, options = Couchbase::Options::LookupIn::DEFAULT)
        req = @kv_request_generator.lookup_in_request(id, specs, options)
        resp = @client.send_request(req)
        ResponseConverter::KV.to_lookup_in_result(resp, specs, options)
      end

      def mutate_in(id, specs, options = Couchbase::Options::MutateIn::DEFAULT)
        req = @kv_request_generator.mutate_in_request(id, specs, options)
        resp = @client.send_request(req)
        ResponseConverter::KV.to_mutate_in_result(resp, specs, options)
      end
    end
  end
end
