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

require_relative "generated/kv.v1_pb"

require "google/protobuf/well_known_types"

module Couchbase
  module StellarNebula
    class Collection
      attr_reader :name

      def initialize(client, bucket_name, scope_name, name)
        @client = client
        @bucket_name = bucket_name
        @scope_name = scope_name
        @name = name
        @kv_request_generator = RequestGenerator::KV.new(@bucket_name, @scope_name, @name)
      end

      def location
        {
          bucket_name: @bucket_name,
          scope_name: @scope_name,
          collection_name: @name,
        }
      end

      def get(id, options = Couchbase::Options::Get::DEFAULT)
        req = @kv_request_generator.get_request(id, options)
        begin
          resp = @client.get(req, timeout: options.timeout)
        rescue GRPC::NotFound
          raise Couchbase::Error::DocumentNotFound
        rescue GRPC::DeadlineExceeded
          throw Couchbase::Error::Timeout
        end
        ResponseConverter::KV.from_get_response(resp, options)
      end

      def get_and_touch(id, expiry, options = Couchbase::Options::GetAndTouch::DEFAULT)
        req = @kv_request_generator.get_and_touch_request(id, expiry, options)
        begin
          resp = @client.get_and_touch(req, timeout: options.timeout)
        rescue GRPC::NotFound
          raise Couchbase::Error::DocumentNotFound
        rescue GRPC::DeadlineExceeded
          throw Couchbase::Error::Timeout
        end
        ResponseConverter::KV.from_get_response(resp, options)
      end

      def upsert(id, content, options = Couchbase::Options::Upsert::DEFAULT)
        req = @kv_request_generator.upsert_request(id, content, options)
        begin
          resp = @client.upsert(req, timeout: options.timeout)
        rescue GRPC::DeadlineExceeded
          raise Couchbase::Error::Timeout
        end
        ResponseConverter::KV.from_upsert_response(resp)
      end

      def remove(id, options = Couchbase::Options::Remove::DEFAULT)
        req = @kv_request_generator.remove_request(id, options)
        begin
          resp = @client.remove(req, timeout: options.timeout)
        rescue GRPC::DeadlineExceeded
          raise Couchbase::Error::Timeout
        rescue GRPC::NotFound
          raise Couchbase::Error::DocumentNotFound
        end
        ResponseConverter::KV.from_remove_response(resp)
      end

      def insert(id, content, options = Couchbase::Options::Insert::DEFAULT)
        req = @kv_request_generator.insert_request(id, content, options)
        begin
          resp = @client.insert(req, timeout: options.timeout)
        rescue GRPC::DeadlineExceeded
          raise Couchbase::Error::Timeout
        rescue GRPC::AlreadyExists
          raise Couchbase::Error::DocumentExists
        end
        ResponseConverter::KV.from_insert_response(resp)
      end

      def replace(id, content, options = Couchbase::Options::Replace::DEFAULT)
        req = @kv_request_generator.replace_request(id, content, options)
        begin
          resp = @client.replace(req, timeout: options.timeout)
        rescue GRPC::DeadlineExceeded
          raise Couchbase::Error::Timeout
        rescue GRPC::NotFound
          raise Couchbase::Error::DocumentNotFound
        end
        ResponseConverter::KV.from_replace_response(resp)
      end

      def exists(id, options = Couchbase::Options::Exists::DEFAULT)
        req = @kv_request_generator.exists_request(id, options)
        begin
          resp = @client.exists(req, timeout: options.timeout)
        rescue GRPC::DeadlineExceeded
          raise Couchbase::Error::Timeout
        end
        ResponseConverter::KV.from_exists_response(resp)
      end
    end
  end
end
