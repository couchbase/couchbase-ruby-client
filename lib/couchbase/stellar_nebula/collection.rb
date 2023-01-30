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
require_relative "get_result"
require_relative "exists_result"
require_relative "mutation_token"
require_relative "mutation_result"
require_relative "upsert_options"
require_relative "remove_options"
require_relative "insert_options"
require_relative "replace_options"
require_relative "exists_options"
require_relative "error"

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
      end

      def location
        {
          bucket_name: @bucket_name,
          scope_name: @scope_name,
          collection_name: @name,
        }
      end

      def get(id, options = GetOptions::DEFAULT)
        req = Generated::KV::V1::GetRequest.new(
          key: id,
          **location,
          **options.to_request
        )
        begin
          resp = @client.get(req, timeout: options.timeout)
        rescue GRPC::NotFound
          raise Error::DocumentNotFound
        rescue GRPC::DeadlineExceeded
          throw Error::Timeout
        end
        GetResult.new(resp)
      end

      def get_and_touch(id, expiry, options = GetAndTouchOptions::DEFAULT)
        expiry = Google::Protobuf::Timestamp.new({:seconds => Utils::Time.extract_expiry_time_point(expiry)})
        req = Generated::KV::V1::GetAndTouchRequest.new(
          **location,
          key: id,
          expiry: expiry,
          **options.to_request
        )
        begin
          resp = @client.get_and_touch(req, timeout: options.timeout)
        rescue GRPC::NotFound
          raise Error::DocumentNotFound
        rescue GRPC::DeadlineExceeded
          throw Error::Timeout
        end
        GetResult.new(resp)
      end

      def upsert(id, content, options = UpsertOptions::DEFAULT)
        encoded = options.transcoder.encode(content)
        req = Generated::KV::V1::UpsertRequest.new(
          key: id,
          content: encoded,
          **location,
          **options.to_request
        )
        begin
          resp = @client.upsert(req, timeout: options.timeout)
        rescue GRPC::DeadlineExceeded
          raise Error::Timeout
        end
        MutationResult.new(resp)
      end

      def remove(id, options = RemoveOptions::DEFAULT)
        req = Generated::KV::V1::RemoveRequest.new(
          key: id,
          **location,
          **options.to_request
        )
        begin
          resp = @client.remove(req, timeout: options.timeout)
        rescue GRPC::DeadlineExceeded
          raise Error::Timeout
        rescue GRPC::NotFound
          raise Error::DocumentNotFound
        end
        MutationResult.new(resp)
      end

      def insert(id, content, options = InsertOptions::DEFAULT)
        encoded = options.transcoder.encode(content)
        req = Generated::KV::V1::InsertRequest.new(
          key: id,
          content: encoded,
          **location,
          **options.to_request
        )
        begin
          resp = @client.insert(req, timeout: options.timeout)
        rescue GRPC::DeadlineExceeded
          raise Error::Timeout
        rescue GRPC::AlreadyExists
          raise Error::DocumentExists
        end
        MutationResult.new(resp)
      end

      def replace(id, content, options = ReplaceOptions::DEFAULT)
        encoded = options.transcoder.encode(content)
        req = Generated::KV::V1::ReplaceRequest.new(
          key: id,
          content: encoded,
          **location,
          **options.to_request
        )
        begin
          resp = @client.replace(req, timeout: options.timeout)
        rescue GRPC::DeadlineExceeded
          raise Error::Timeout
        rescue GRPC::NotFound
          raise Error::DocumentNotFound
        end
        MutationResult.new(resp)
      end

      def exists(id, options = ExistsOptions::DEFAULT)
        req = Generated::KV::V1::ExistsRequest.new(
          key: id,
          **location
        )
        begin
          resp = @client.exists(req, timeout: options.timeout)
        rescue GRPC::DeadlineExceeded
          raise Error::Timeout
        end
        ExistsResult.new(resp)
      end
    end
  end
end
