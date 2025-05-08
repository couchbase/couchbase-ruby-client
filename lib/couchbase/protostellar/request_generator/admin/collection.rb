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

require "couchbase/protostellar/request"
require "couchbase/protostellar/generated/admin/collection/v1/collection_pb"

module Couchbase
  module Protostellar
    module RequestGenerator
      module Admin
        class Collection
          attr_reader :client

          def initialize(bucket_name:)
            @bucket_name = bucket_name
          end

          def list_collections_request(options)
            proto_req = Generated::Admin::Collection::V1::ListCollectionsRequest.new(
              bucket_name: @bucket_name,
            )
            create_request(proto_req, :list_collections, options, idempotent: true)
          end

          def create_scope_request(scope_name, options)
            proto_req = Generated::Admin::Collection::V1::CreateScopeRequest.new(
              bucket_name: @bucket_name,
              scope_name: scope_name,
            )
            create_request(proto_req, :create_scope, options)
          end

          def delete_scope_request(scope_name, options)
            proto_req = Generated::Admin::Collection::V1::DeleteScopeRequest.new(
              bucket_name: @bucket_name,
              scope_name: scope_name,
            )
            create_request(proto_req, :delete_scope, options)
          end

          def create_collection_request(scope_name, collection_name, settings, options)
            proto_opts = {}
            proto_opts[:max_expiry_secs] = settings.max_expiry unless settings.max_expiry.nil?
            unless settings.history.nil?
              raise Couchbase::Error::FeatureNotAvailable, "The #{Protostellar::NAME} protocol does not support the history setting"
            end

            proto_req = Generated::Admin::Collection::V1::CreateCollectionRequest.new(
              bucket_name: @bucket_name,
              scope_name: scope_name,
              collection_name: collection_name,
              **proto_opts,
            )
            create_request(proto_req, :create_collection, options)
          end

          def delete_collection_request(scope_name, collection_name, options)
            proto_req = Generated::Admin::Collection::V1::DeleteCollectionRequest.new(
              bucket_name: @bucket_name,
              scope_name: scope_name,
              collection_name: collection_name,
            )
            create_request(proto_req, :delete_collection, options)
          end

          private

          def create_request(proto_request, rpc, options, idempotent: false)
            Request.new(
              service: :collection_admin,
              rpc: rpc,
              proto_request: proto_request,
              idempotent: idempotent,
              timeout: options.timeout,
            )
          end
        end
      end
    end
  end
end
