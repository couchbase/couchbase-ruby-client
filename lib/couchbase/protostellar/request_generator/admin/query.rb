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

require "couchbase/protostellar/generated/admin/query/v1/query_pb"
require "couchbase/protostellar/request"

module Couchbase
  module Protostellar
    module RequestGenerator
      module Admin
        class Query
          def initialize(bucket_name: nil, scope_name: nil, collection_name: nil)
            @bucket_name = bucket_name
            @scope_name = scope_name
            @collection_name = collection_name
          end

          def get_all_indexes_request(options, bucket_name = nil)
            proto_req = Generated::Admin::Query::V1::GetAllIndexesRequest.new(
              **location(bucket_name: bucket_name)
            )

            create_request(proto_req, :get_all_indexes, options, idempotent: true)
          end

          def create_primary_index_request(options, bucket_name = nil)
            proto_opts = {
              deferred: options.deferred,
            }
            proto_opts[:num_replicas] = options.num_replicas unless options.num_replicas.nil?

            proto_req = Generated::Admin::Query::V1::CreatePrimaryIndexRequest.new(
              **location(bucket_name: bucket_name),
              **proto_opts
            )

            create_request(proto_req, :create_primary_index, options)
          end

          def create_index_request(index_name, fields, options, bucket_name = nil)
            proto_opts = {
              deferred: options.deferred,
            }
            proto_opts[:num_replicas] = options.num_replicas unless options.num_replicas.nil?

            proto_req = Generated::Admin::Query::V1::CreateIndexRequest.new(
              **location(bucket_name: bucket_name),
              name: index_name,
              fields: fields,
              **proto_opts
            )

            create_request(proto_req, :create_index, options)
          end

          def drop_primary_index_request(options, bucket_name = nil)
            proto_req = Generated::Admin::Query::V1::DropPrimaryIndexRequest.new(
              **location(bucket_name: bucket_name)
            )

            create_request(proto_req, :drop_primary_index, options)
          end

          def drop_index_request(index_name, options, bucket_name = nil)
            proto_req = Generated::Admin::Query::V1::DropIndexRequest.new(
              **location(bucket_name: bucket_name),
              name: index_name
            )

            create_request(proto_req, :drop_index, options)
          end

          def build_deferred_indexes_request(options, bucket_name = nil)
            proto_req = Generated::Admin::Query::V1::BuildDeferredIndexesRequest.new(
              **location(bucket_name: bucket_name)
            )

            create_request(proto_req, :build_deferred_indexes, options)
          end

          private

          def location(bucket_name: nil)
            {
              bucket_name: bucket_name || @bucket_name,
              scope_name: @scope_name,
              collection_name: @collection_name,
            }.compact
          end

          def create_request(proto_request, rpc, options, idempotent: false)
            Request.new(
              service: :query_admin,
              rpc: rpc,
              proto_request: proto_request,
              idempotent: idempotent,
              timeout: options.timeout
            )
          end
        end
      end
    end
  end
end
