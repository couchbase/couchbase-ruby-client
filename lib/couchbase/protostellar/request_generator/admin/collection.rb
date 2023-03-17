# frozen_string_literal: true

require "couchbase/protostellar/request"
require "couchbase/protostellar/generated/admin/collection.v1_pb"

module Couchbase
  module Protostellar
    module RequestGenerator
      module Admin
        class Collection
          attr_reader :client
          attr_reader :default_timeout

          def initialize(bucket_name:, default_timeout: nil)
            @bucket_name = bucket_name

            # TODO: Use the KV timeout from the cluster's options
            @default_timeout = default_timeout.nil? ? TimeoutDefaults::KEY_VALUE : default_timeout
          end

          def list_collections_request(options)
            proto_req = Generated::Admin::Collection::V1::ListCollectionsRequest.new(
              bucket_name: @bucket_name
            )
            create_request(proto_req, :list_collections, options, idempotent: true)
          end

          def create_scope_request(scope_name, options)
            proto_req = Generated::Admin::Collection::V1::CreateScopeRequest.new(
              bucket_name: @bucket_name,
              scope_name: scope_name
            )
            create_request(proto_req, :create_scope, options)
          end

          def delete_scope_request(scope_name, options)
            proto_req = Generated::Admin::Collection::V1::DeleteScopeRequest.new(
              bucket_name: @bucket_name,
              scope_name: scope_name
            )
            create_request(proto_req, :delete_scope, options)
          end

          def create_collection_request(collection_spec, options)
            proto_opts = {}
            proto_opts[:max_expiry_secs] = collection_spec.max_expiry unless collection_spec.max_expiry.nil?

            proto_req = Generated::Admin::Collection::V1::CreateCollectionRequest.new(
              bucket_name: @bucket_name,
              scope_name: collection_spec.scope_name,
              collection_name: collection_spec.name,
              **proto_opts
            )
            create_request(proto_req, :create_collection, options)
          end

          def delete_collection_request(collection_spec, options)
            proto_req = Generated::Admin::Collection::V1::DeleteCollectionRequest.new(
              bucket_name: @bucket_name,
              scope_name: collection_spec.scope_name,
              collection_name: collection_spec.name
            )
            create_request(proto_req, :delete_collection, options)
          end

          private

          def create_request(proto_request, rpc, options, idempotent: false)
            req = Request.new(
              service: :collection_admin,
              rpc: rpc,
              proto_request: proto_request,
              idempotent: idempotent,
              timeout: get_timeout(options)
            )
          end

          def get_timeout(options)
            if options.timeout.nil?
              @default_timeout
            else
              options.timeout
            end
          end
        end
      end
    end
  end
end
