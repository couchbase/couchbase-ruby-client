# Generated by the protocol buffer compiler.  DO NOT EDIT!
# Source: couchbase/admin/collection.v1.proto for package 'Couchbase.Protostellar.Generated.Admin.Collection.V1'

require 'grpc'
require_relative 'collection.v1_pb'

module Couchbase
  module Protostellar
    module Generated
      module Admin
        module Collection
          module V1
            module CollectionAdmin
              class Service
                include ::GRPC::GenericService

                self.marshal_class_method = :encode
                self.unmarshal_class_method = :decode
                self.service_name = 'couchbase.admin.collection.v1.CollectionAdmin'

                rpc :ListCollections, ::Couchbase::Protostellar::Generated::Admin::Collection::V1::ListCollectionsRequest,
                    ::Couchbase::Protostellar::Generated::Admin::Collection::V1::ListCollectionsResponse
                rpc :CreateScope, ::Couchbase::Protostellar::Generated::Admin::Collection::V1::CreateScopeRequest, ::Couchbase::Protostellar::Generated::Admin::Collection::V1::CreateScopeResponse
                rpc :DeleteScope, ::Couchbase::Protostellar::Generated::Admin::Collection::V1::DeleteScopeRequest, ::Couchbase::Protostellar::Generated::Admin::Collection::V1::DeleteScopeResponse
                rpc :CreateCollection, ::Couchbase::Protostellar::Generated::Admin::Collection::V1::CreateCollectionRequest,
                    ::Couchbase::Protostellar::Generated::Admin::Collection::V1::CreateCollectionResponse
                rpc :DeleteCollection, ::Couchbase::Protostellar::Generated::Admin::Collection::V1::DeleteCollectionRequest,
                    ::Couchbase::Protostellar::Generated::Admin::Collection::V1::DeleteCollectionResponse
              end

              Stub = Service.rpc_stub_class
            end
          end
        end
      end
    end
  end
end