# Generated by the protocol buffer compiler.  DO NOT EDIT!
# Source: couchbase/routing/v1/routing.proto for package 'Couchbase.Protostellar.Generated.Routing.V1'

require 'grpc'
require 'couchbase/protostellar/generated/routing/v1/routing_pb'

module Couchbase
  module Protostellar
    module Generated
      module Routing
        module V1
          module RoutingService
            class Service

              include ::GRPC::GenericService

              self.marshal_class_method = :encode
              self.unmarshal_class_method = :decode
              self.service_name = 'couchbase.routing.v1.RoutingService'

              rpc :WatchRouting, ::Couchbase::Protostellar::Generated::Routing::V1::WatchRoutingRequest, stream(::Couchbase::Protostellar::Generated::Routing::V1::WatchRoutingResponse)
            end

            Stub = Service.rpc_stub_class
          end
        end
      end
    end
  end
end
