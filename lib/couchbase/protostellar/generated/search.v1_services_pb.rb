# Generated by the protocol buffer compiler.  DO NOT EDIT!
# Source: couchbase/search.v1.proto for package 'Couchbase.Protostellar.Generated.Search.V1'

require 'grpc'
require_relative 'search.v1_pb'

module Couchbase
  module Protostellar
    module Generated
      module Search
        module V1
          module Search
            class Service
              include ::GRPC::GenericService

              self.marshal_class_method = :encode
              self.unmarshal_class_method = :decode
              self.service_name = 'couchbase.search.v1.Search'

              rpc :SearchQuery, ::Couchbase::Protostellar::Generated::Search::V1::SearchQueryRequest, stream(::Couchbase::Protostellar::Generated::Search::V1::SearchQueryResponse)
            end

            Stub = Service.rpc_stub_class
          end
        end
      end
    end
  end
end
