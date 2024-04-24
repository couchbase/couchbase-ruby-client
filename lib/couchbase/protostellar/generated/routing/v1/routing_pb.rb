# frozen_string_literal: true
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: couchbase/routing/v1/routing.proto

require 'google/protobuf'


descriptor_data = "\n\"couchbase/routing/v1/routing.proto\x12\x14\x63ouchbase.routing.v1\"D\n\x0fRoutingEndpoint\x12\n\n\x02id\x18\x01 \x01(\t\x12\x14\n\x0cserver_group\x18\x02 \x01(\t\x12\x0f\n\x07\x61\x64\x64ress\x18\x03 \x01(\t\"[\n\x13\x44\x61taRoutingEndpoint\x12\x14\n\x0c\x65ndpoint_idx\x18\x01 \x01(\r\x12\x16\n\x0elocal_vbuckets\x18\x02 \x03(\r\x12\x16\n\x0egroup_vbuckets\x18\x03 \x03(\r\"p\n\x1aVbucketDataRoutingStrategy\x12<\n\tendpoints\x18\x01 \x03(\x0b\x32).couchbase.routing.v1.DataRoutingEndpoint\x12\x14\n\x0cnum_vbuckets\x18\x03 \x01(\r\",\n\x14QueryRoutingEndpoint\x12\x14\n\x0c\x65ndpoint_idx\x18\x01 \x01(\r\"M\n\x0cQueryRouting\x12=\n\tendpoints\x18\x01 \x03(\x0b\x32*.couchbase.routing.v1.QueryRoutingEndpoint\",\n\x14ViewsRoutingEndpoint\x12\x14\n\x0c\x65ndpoint_idx\x18\x01 \x01(\r\"M\n\x0cViewsRouting\x12=\n\tendpoints\x18\x01 \x03(\x0b\x32*.couchbase.routing.v1.ViewsRoutingEndpoint\"?\n\x13WatchRoutingRequest\x12\x18\n\x0b\x62ucket_name\x18\x01 \x01(\tH\x00\x88\x01\x01\x42\x0e\n\x0c_bucket_name\"\xe8\x02\n\x14WatchRoutingResponse\x12\x10\n\x08revision\x18\x01 \x03(\x04\x12\x38\n\tendpoints\x18\x02 \x03(\x0b\x32%.couchbase.routing.v1.RoutingEndpoint\x12P\n\x14vbucket_data_routing\x18\x03 \x01(\x0b\x32\x30.couchbase.routing.v1.VbucketDataRoutingStrategyH\x00\x12>\n\rquery_routing\x18\x04 \x01(\x0b\x32\".couchbase.routing.v1.QueryRoutingH\x01\x88\x01\x01\x12>\n\rviews_routing\x18\x05 \x01(\x0b\x32\".couchbase.routing.v1.ViewsRoutingH\x02\x88\x01\x01\x42\x0e\n\x0c\x64\x61ta_routingB\x10\n\x0e_query_routingB\x10\n\x0e_views_routing2{\n\x0eRoutingService\x12i\n\x0cWatchRouting\x12).couchbase.routing.v1.WatchRoutingRequest\x1a*.couchbase.routing.v1.WatchRoutingResponse\"\x00\x30\x01\x42\xf8\x01\n,com.couchbase.client.protostellar.routing.v1P\x01ZBgithub.com/couchbase/goprotostellar/genproto/routing_v1;routing_v1\xaa\x02!Couchbase.Protostellar.Routing.V1\xca\x02+Couchbase\\Protostellar\\Generated\\Routing\\V1\xea\x02/Couchbase::Protostellar::Generated::Routing::V1b\x06proto3"

pool = Google::Protobuf::DescriptorPool.generated_pool

begin
  pool.add_serialized_file(descriptor_data)
rescue TypeError => e
  # Compatibility code: will be removed in the next major version.
  require 'google/protobuf/descriptor_pb'
  parsed = Google::Protobuf::FileDescriptorProto.decode(descriptor_data)
  parsed.clear_dependency
  serialized = parsed.class.encode(parsed)
  file = pool.add_serialized_file(serialized)
  warn "Warning: Protobuf detected an import path issue while loading generated file #{__FILE__}"
  imports = [
  ]
  imports.each do |type_name, expected_filename|
    import_file = pool.lookup(type_name).file_descriptor
    if import_file.name != expected_filename
      warn "- #{file.name} imports #{expected_filename}, but that import was loaded as #{import_file.name}"
    end
  end
  warn "Each proto file must use a consistent fully-qualified name."
  warn "This will become an error in the next major version."
end

module Couchbase
  module Protostellar
    module Generated
      module Routing
        module V1
          RoutingEndpoint = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.routing.v1.RoutingEndpoint").msgclass
          DataRoutingEndpoint = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.routing.v1.DataRoutingEndpoint").msgclass
          VbucketDataRoutingStrategy = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.routing.v1.VbucketDataRoutingStrategy").msgclass
          QueryRoutingEndpoint = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.routing.v1.QueryRoutingEndpoint").msgclass
          QueryRouting = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.routing.v1.QueryRouting").msgclass
          ViewsRoutingEndpoint = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.routing.v1.ViewsRoutingEndpoint").msgclass
          ViewsRouting = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.routing.v1.ViewsRouting").msgclass
          WatchRoutingRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.routing.v1.WatchRoutingRequest").msgclass
          WatchRoutingResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.routing.v1.WatchRoutingResponse").msgclass
        end
      end
    end
  end
end