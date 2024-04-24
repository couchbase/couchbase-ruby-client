# frozen_string_literal: true
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: couchbase/analytics/v1/analytics.proto

require 'google/protobuf'

require 'google/protobuf/duration_pb'


descriptor_data = "\n&couchbase/analytics/v1/analytics.proto\x12\x16\x63ouchbase.analytics.v1\x1a\x1egoogle/protobuf/duration.proto\"\xfc\x04\n\x15\x41nalyticsQueryRequest\x12\x18\n\x0b\x62ucket_name\x18\x08 \x01(\tH\x00\x88\x01\x01\x12\x17\n\nscope_name\x18\t \x01(\tH\x01\x88\x01\x01\x12\x11\n\tstatement\x18\x01 \x01(\t\x12\x16\n\tread_only\x18\x02 \x01(\x08H\x02\x88\x01\x01\x12\x1e\n\x11\x63lient_context_id\x18\x03 \x01(\tH\x03\x88\x01\x01\x12\x15\n\x08priority\x18\x04 \x01(\x08H\x04\x88\x01\x01\x12\\\n\x10scan_consistency\x18\x05 \x01(\x0e\x32=.couchbase.analytics.v1.AnalyticsQueryRequest.ScanConsistencyH\x05\x88\x01\x01\x12\x1d\n\x15positional_parameters\x18\x06 \x03(\x0c\x12\\\n\x10named_parameters\x18\x07 \x03(\x0b\x32\x42.couchbase.analytics.v1.AnalyticsQueryRequest.NamedParametersEntry\x1a\x36\n\x14NamedParametersEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\x0c:\x02\x38\x01\"V\n\x0fScanConsistency\x12 \n\x1cSCAN_CONSISTENCY_NOT_BOUNDED\x10\x00\x12!\n\x1dSCAN_CONSISTENCY_REQUEST_PLUS\x10\x01\x42\x0e\n\x0c_bucket_nameB\r\n\x0b_scope_nameB\x0c\n\n_read_onlyB\x14\n\x12_client_context_idB\x0b\n\t_priorityB\x13\n\x11_scan_consistency\"\xb8\x05\n\x16\x41nalyticsQueryResponse\x12\x0c\n\x04rows\x18\x01 \x03(\x0c\x12O\n\tmeta_data\x18\x02 \x01(\x0b\x32\x37.couchbase.analytics.v1.AnalyticsQueryResponse.MetaDataH\x00\x88\x01\x01\x1a\x8b\x02\n\x07Metrics\x12/\n\x0c\x65lapsed_time\x18\x01 \x01(\x0b\x32\x19.google.protobuf.Duration\x12\x31\n\x0e\x65xecution_time\x18\x02 \x01(\x0b\x32\x19.google.protobuf.Duration\x12\x14\n\x0cresult_count\x18\x03 \x01(\x04\x12\x13\n\x0bresult_size\x18\x04 \x01(\x04\x12\x16\n\x0emutation_count\x18\x05 \x01(\x04\x12\x12\n\nsort_count\x18\x06 \x01(\x04\x12\x13\n\x0b\x65rror_count\x18\x07 \x01(\x04\x12\x15\n\rwarning_count\x18\x08 \x01(\x04\x12\x19\n\x11processed_objects\x18\t \x01(\x04\x1a\xa2\x02\n\x08MetaData\x12\x12\n\nrequest_id\x18\x01 \x01(\t\x12\x19\n\x11\x63lient_context_id\x18\x02 \x01(\t\x12G\n\x07metrics\x18\x03 \x01(\x0b\x32\x36.couchbase.analytics.v1.AnalyticsQueryResponse.Metrics\x12\x11\n\tsignature\x18\x04 \x01(\x0c\x12Q\n\x08warnings\x18\x05 \x03(\x0b\x32?.couchbase.analytics.v1.AnalyticsQueryResponse.MetaData.Warning\x12\x0e\n\x06status\x18\x06 \x01(\t\x1a(\n\x07Warning\x12\x0c\n\x04\x63ode\x18\x01 \x01(\r\x12\x0f\n\x07message\x18\x02 \x01(\tB\x0c\n\n_meta_data2\x87\x01\n\x10\x41nalyticsService\x12s\n\x0e\x41nalyticsQuery\x12-.couchbase.analytics.v1.AnalyticsQueryRequest\x1a..couchbase.analytics.v1.AnalyticsQueryResponse\"\x00\x30\x01\x42\x84\x02\n.com.couchbase.client.protostellar.analytics.v1P\x01ZFgithub.com/couchbase/goprotostellar/genproto/analytics_v1;analytics_v1\xaa\x02#Couchbase.Protostellar.Analytics.V1\xca\x02-Couchbase\\Protostellar\\Generated\\Analytics\\V1\xea\x02\x31\x43ouchbase::Protostellar::Generated::Analytics::V1b\x06proto3"

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
    ["google.protobuf.Duration", "google/protobuf/duration.proto"],
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
      module Analytics
        module V1
          AnalyticsQueryRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.analytics.v1.AnalyticsQueryRequest").msgclass
          AnalyticsQueryRequest::ScanConsistency = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.analytics.v1.AnalyticsQueryRequest.ScanConsistency").enummodule
          AnalyticsQueryResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.analytics.v1.AnalyticsQueryResponse").msgclass
          AnalyticsQueryResponse::Metrics = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.analytics.v1.AnalyticsQueryResponse.Metrics").msgclass
          AnalyticsQueryResponse::MetaData = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.analytics.v1.AnalyticsQueryResponse.MetaData").msgclass
          AnalyticsQueryResponse::MetaData::Warning = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.analytics.v1.AnalyticsQueryResponse.MetaData.Warning").msgclass
        end
      end
    end
  end
end