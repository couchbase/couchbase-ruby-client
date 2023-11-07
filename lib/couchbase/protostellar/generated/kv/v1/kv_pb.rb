# frozen_string_literal: true
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: couchbase/kv/v1/kv.proto

require 'google/protobuf'

require 'google/rpc/status_pb'
require 'google/protobuf/timestamp_pb'


descriptor_data = "\n\x18\x63ouchbase/kv/v1/kv.proto\x12\x0f\x63ouchbase.kv.v1\x1a\x17google/rpc/status.proto\x1a\x1fgoogle/protobuf/timestamp.proto\"E\n\x14LegacyDurabilitySpec\x12\x16\n\x0enum_replicated\x18\x01 \x01(\r\x12\x15\n\rnum_persisted\x18\x02 \x01(\r\"^\n\rMutationToken\x12\x13\n\x0b\x62ucket_name\x18\x01 \x01(\t\x12\x12\n\nvbucket_id\x18\x02 \x01(\r\x12\x14\n\x0cvbucket_uuid\x18\x03 \x01(\x04\x12\x0e\n\x06seq_no\x18\x04 \x01(\x04\"\xbb\x01\n\nGetRequest\x12\x13\n\x0b\x62ucket_name\x18\x01 \x01(\t\x12\x12\n\nscope_name\x18\x02 \x01(\t\x12\x17\n\x0f\x63ollection_name\x18\x03 \x01(\t\x12\x0b\n\x03key\x18\x04 \x01(\t\x12\x0f\n\x07project\x18\x05 \x03(\t\x12=\n\x0b\x63ompression\x18\x06 \x01(\x0e\x32#.couchbase.kv.v1.CompressionEnabledH\x00\x88\x01\x01\x42\x0e\n\x0c_compression\"\xb2\x01\n\x0bGetResponse\x12\x1e\n\x14\x63ontent_uncompressed\x18\x01 \x01(\x0cH\x00\x12\x1c\n\x12\x63ontent_compressed\x18\x07 \x01(\x0cH\x00\x12\x15\n\rcontent_flags\x18\x06 \x01(\r\x12\x0b\n\x03\x63\x61s\x18\x03 \x01(\x04\x12*\n\x06\x65xpiry\x18\x04 \x01(\x0b\x32\x1a.google.protobuf.TimestampB\t\n\x07\x63ontentJ\x04\x08\x02\x10\x03J\x04\x08\x05\x10\x06\"\x86\x02\n\x12GetAndTouchRequest\x12\x13\n\x0b\x62ucket_name\x18\x01 \x01(\t\x12\x12\n\nscope_name\x18\x02 \x01(\t\x12\x17\n\x0f\x63ollection_name\x18\x03 \x01(\t\x12\x0b\n\x03key\x18\x04 \x01(\t\x12\x31\n\x0b\x65xpiry_time\x18\x05 \x01(\x0b\x32\x1a.google.protobuf.TimestampH\x00\x12\x15\n\x0b\x65xpiry_secs\x18\x06 \x01(\rH\x00\x12=\n\x0b\x63ompression\x18\x07 \x01(\x0e\x32#.couchbase.kv.v1.CompressionEnabledH\x01\x88\x01\x01\x42\x08\n\x06\x65xpiryB\x0e\n\x0c_compression\"\xba\x01\n\x13GetAndTouchResponse\x12\x1e\n\x14\x63ontent_uncompressed\x18\x01 \x01(\x0cH\x00\x12\x1c\n\x12\x63ontent_compressed\x18\x07 \x01(\x0cH\x00\x12\x15\n\rcontent_flags\x18\x06 \x01(\r\x12\x0b\n\x03\x63\x61s\x18\x03 \x01(\x04\x12*\n\x06\x65xpiry\x18\x04 \x01(\x0b\x32\x1a.google.protobuf.TimestampB\t\n\x07\x63ontentJ\x04\x08\x02\x10\x03J\x04\x08\x05\x10\x06\"\xc4\x01\n\x11GetAndLockRequest\x12\x13\n\x0b\x62ucket_name\x18\x01 \x01(\t\x12\x12\n\nscope_name\x18\x02 \x01(\t\x12\x17\n\x0f\x63ollection_name\x18\x03 \x01(\t\x12\x0b\n\x03key\x18\x04 \x01(\t\x12\x11\n\tlock_time\x18\x05 \x01(\r\x12=\n\x0b\x63ompression\x18\x06 \x01(\x0e\x32#.couchbase.kv.v1.CompressionEnabledH\x00\x88\x01\x01\x42\x0e\n\x0c_compression\"\xb9\x01\n\x12GetAndLockResponse\x12\x1e\n\x14\x63ontent_uncompressed\x18\x01 \x01(\x0cH\x00\x12\x1c\n\x12\x63ontent_compressed\x18\x07 \x01(\x0cH\x00\x12\x15\n\rcontent_flags\x18\x06 \x01(\r\x12\x0b\n\x03\x63\x61s\x18\x03 \x01(\x04\x12*\n\x06\x65xpiry\x18\x04 \x01(\x0b\x32\x1a.google.protobuf.TimestampB\t\n\x07\x63ontentJ\x04\x08\x02\x10\x03J\x04\x08\x05\x10\x06\"k\n\rUnlockRequest\x12\x13\n\x0b\x62ucket_name\x18\x01 \x01(\t\x12\x12\n\nscope_name\x18\x02 \x01(\t\x12\x17\n\x0f\x63ollection_name\x18\x03 \x01(\t\x12\x0b\n\x03key\x18\x04 \x01(\t\x12\x0b\n\x03\x63\x61s\x18\x05 \x01(\x04\"\x10\n\x0eUnlockResponse\"\xb1\x01\n\x0cTouchRequest\x12\x13\n\x0b\x62ucket_name\x18\x01 \x01(\t\x12\x12\n\nscope_name\x18\x02 \x01(\t\x12\x17\n\x0f\x63ollection_name\x18\x03 \x01(\t\x12\x0b\n\x03key\x18\x04 \x01(\t\x12\x31\n\x0b\x65xpiry_time\x18\x05 \x01(\x0b\x32\x1a.google.protobuf.TimestampH\x00\x12\x15\n\x0b\x65xpiry_secs\x18\x06 \x01(\rH\x00\x42\x08\n\x06\x65xpiry\"T\n\rTouchResponse\x12\x0b\n\x03\x63\x61s\x18\x01 \x01(\x04\x12\x36\n\x0emutation_token\x18\x02 \x01(\x0b\x32\x1e.couchbase.kv.v1.MutationToken\"^\n\rExistsRequest\x12\x13\n\x0b\x62ucket_name\x18\x01 \x01(\t\x12\x12\n\nscope_name\x18\x02 \x01(\t\x12\x17\n\x0f\x63ollection_name\x18\x03 \x01(\t\x12\x0b\n\x03key\x18\x04 \x01(\t\"-\n\x0e\x45xistsResponse\x12\x0e\n\x06result\x18\x01 \x01(\x08\x12\x0b\n\x03\x63\x61s\x18\x02 \x01(\x04\"\xee\x02\n\rInsertRequest\x12\x13\n\x0b\x62ucket_name\x18\x01 \x01(\t\x12\x12\n\nscope_name\x18\x02 \x01(\t\x12\x17\n\x0f\x63ollection_name\x18\x03 \x01(\t\x12\x0b\n\x03key\x18\x04 \x01(\t\x12\x1e\n\x14\x63ontent_uncompressed\x18\x05 \x01(\x0cH\x00\x12\x1c\n\x12\x63ontent_compressed\x18\x0c \x01(\x0cH\x00\x12\x15\n\rcontent_flags\x18\x0b \x01(\r\x12\x31\n\x0b\x65xpiry_time\x18\x07 \x01(\x0b\x32\x1a.google.protobuf.TimestampH\x01\x12\x15\n\x0b\x65xpiry_secs\x18\n \x01(\rH\x01\x12?\n\x10\x64urability_level\x18\t \x01(\x0e\x32 .couchbase.kv.v1.DurabilityLevelH\x02\x88\x01\x01\x42\t\n\x07\x63ontentB\x08\n\x06\x65xpiryB\x13\n\x11_durability_levelJ\x04\x08\x06\x10\x07\"U\n\x0eInsertResponse\x12\x0b\n\x03\x63\x61s\x18\x01 \x01(\x04\x12\x36\n\x0emutation_token\x18\x02 \x01(\x0b\x32\x1e.couchbase.kv.v1.MutationToken\"\xb8\x03\n\rUpsertRequest\x12\x13\n\x0b\x62ucket_name\x18\x01 \x01(\t\x12\x12\n\nscope_name\x18\x02 \x01(\t\x12\x17\n\x0f\x63ollection_name\x18\x03 \x01(\t\x12\x0b\n\x03key\x18\x04 \x01(\t\x12\x1e\n\x14\x63ontent_uncompressed\x18\x05 \x01(\x0cH\x00\x12\x1c\n\x12\x63ontent_compressed\x18\r \x01(\x0cH\x00\x12\x15\n\rcontent_flags\x18\x0b \x01(\r\x12\x31\n\x0b\x65xpiry_time\x18\x07 \x01(\x0b\x32\x1a.google.protobuf.TimestampH\x01\x12\x15\n\x0b\x65xpiry_secs\x18\n \x01(\rH\x01\x12(\n\x1bpreserve_expiry_on_existing\x18\x0c \x01(\x08H\x02\x88\x01\x01\x12?\n\x10\x64urability_level\x18\t \x01(\x0e\x32 .couchbase.kv.v1.DurabilityLevelH\x03\x88\x01\x01\x42\t\n\x07\x63ontentB\x08\n\x06\x65xpiryB\x1e\n\x1c_preserve_expiry_on_existingB\x13\n\x11_durability_levelJ\x04\x08\x06\x10\x07\"U\n\x0eUpsertResponse\x12\x0b\n\x03\x63\x61s\x18\x01 \x01(\x04\x12\x36\n\x0emutation_token\x18\x02 \x01(\x0b\x32\x1e.couchbase.kv.v1.MutationToken\"\x89\x03\n\x0eReplaceRequest\x12\x13\n\x0b\x62ucket_name\x18\x01 \x01(\t\x12\x12\n\nscope_name\x18\x02 \x01(\t\x12\x17\n\x0f\x63ollection_name\x18\x03 \x01(\t\x12\x0b\n\x03key\x18\x04 \x01(\t\x12\x1e\n\x14\x63ontent_uncompressed\x18\x05 \x01(\x0cH\x00\x12\x1c\n\x12\x63ontent_compressed\x18\r \x01(\x0cH\x00\x12\x15\n\rcontent_flags\x18\x0c \x01(\r\x12\x10\n\x03\x63\x61s\x18\x07 \x01(\x04H\x02\x88\x01\x01\x12\x31\n\x0b\x65xpiry_time\x18\x08 \x01(\x0b\x32\x1a.google.protobuf.TimestampH\x01\x12\x15\n\x0b\x65xpiry_secs\x18\x0b \x01(\rH\x01\x12?\n\x10\x64urability_level\x18\n \x01(\x0e\x32 .couchbase.kv.v1.DurabilityLevelH\x03\x88\x01\x01\x42\t\n\x07\x63ontentB\x08\n\x06\x65xpiryB\x06\n\x04_casB\x13\n\x11_durability_levelJ\x04\x08\x06\x10\x07\"V\n\x0fReplaceResponse\x12\x0b\n\x03\x63\x61s\x18\x01 \x01(\x04\x12\x36\n\x0emutation_token\x18\x02 \x01(\x0b\x32\x1e.couchbase.kv.v1.MutationToken\"\xce\x01\n\rRemoveRequest\x12\x13\n\x0b\x62ucket_name\x18\x01 \x01(\t\x12\x12\n\nscope_name\x18\x02 \x01(\t\x12\x17\n\x0f\x63ollection_name\x18\x03 \x01(\t\x12\x0b\n\x03key\x18\x04 \x01(\t\x12\x10\n\x03\x63\x61s\x18\x05 \x01(\x04H\x00\x88\x01\x01\x12?\n\x10\x64urability_level\x18\x07 \x01(\x0e\x32 .couchbase.kv.v1.DurabilityLevelH\x01\x88\x01\x01\x42\x06\n\x04_casB\x13\n\x11_durability_level\"U\n\x0eRemoveResponse\x12\x0b\n\x03\x63\x61s\x18\x01 \x01(\x04\x12\x36\n\x0emutation_token\x18\x02 \x01(\x0b\x32\x1e.couchbase.kv.v1.MutationToken\"\xbc\x02\n\x10IncrementRequest\x12\x13\n\x0b\x62ucket_name\x18\x01 \x01(\t\x12\x12\n\nscope_name\x18\x02 \x01(\t\x12\x17\n\x0f\x63ollection_name\x18\x03 \x01(\t\x12\x0b\n\x03key\x18\x04 \x01(\t\x12\r\n\x05\x64\x65lta\x18\x05 \x01(\x04\x12\x31\n\x0b\x65xpiry_time\x18\x06 \x01(\x0b\x32\x1a.google.protobuf.TimestampH\x00\x12\x15\n\x0b\x65xpiry_secs\x18\n \x01(\rH\x00\x12\x14\n\x07initial\x18\x07 \x01(\x03H\x01\x88\x01\x01\x12?\n\x10\x64urability_level\x18\t \x01(\x0e\x32 .couchbase.kv.v1.DurabilityLevelH\x02\x88\x01\x01\x42\x08\n\x06\x65xpiryB\n\n\x08_initialB\x13\n\x11_durability_level\"i\n\x11IncrementResponse\x12\x0b\n\x03\x63\x61s\x18\x01 \x01(\x04\x12\x0f\n\x07\x63ontent\x18\x02 \x01(\x03\x12\x36\n\x0emutation_token\x18\x03 \x01(\x0b\x32\x1e.couchbase.kv.v1.MutationToken\"\xbc\x02\n\x10\x44\x65\x63rementRequest\x12\x13\n\x0b\x62ucket_name\x18\x01 \x01(\t\x12\x12\n\nscope_name\x18\x02 \x01(\t\x12\x17\n\x0f\x63ollection_name\x18\x03 \x01(\t\x12\x0b\n\x03key\x18\x04 \x01(\t\x12\r\n\x05\x64\x65lta\x18\x05 \x01(\x04\x12\x31\n\x0b\x65xpiry_time\x18\x06 \x01(\x0b\x32\x1a.google.protobuf.TimestampH\x00\x12\x15\n\x0b\x65xpiry_secs\x18\n \x01(\rH\x00\x12\x14\n\x07initial\x18\x07 \x01(\x03H\x01\x88\x01\x01\x12?\n\x10\x64urability_level\x18\t \x01(\x0e\x32 .couchbase.kv.v1.DurabilityLevelH\x02\x88\x01\x01\x42\x08\n\x06\x65xpiryB\n\n\x08_initialB\x13\n\x11_durability_level\"i\n\x11\x44\x65\x63rementResponse\x12\x0b\n\x03\x63\x61s\x18\x01 \x01(\x04\x12\x0f\n\x07\x63ontent\x18\x02 \x01(\x03\x12\x36\n\x0emutation_token\x18\x03 \x01(\x0b\x32\x1e.couchbase.kv.v1.MutationToken\"\xdf\x01\n\rAppendRequest\x12\x13\n\x0b\x62ucket_name\x18\x01 \x01(\t\x12\x12\n\nscope_name\x18\x02 \x01(\t\x12\x17\n\x0f\x63ollection_name\x18\x03 \x01(\t\x12\x0b\n\x03key\x18\x04 \x01(\t\x12\x0f\n\x07\x63ontent\x18\x05 \x01(\x0c\x12\x10\n\x03\x63\x61s\x18\x06 \x01(\x04H\x00\x88\x01\x01\x12?\n\x10\x64urability_level\x18\x08 \x01(\x0e\x32 .couchbase.kv.v1.DurabilityLevelH\x01\x88\x01\x01\x42\x06\n\x04_casB\x13\n\x11_durability_level\"U\n\x0e\x41ppendResponse\x12\x0b\n\x03\x63\x61s\x18\x01 \x01(\x04\x12\x36\n\x0emutation_token\x18\x02 \x01(\x0b\x32\x1e.couchbase.kv.v1.MutationToken\"\xe0\x01\n\x0ePrependRequest\x12\x13\n\x0b\x62ucket_name\x18\x01 \x01(\t\x12\x12\n\nscope_name\x18\x02 \x01(\t\x12\x17\n\x0f\x63ollection_name\x18\x03 \x01(\t\x12\x0b\n\x03key\x18\x04 \x01(\t\x12\x0f\n\x07\x63ontent\x18\x05 \x01(\x0c\x12\x10\n\x03\x63\x61s\x18\x06 \x01(\x04H\x00\x88\x01\x01\x12?\n\x10\x64urability_level\x18\x08 \x01(\x0e\x32 .couchbase.kv.v1.DurabilityLevelH\x01\x88\x01\x01\x42\x06\n\x04_casB\x13\n\x11_durability_level\"V\n\x0fPrependResponse\x12\x0b\n\x03\x63\x61s\x18\x01 \x01(\x04\x12\x36\n\x0emutation_token\x18\x02 \x01(\x0b\x32\x1e.couchbase.kv.v1.MutationToken\"\xad\x04\n\x0fLookupInRequest\x12\x13\n\x0b\x62ucket_name\x18\x01 \x01(\t\x12\x12\n\nscope_name\x18\x02 \x01(\t\x12\x17\n\x0f\x63ollection_name\x18\x03 \x01(\t\x12\x0b\n\x03key\x18\x04 \x01(\t\x12\x34\n\x05specs\x18\x05 \x03(\x0b\x32%.couchbase.kv.v1.LookupInRequest.Spec\x12:\n\x05\x66lags\x18\x06 \x01(\x0b\x32&.couchbase.kv.v1.LookupInRequest.FlagsH\x00\x88\x01\x01\x1a\x95\x02\n\x04Spec\x12\x42\n\toperation\x18\x01 \x01(\x0e\x32/.couchbase.kv.v1.LookupInRequest.Spec.Operation\x12\x0c\n\x04path\x18\x02 \x01(\t\x12?\n\x05\x66lags\x18\x03 \x01(\x0b\x32+.couchbase.kv.v1.LookupInRequest.Spec.FlagsH\x00\x88\x01\x01\x1a%\n\x05\x46lags\x12\x12\n\x05xattr\x18\x01 \x01(\x08H\x00\x88\x01\x01\x42\x08\n\x06_xattr\"I\n\tOperation\x12\x11\n\rOPERATION_GET\x10\x00\x12\x14\n\x10OPERATION_EXISTS\x10\x01\x12\x13\n\x0fOPERATION_COUNT\x10\x02\x42\x08\n\x06_flags\x1a\x37\n\x05\x46lags\x12\x1b\n\x0e\x61\x63\x63\x65ss_deleted\x18\x01 \x01(\x08H\x00\x88\x01\x01\x42\x11\n\x0f_access_deletedB\x08\n\x06_flags\"\x93\x01\n\x10LookupInResponse\x12\x35\n\x05specs\x18\x01 \x03(\x0b\x32&.couchbase.kv.v1.LookupInResponse.Spec\x12\x0b\n\x03\x63\x61s\x18\x02 \x01(\x04\x1a;\n\x04Spec\x12\"\n\x06status\x18\x01 \x01(\x0b\x32\x12.google.rpc.Status\x12\x0f\n\x07\x63ontent\x18\x02 \x01(\x0c\"\x97\t\n\x0fMutateInRequest\x12\x13\n\x0b\x62ucket_name\x18\x01 \x01(\t\x12\x12\n\nscope_name\x18\x02 \x01(\t\x12\x17\n\x0f\x63ollection_name\x18\x03 \x01(\t\x12\x0b\n\x03key\x18\x04 \x01(\t\x12\x34\n\x05specs\x18\x05 \x03(\x0b\x32%.couchbase.kv.v1.MutateInRequest.Spec\x12K\n\x0estore_semantic\x18\x06 \x01(\x0e\x32..couchbase.kv.v1.MutateInRequest.StoreSemanticH\x01\x88\x01\x01\x12?\n\x10\x64urability_level\x18\x08 \x01(\x0e\x32 .couchbase.kv.v1.DurabilityLevelH\x02\x88\x01\x01\x12\x10\n\x03\x63\x61s\x18\t \x01(\x04H\x03\x88\x01\x01\x12:\n\x05\x66lags\x18\n \x01(\x0b\x32&.couchbase.kv.v1.MutateInRequest.FlagsH\x04\x88\x01\x01\x12\x31\n\x0b\x65xpiry_time\x18\x0b \x01(\x0b\x32\x1a.google.protobuf.TimestampH\x00\x12\x15\n\x0b\x65xpiry_secs\x18\x0c \x01(\rH\x00\x1a\xf8\x03\n\x04Spec\x12\x42\n\toperation\x18\x01 \x01(\x0e\x32/.couchbase.kv.v1.MutateInRequest.Spec.Operation\x12\x0c\n\x04path\x18\x02 \x01(\t\x12\x0f\n\x07\x63ontent\x18\x03 \x01(\x0c\x12?\n\x05\x66lags\x18\x04 \x01(\x0b\x32+.couchbase.kv.v1.MutateInRequest.Spec.FlagsH\x00\x88\x01\x01\x1aO\n\x05\x46lags\x12\x18\n\x0b\x63reate_path\x18\x01 \x01(\x08H\x00\x88\x01\x01\x12\x12\n\x05xattr\x18\x02 \x01(\x08H\x01\x88\x01\x01\x42\x0e\n\x0c_create_pathB\x08\n\x06_xattr\"\xf0\x01\n\tOperation\x12\x14\n\x10OPERATION_INSERT\x10\x00\x12\x14\n\x10OPERATION_UPSERT\x10\x01\x12\x15\n\x11OPERATION_REPLACE\x10\x02\x12\x14\n\x10OPERATION_REMOVE\x10\x03\x12\x1a\n\x16OPERATION_ARRAY_APPEND\x10\x04\x12\x1b\n\x17OPERATION_ARRAY_PREPEND\x10\x05\x12\x1a\n\x16OPERATION_ARRAY_INSERT\x10\x06\x12\x1e\n\x1aOPERATION_ARRAY_ADD_UNIQUE\x10\x07\x12\x15\n\x11OPERATION_COUNTER\x10\x08\x42\x08\n\x06_flags\x1a\x37\n\x05\x46lags\x12\x1b\n\x0e\x61\x63\x63\x65ss_deleted\x18\x01 \x01(\x08H\x00\x88\x01\x01\x42\x11\n\x0f_access_deleted\"a\n\rStoreSemantic\x12\x1a\n\x16STORE_SEMANTIC_REPLACE\x10\x00\x12\x19\n\x15STORE_SEMANTIC_UPSERT\x10\x01\x12\x19\n\x15STORE_SEMANTIC_INSERT\x10\x02\x42\x08\n\x06\x65xpiryB\x11\n\x0f_store_semanticB\x13\n\x11_durability_levelB\x06\n\x04_casB\x08\n\x06_flags\"\xb8\x01\n\x10MutateInResponse\x12\x35\n\x05specs\x18\x01 \x03(\x0b\x32&.couchbase.kv.v1.MutateInResponse.Spec\x12\x0b\n\x03\x63\x61s\x18\x02 \x01(\x04\x12\x36\n\x0emutation_token\x18\x03 \x01(\x0b\x32\x1e.couchbase.kv.v1.MutationToken\x1a(\n\x04Spec\x12\x14\n\x07\x63ontent\x18\x01 \x01(\x0cH\x00\x88\x01\x01\x42\n\n\x08_content\"f\n\x15GetAllReplicasRequest\x12\x13\n\x0b\x62ucket_name\x18\x01 \x01(\t\x12\x12\n\nscope_name\x18\x02 \x01(\t\x12\x17\n\x0f\x63ollection_name\x18\x03 \x01(\t\x12\x0b\n\x03key\x18\x04 \x01(\t\"a\n\x16GetAllReplicasResponse\x12\x12\n\nis_replica\x18\x01 \x01(\x08\x12\x0f\n\x07\x63ontent\x18\x02 \x01(\x0c\x12\x15\n\rcontent_flags\x18\x03 \x01(\r\x12\x0b\n\x03\x63\x61s\x18\x04 \x01(\x04*\x8f\x01\n\x0f\x44urabilityLevel\x12\x1d\n\x19\x44URABILITY_LEVEL_MAJORITY\x10\x00\x12\x33\n/DURABILITY_LEVEL_MAJORITY_AND_PERSIST_TO_ACTIVE\x10\x01\x12(\n$DURABILITY_LEVEL_PERSIST_TO_MAJORITY\x10\x02*V\n\x12\x43ompressionEnabled\x12 \n\x1c\x43OMPRESSION_ENABLED_OPTIONAL\x10\x00\x12\x1e\n\x1a\x43OMPRESSION_ENABLED_ALWAYS\x10\x01\x32\xf5\n\n\tKvService\x12\x42\n\x03Get\x12\x1b.couchbase.kv.v1.GetRequest\x1a\x1c.couchbase.kv.v1.GetResponse\"\x00\x12Z\n\x0bGetAndTouch\x12#.couchbase.kv.v1.GetAndTouchRequest\x1a$.couchbase.kv.v1.GetAndTouchResponse\"\x00\x12W\n\nGetAndLock\x12\".couchbase.kv.v1.GetAndLockRequest\x1a#.couchbase.kv.v1.GetAndLockResponse\"\x00\x12K\n\x06Unlock\x12\x1e.couchbase.kv.v1.UnlockRequest\x1a\x1f.couchbase.kv.v1.UnlockResponse\"\x00\x12H\n\x05Touch\x12\x1d.couchbase.kv.v1.TouchRequest\x1a\x1e.couchbase.kv.v1.TouchResponse\"\x00\x12K\n\x06\x45xists\x12\x1e.couchbase.kv.v1.ExistsRequest\x1a\x1f.couchbase.kv.v1.ExistsResponse\"\x00\x12K\n\x06Insert\x12\x1e.couchbase.kv.v1.InsertRequest\x1a\x1f.couchbase.kv.v1.InsertResponse\"\x00\x12K\n\x06Upsert\x12\x1e.couchbase.kv.v1.UpsertRequest\x1a\x1f.couchbase.kv.v1.UpsertResponse\"\x00\x12N\n\x07Replace\x12\x1f.couchbase.kv.v1.ReplaceRequest\x1a .couchbase.kv.v1.ReplaceResponse\"\x00\x12K\n\x06Remove\x12\x1e.couchbase.kv.v1.RemoveRequest\x1a\x1f.couchbase.kv.v1.RemoveResponse\"\x00\x12T\n\tIncrement\x12!.couchbase.kv.v1.IncrementRequest\x1a\".couchbase.kv.v1.IncrementResponse\"\x00\x12T\n\tDecrement\x12!.couchbase.kv.v1.DecrementRequest\x1a\".couchbase.kv.v1.DecrementResponse\"\x00\x12K\n\x06\x41ppend\x12\x1e.couchbase.kv.v1.AppendRequest\x1a\x1f.couchbase.kv.v1.AppendResponse\"\x00\x12N\n\x07Prepend\x12\x1f.couchbase.kv.v1.PrependRequest\x1a .couchbase.kv.v1.PrependResponse\"\x00\x12Q\n\x08LookupIn\x12 .couchbase.kv.v1.LookupInRequest\x1a!.couchbase.kv.v1.LookupInResponse\"\x00\x12Q\n\x08MutateIn\x12 .couchbase.kv.v1.MutateInRequest\x1a!.couchbase.kv.v1.MutateInResponse\"\x00\x12\x65\n\x0eGetAllReplicas\x12&.couchbase.kv.v1.GetAllReplicasRequest\x1a\'.couchbase.kv.v1.GetAllReplicasResponse\"\x00\x30\x01\x42\xda\x01\n\'com.couchbase.client.protostellar.kv.v1P\x01Z8github.com/couchbase/goprotostellar/genproto/kv_v1;kv_v1\xaa\x02\x1c\x43ouchbase.Protostellar.KV.V1\xca\x02&Couchbase\\Protostellar\\Generated\\KV\\V1\xea\x02*Couchbase::Protostellar::Generated::KV::V1b\x06proto3"

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
    ["google.protobuf.Timestamp", "google/protobuf/timestamp.proto"],
    ["google.rpc.Status", "google/rpc/status.proto"],
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
      module KV
        module V1
          LegacyDurabilitySpec = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.LegacyDurabilitySpec").msgclass
          MutationToken = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.MutationToken").msgclass
          GetRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.GetRequest").msgclass
          GetResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.GetResponse").msgclass
          GetAndTouchRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.GetAndTouchRequest").msgclass
          GetAndTouchResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.GetAndTouchResponse").msgclass
          GetAndLockRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.GetAndLockRequest").msgclass
          GetAndLockResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.GetAndLockResponse").msgclass
          UnlockRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.UnlockRequest").msgclass
          UnlockResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.UnlockResponse").msgclass
          TouchRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.TouchRequest").msgclass
          TouchResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.TouchResponse").msgclass
          ExistsRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.ExistsRequest").msgclass
          ExistsResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.ExistsResponse").msgclass
          InsertRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.InsertRequest").msgclass
          InsertResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.InsertResponse").msgclass
          UpsertRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.UpsertRequest").msgclass
          UpsertResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.UpsertResponse").msgclass
          ReplaceRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.ReplaceRequest").msgclass
          ReplaceResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.ReplaceResponse").msgclass
          RemoveRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.RemoveRequest").msgclass
          RemoveResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.RemoveResponse").msgclass
          IncrementRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.IncrementRequest").msgclass
          IncrementResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.IncrementResponse").msgclass
          DecrementRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.DecrementRequest").msgclass
          DecrementResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.DecrementResponse").msgclass
          AppendRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.AppendRequest").msgclass
          AppendResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.AppendResponse").msgclass
          PrependRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.PrependRequest").msgclass
          PrependResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.PrependResponse").msgclass
          LookupInRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.LookupInRequest").msgclass
          LookupInRequest::Spec = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.LookupInRequest.Spec").msgclass
          LookupInRequest::Spec::Flags = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.LookupInRequest.Spec.Flags").msgclass
          LookupInRequest::Spec::Operation = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.LookupInRequest.Spec.Operation").enummodule
          LookupInRequest::Flags = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.LookupInRequest.Flags").msgclass
          LookupInResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.LookupInResponse").msgclass
          LookupInResponse::Spec = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.LookupInResponse.Spec").msgclass
          MutateInRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.MutateInRequest").msgclass
          MutateInRequest::Spec = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.MutateInRequest.Spec").msgclass
          MutateInRequest::Spec::Flags = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.MutateInRequest.Spec.Flags").msgclass
          MutateInRequest::Spec::Operation = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.MutateInRequest.Spec.Operation").enummodule
          MutateInRequest::Flags = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.MutateInRequest.Flags").msgclass
          MutateInRequest::StoreSemantic = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.MutateInRequest.StoreSemantic").enummodule
          MutateInResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.MutateInResponse").msgclass
          MutateInResponse::Spec = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.MutateInResponse.Spec").msgclass
          GetAllReplicasRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.GetAllReplicasRequest").msgclass
          GetAllReplicasResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.GetAllReplicasResponse").msgclass
          DurabilityLevel = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.DurabilityLevel").enummodule
          CompressionEnabled = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.kv.v1.CompressionEnabled").enummodule
        end
      end
    end
  end
end
