# frozen_string_literal: true
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: couchbase/internal/hooks/v1/hooks.proto

require 'google/protobuf'

require 'google/protobuf/any_pb'


descriptor_data = "\n\'couchbase/internal/hooks/v1/hooks.proto\x12\x1b\x63ouchbase.internal.hooks.v1\x1a\x19google/protobuf/any.proto\"\'\n\x19\x43reateHooksContextRequest\x12\n\n\x02id\x18\x01 \x01(\t\"\x1c\n\x1a\x43reateHooksContextResponse\"(\n\x1a\x44\x65stroyHooksContextRequest\x12\n\n\x02id\x18\x01 \x01(\t\"\x1d\n\x1b\x44\x65stroyHooksContextResponse\"[\n\x08ValueRef\x12\x17\n\rrequest_field\x18\x01 \x01(\tH\x00\x12\x17\n\rcounter_value\x18\x02 \x01(\tH\x00\x12\x14\n\njson_value\x18\x03 \x01(\x0cH\x00\x42\x07\n\x05value\"\xb7\x01\n\rHookCondition\x12\x33\n\x04left\x18\x01 \x01(\x0b\x32%.couchbase.internal.hooks.v1.ValueRef\x12;\n\x02op\x18\x02 \x01(\x0e\x32/.couchbase.internal.hooks.v1.ComparisonOperator\x12\x34\n\x05right\x18\x03 \x01(\x0b\x32%.couchbase.internal.hooks.v1.ValueRef\"\xf2\x07\n\nHookAction\x12\x38\n\x02if\x18\x01 \x01(\x0b\x32*.couchbase.internal.hooks.v1.HookAction.IfH\x00\x12\x42\n\x07\x63ounter\x18\x02 \x01(\x0b\x32/.couchbase.internal.hooks.v1.HookAction.CounterH\x00\x12P\n\x0fwait_on_barrier\x18\x03 \x01(\x0b\x32\x35.couchbase.internal.hooks.v1.HookAction.WaitOnBarrierH\x00\x12O\n\x0esignal_barrier\x18\x04 \x01(\x0b\x32\x35.couchbase.internal.hooks.v1.HookAction.SignalBarrierH\x00\x12Q\n\x0freturn_response\x18\x05 \x01(\x0b\x32\x36.couchbase.internal.hooks.v1.HookAction.ReturnResponseH\x00\x12K\n\x0creturn_error\x18\x06 \x01(\x0b\x32\x33.couchbase.internal.hooks.v1.HookAction.ReturnErrorH\x00\x12\x42\n\x07\x65xecute\x18\x07 \x01(\x0b\x32/.couchbase.internal.hooks.v1.HookAction.ExecuteH\x00\x1a\xb1\x01\n\x02If\x12\x38\n\x04\x63ond\x18\x01 \x03(\x0b\x32*.couchbase.internal.hooks.v1.HookCondition\x12\x36\n\x05match\x18\x02 \x03(\x0b\x32\'.couchbase.internal.hooks.v1.HookAction\x12\x39\n\x08no_match\x18\x03 \x03(\x0b\x32\'.couchbase.internal.hooks.v1.HookAction\x1a,\n\x07\x43ounter\x12\x12\n\ncounter_id\x18\x01 \x01(\t\x12\r\n\x05\x64\x65lta\x18\x02 \x01(\x03\x1a#\n\rWaitOnBarrier\x12\x12\n\nbarrier_id\x18\x01 \x01(\t\x1a\x37\n\rSignalBarrier\x12\x12\n\nbarrier_id\x18\x01 \x01(\t\x12\x12\n\nsignal_all\x18\x02 \x01(\x08\x1a\x35\n\x0eReturnResponse\x12#\n\x05value\x18\x01 \x01(\x0b\x32\x14.google.protobuf.Any\x1aS\n\x0bReturnError\x12\x0c\n\x04\x63ode\x18\x01 \x01(\x05\x12\x0f\n\x07message\x18\x02 \x01(\t\x12%\n\x07\x64\x65tails\x18\x03 \x03(\x0b\x32\x14.google.protobuf.Any\x1a\t\n\x07\x45xecuteB\x08\n\x06\x61\x63tion\"z\n\x04Hook\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x13\n\x0b\x64\x65scription\x18\x02 \x01(\t\x12\x15\n\rtarget_method\x18\x03 \x01(\t\x12\x38\n\x07\x61\x63tions\x18\x04 \x03(\x0b\x32\'.couchbase.internal.hooks.v1.HookAction\"]\n\x0f\x41\x64\x64HooksRequest\x12\x18\n\x10hooks_context_id\x18\x01 \x01(\t\x12\x30\n\x05hooks\x18\x02 \x03(\x0b\x32!.couchbase.internal.hooks.v1.Hook\"\x12\n\x10\x41\x64\x64HooksResponse\"C\n\x13WatchBarrierRequest\x12\x18\n\x10hooks_context_id\x18\x01 \x01(\t\x12\x12\n\nbarrier_id\x18\x02 \x01(\t\"\'\n\x14WatchBarrierResponse\x12\x0f\n\x07wait_id\x18\x02 \x01(\t\"f\n\x14SignalBarrierRequest\x12\x18\n\x10hooks_context_id\x18\x01 \x01(\t\x12\x12\n\nbarrier_id\x18\x02 \x01(\t\x12\x14\n\x07wait_id\x18\x03 \x01(\tH\x00\x88\x01\x01\x42\n\n\x08_wait_id\"\x17\n\x15SignalBarrierResponse*\xfa\x01\n\x12\x43omparisonOperator\x12\x1d\n\x19\x43OMPARISON_OPERATOR_EQUAL\x10\x00\x12!\n\x1d\x43OMPARISON_OPERATOR_NOT_EQUAL\x10\x01\x12$\n COMPARISON_OPERATOR_GREATER_THAN\x10\x02\x12-\n)COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL\x10\x03\x12!\n\x1d\x43OMPARISON_OPERATOR_LESS_THAN\x10\x04\x12*\n&COMPARISON_OPERATOR_LESS_THAN_OR_EQUAL\x10\x05\x32\x83\x05\n\x0cHooksService\x12\x87\x01\n\x12\x43reateHooksContext\x12\x36.couchbase.internal.hooks.v1.CreateHooksContextRequest\x1a\x37.couchbase.internal.hooks.v1.CreateHooksContextResponse\"\x00\x12\x8a\x01\n\x13\x44\x65stroyHooksContext\x12\x37.couchbase.internal.hooks.v1.DestroyHooksContextRequest\x1a\x38.couchbase.internal.hooks.v1.DestroyHooksContextResponse\"\x00\x12i\n\x08\x41\x64\x64Hooks\x12,.couchbase.internal.hooks.v1.AddHooksRequest\x1a-.couchbase.internal.hooks.v1.AddHooksResponse\"\x00\x12w\n\x0cWatchBarrier\x12\x30.couchbase.internal.hooks.v1.WatchBarrierRequest\x1a\x31.couchbase.internal.hooks.v1.WatchBarrierResponse\"\x00\x30\x01\x12x\n\rSignalBarrier\x12\x31.couchbase.internal.hooks.v1.SignalBarrierRequest\x1a\x32.couchbase.internal.hooks.v1.SignalBarrierResponse\"\x00\x42\xa3\x02\n3com.couchbase.client.protostellar.internal.hooks.v1P\x01ZPgithub.com/couchbase/goprotostellar/genproto/internal_hooks_v1;internal_hooks_v1\xaa\x02(Couchbase.Protostellar.Internal.Hooks.V1\xca\x02\x32\x43ouchbase\\Protostellar\\Generated\\Internal\\Hooks\\V1\xea\x02\x37\x43ouchbase::Protostellar::Generated::Internal::Hooks::V1b\x06proto3"

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
    ["google.protobuf.Any", "google/protobuf/any.proto"],
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
      module Internal
        module Hooks
          module V1
            CreateHooksContextRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.internal.hooks.v1.CreateHooksContextRequest").msgclass
            CreateHooksContextResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.internal.hooks.v1.CreateHooksContextResponse").msgclass
            DestroyHooksContextRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.internal.hooks.v1.DestroyHooksContextRequest").msgclass
            DestroyHooksContextResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.internal.hooks.v1.DestroyHooksContextResponse").msgclass
            ValueRef = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.internal.hooks.v1.ValueRef").msgclass
            HookCondition = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.internal.hooks.v1.HookCondition").msgclass
            HookAction = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.internal.hooks.v1.HookAction").msgclass
            HookAction::If = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.internal.hooks.v1.HookAction.If").msgclass
            HookAction::Counter = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.internal.hooks.v1.HookAction.Counter").msgclass
            HookAction::WaitOnBarrier = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.internal.hooks.v1.HookAction.WaitOnBarrier").msgclass
            HookAction::SignalBarrier = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.internal.hooks.v1.HookAction.SignalBarrier").msgclass
            HookAction::ReturnResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.internal.hooks.v1.HookAction.ReturnResponse").msgclass
            HookAction::ReturnError = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.internal.hooks.v1.HookAction.ReturnError").msgclass
            HookAction::Execute = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.internal.hooks.v1.HookAction.Execute").msgclass
            Hook = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.internal.hooks.v1.Hook").msgclass
            AddHooksRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.internal.hooks.v1.AddHooksRequest").msgclass
            AddHooksResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.internal.hooks.v1.AddHooksResponse").msgclass
            WatchBarrierRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.internal.hooks.v1.WatchBarrierRequest").msgclass
            WatchBarrierResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.internal.hooks.v1.WatchBarrierResponse").msgclass
            SignalBarrierRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.internal.hooks.v1.SignalBarrierRequest").msgclass
            SignalBarrierResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.internal.hooks.v1.SignalBarrierResponse").msgclass
            ComparisonOperator = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("couchbase.internal.hooks.v1.ComparisonOperator").enummodule
          end
        end
      end
    end
  end
end
