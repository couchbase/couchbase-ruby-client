# frozen_string_literal: true

#  Copyright 2023-Present. Couchbase, Inc.
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

require "couchbase/json_transcoder"
require "couchbase/utils/time"
require "couchbase/errors"

require "couchbase/protostellar/generated/kv/v1/kv_pb"
require "couchbase/protostellar/request"

require "google/protobuf/well_known_types"

module Couchbase
  module Protostellar
    module RequestGenerator
      class KV
        DURABILITY_LEVEL_MAP = {
          :majority => :DURABILITY_LEVEL_MAJORITY,
          :majority_and_persist_to_active => :DURABILITY_LEVEL_MAJORITY_AND_PERSIST_TO_ACTIVE,
          :persist_to_majority => :DURABILITY_LEVEL_PERSIST_TO_MAJORITY,
        }.freeze

        LOOKUP_IN_OPERATION_TYPE_MAP = {
          :get => :OPERATION_GET,
          :get_doc => :OPERATION_GET,
          :exists => :OPERATION_EXISTS,
          :count => :OPERATION_COUNT,
        }.freeze

        MUTATE_IN_OPERATION_TYPE_MAP = {
          :set_doc => :OPERATION_REPLACE,
          :replace => :OPERATION_REPLACE,
          :dict_add => :OPERATION_INSERT,
          :remove_doc => :OPERATION_REMOVE,
          :remove => :OPERATION_REMOVE,
          :dict_upsert => :OPERATION_UPSERT,
          :array_push_last => :OPERATION_ARRAY_APPEND,
          :array_push_first => :OPERATION_ARRAY_PREPEND,
          :array_insert => :OPERATION_ARRAY_INSERT,
          :array_add_unique => :OPERATION_ARRAY_ADD_UNIQUE,
          :counter => :OPERATION_COUNTER,
        }.freeze

        MUTATE_IN_STORE_SEMANTIC_MAP = {
          :replace => :STORE_SEMANTIC_REPLACE,
          :upsert => :STORE_SEMANTIC_UPSERT,
          :insert => :STORE_SEMANTIC_INSERT,
        }.freeze

        attr_reader :bucket_name
        attr_reader :scope_name
        attr_reader :collection_name

        def initialize(bucket_name, scope_name, collection_name)
          @bucket_name = bucket_name
          @scope_name = scope_name
          @collection_name = collection_name
        end

        def location
          {
            bucket_name: @bucket_name,
            scope_name: @scope_name,
            collection_name: @collection_name,
          }
        end

        def get_request(id, options)
          proto_req = Generated::KV::V1::GetRequest.new(
            key: id,
            **location
          )

          create_kv_request(proto_req, :get, options, idempotent: true)
        end

        def get_and_touch_request(id, expiry, _options)
          expiry_type, expiry_value = get_expiry(expiry)

          raise ArgumentError, "Expiry cannot be nil" if expiry_value.nil?

          proto_req = Generated::KV::V1::GetAndTouchRequest.new(
            **{expiry_type => expiry_value},
            **location,
            key: id
          )

          create_kv_request(proto_req, :get_and_touch, options)
        end

        def get_and_lock_request(id, lock_time, options)
          proto_req = Generated::KV::V1::GetAndLockRequest.new(
            **location,
            key: id,
            lock_time: lock_time.respond_to?(:in_seconds) ? lock_time.public_send(:in_seconds) : lock_time
          )

          create_kv_request(proto_req, :get_and_lock, options)
        end

        def unlock_request(id, cas, options)
          proto_req = Generated::KV::V1::UnlockRequest.new(
            **location,
            key: id,
            cas: cas
          )

          create_kv_request(proto_req, :unlock, options)
        end

        def touch_request(id, expiry, options)
          expiry_type, expiry_value = get_expiry(expiry)

          raise ArgumentError, "Expiry cannot be nil" if expiry_value.nil?

          proto_req = Generated::KV::V1::TouchRequest.new(
            **{expiry_type => expiry_value},
            **location,
            key: id
          )

          create_kv_request(proto_req, :touch, options)
        end

        def upsert_request(id, content, options)
          encoded, flag = get_encoded(content, options)

          proto_opts = {
            content_flags: flag,
          }
          proto_opts[:durability_level] = get_durability_level(options) unless options.durability_level == :none
          unless options.preserve_expiry
            expiry_type, expiry_value = get_expiry(options)
            proto_opts[expiry_type] = expiry_value unless expiry_value.nil?
          end

          proto_req = Generated::KV::V1::UpsertRequest.new(
            key: id,
            content: encoded,
            **location,
            **proto_opts
          )

          create_kv_request(proto_req, :upsert, options)
        end

        def insert_request(id, content, options)
          encoded, flag = get_encoded(content, options)

          proto_opts = {
            content_flags: flag,
          }

          proto_opts[:durability_level] = get_durability_level(options) unless options.durability_level == :none
          expiry_type, expiry_value = get_expiry(options)
          proto_opts[expiry_type] = expiry_value unless expiry_value.nil?

          proto_req = Generated::KV::V1::InsertRequest.new(
            key: id,
            content: encoded,
            **location,
            **proto_opts
          )

          create_kv_request(proto_req, :insert, options)
        end

        def replace_request(id, content, options)
          encoded, flag = get_encoded(content, options)

          proto_opts = {
            content_flags: flag,
          }

          proto_opts[:cas] = options.cas unless options.cas.nil?
          proto_opts[:durability_level] = get_durability_level(options) unless options.durability_level == :none
          unless options.preserve_expiry
            expiry_type, expiry_value = get_expiry(options)
            proto_opts[expiry_type] = expiry_value unless expiry_value.nil?
          end
          proto_req = Generated::KV::V1::ReplaceRequest.new(
            key: id,
            content: encoded,
            **location,
            **proto_opts
          )

          create_kv_request(proto_req, :replace, options)
        end

        def remove_request(id, options)
          proto_opts = {}

          proto_opts[:cas] = options.cas unless options.cas.nil?
          proto_opts[:durability_level] = get_durability_level(options) unless options.durability_level == :none

          proto_req = Generated::KV::V1::RemoveRequest.new(
            key: id,
            **location,
            **proto_opts
          )

          create_kv_request(proto_req, :remove, options)
        end

        def exists_request(id, options)
          proto_opts = {}

          proto_req = Generated::KV::V1::ExistsRequest.new(
            key: id,
            **proto_opts,
            **location
          )

          create_kv_request(proto_req, :exists, options)
        end

        def mutate_in_request(id, specs, options)
          proto_opts = {
            flags: get_mutate_in_flags(options),
            store_semantic: get_mutate_in_store_semantic(options),
            durability_level: get_durability_level(options),
          }
          proto_opts[:cas] = options.cas unless options.cas.nil?

          proto_req = Generated::KV::V1::MutateInRequest.new(
            key: id,
            **location,
            specs: specs.map { |s| get_mutate_in_spec(s) },
            **proto_opts
          )

          create_kv_request(proto_req, :mutate_in, options)
        end

        def lookup_in_request(id, specs, options)
          proto_opts = {
            flags: get_lookup_in_flags(options),
          }

          proto_req = Generated::KV::V1::LookupInRequest.new(
            key: id,
            **location,
            specs: specs.map { |s| get_lookup_in_spec(s) },
            **proto_opts
          )

          puts proto_req

          create_kv_request(proto_req, :lookup_in, options, idempotent: true)
        end

        def increment_request(id, options)
          proto_opts = {
            delta: options.delta,
          }
          expiry_type, expiry_value = get_expiry(options)
          proto_opts[expiry_type] = expiry_value unless expiry_value.nil?
          proto_opts[:initial] = options.initial unless options.initial.nil?

          proto_req = Generated::KV::V1::IncrementRequest.new(
            key: id,
            **location,
            **proto_opts
          )

          create_kv_request(proto_req, :increment, options)
        end

        def decrement_request(id, options)
          proto_opts = {
            delta: options.delta,
          }
          expiry_type, expiry_value = get_expiry(options)
          proto_opts[expiry_type] = expiry_value unless expiry_value.nil?
          proto_opts[:initial] = options.initial unless options.initial.nil?

          proto_req = Generated::KV::V1::DecrementRequest.new(
            key: id,
            **location,
            **proto_opts
          )

          create_kv_request(proto_req, :decrement, options)
        end

        def append_request(id, content, options)
          proto_opts = {}
          proto_opts[:cas] = options.cas unless options.cas.nil?
          proto_opts[:durability_level] = get_durability_level(options) unless options.durability_level == :none

          proto_req = Generated::KV::V1::AppendRequest.new(
            key: id,
            content: content,
            **location,
            **proto_opts
          )

          create_kv_request(proto_req, :append, options)
        end

        def prepend_request(id, content, options)
          proto_opts = {}
          proto_opts[:cas] = options.cas unless options.cas.nil?
          proto_opts[:durability_level] = get_durability_level(options) unless options.durability_level == :none

          proto_req = Generated::KV::V1::PrependRequest.new(
            key: id,
            content: content,
            **location,
            **proto_opts
          )

          create_kv_request(proto_req, :prepend, options)
        end

        private

        def create_kv_request(proto_request, rpc, options, idempotent: false)
          Request.new(
            service: :kv,
            rpc: rpc,
            proto_request: proto_request,
            timeout: options.timeout,
            idempotent: idempotent
          )
        end

        def get_expiry(options_or_expiry)
          if options_or_expiry.respond_to? :expiry
            type, time_or_duration = options_or_expiry.expiry
          else
            type, time_or_duration = Couchbase::Utils::Time.extract_expiry_time(options_or_expiry)
          end

          return [nil, nil] if time_or_duration.nil?

          case type
          when :duration
            [:expiry_secs, time_or_duration]
          when :time_point
            timestamp = Google::Protobuf::Timestamp.new(
              seconds: time_or_duration.tv_sec,
              nanos: time_or_duration.tv_nsec
            )
            [:expiry_time, timestamp]
          else
            raise Protostellar::Error::InvalidExpiryType
          end
        end

        def get_durability_level(options)
          if options.durability_level == :none
            nil
          else
            DURABILITY_LEVEL_MAP[options.durability_level]
          end
        end

        def get_lookup_in_spec(lookup_in_spec)
          Generated::KV::V1::LookupInRequest::Spec.new(
            operation: get_lookup_in_operation(lookup_in_spec),
            path: lookup_in_spec.path,
            flags: get_lookup_in_spec_flags(lookup_in_spec)
          )
        end

        def get_lookup_in_operation(lookup_in_spec)
          LOOKUP_IN_OPERATION_TYPE_MAP[lookup_in_spec.type]
        end

        def get_lookup_in_spec_flags(lookup_in_spec)
          Generated::KV::V1::LookupInRequest::Spec::Flags.new(
            xattr: lookup_in_spec.xattr?
          )
        end

        def get_lookup_in_flags(options)
          Generated::KV::V1::LookupInRequest::Flags.new(
            access_deleted: options.access_deleted
          )
        end

        def get_mutate_in_spec(mutate_in_spec)
          Generated::KV::V1::MutateInRequest::Spec.new(
            operation: get_mutate_in_operation(mutate_in_spec),
            path: mutate_in_spec.path,
            content: mutate_in_spec.param.to_s,
            flags: get_mutate_in_spec_flags(mutate_in_spec)
          )
        end

        def get_mutate_in_operation(mutate_in_spec)
          MUTATE_IN_OPERATION_TYPE_MAP[mutate_in_spec.type]
        end

        def get_mutate_in_spec_flags(mutate_in_spec)
          Generated::KV::V1::MutateInRequest::Spec::Flags.new(
            create_path: mutate_in_spec.create_path?,
            xattr: mutate_in_spec.xattr?
          )
        end

        def get_mutate_in_flags(options)
          Generated::KV::V1::MutateInRequest::Flags.new(
            access_deleted: options.access_deleted
          )
        end

        def get_mutate_in_store_semantic(options)
          MUTATE_IN_STORE_SEMANTIC_MAP[options.store_semantics]
        end

        def get_encoded(content, options)
          if options.transcoder.nil?
            encoded = content.to_s
            flag = 0
          else
            encoded, flag = options.transcoder.encode(content)
          end
          [encoded, flag]
        end
      end
    end
  end
end
