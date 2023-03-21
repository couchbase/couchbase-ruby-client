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
require "couchbase/protostellar/timeout_defaults"

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
        attr_reader :default_timeout

        def initialize(bucket_name, scope_name, collection_name, default_timeout = nil)
          @bucket_name = bucket_name
          @scope_name = scope_name
          @collection_name = collection_name

          # TODO: Use the KV timeout from the cluster's options
          @default_timeout = default_timeout.nil? ? TimeoutDefaults::KEY_VALUE : default_timeout
        end

        def location
          {
            bucket_name: @bucket_name,
            scope_name: @scope_name,
            collection_name: @collection_name,
          }
        end

        def get_request(id, options)
          proto_opts = {}

          proto_req = Generated::KV::V1::GetRequest.new(
            key: id,
            **location,
            **proto_opts
          )

          create_kv_request(proto_req, :get, options, true)
        end

        def get_and_touch_request(id, expiry, _options)
          proto_opts = {}
          expiry = get_expiry(expiry)

          proto_req = Generated::KV::V1::GetAndTouchRequest.new(
            **location,
            key: id,
            expiry_time: expiry,
            **proto_opts
          )

          create_kv_request(proto_req, :get_and_touch, options)
        end

        def upsert_request(id, content, options)
          encoded = get_encoded(content, options)

          proto_opts = {
            content_type: get_content_type(options),
          }
          proto_opts[:durability_level] = get_durability_level(options) unless options.durability_level == :none
          proto_opts[:expiry_time] = get_expiry(options) unless options.preserve_expiry  # TODO: Figure out expiry

          proto_req = Generated::KV::V1::UpsertRequest.new(
            key: id,
            content: encoded,
            **location,
            **proto_opts
          )

          create_kv_request(proto_req, :upsert, options)
        end

        def insert_request(id, content, options)
          encoded = get_encoded(content, options)

          proto_opts = {
            content_type: get_content_type(options),
          }

          proto_opts[:durability_level] = get_durability_level(options) unless options.durability_level == :none
          proto_opts[:expiry_time] = get_expiry(options) unless options.expiry[1].nil?

          proto_req = Generated::KV::V1::InsertRequest.new(
            key: id,
            content: encoded,
            **location,
            **proto_opts
          )

          create_kv_request(proto_req, :insert, options)
        end

        def replace_request(id, content, options)
          encoded = get_encoded(content, options)

          proto_opts = {
            content_type: get_content_type(options),
          }

          proto_opts[:cas] = options.cas unless options.cas.nil?
          proto_opts[:durability_level] = get_durability_level(options) unless options.durability_level == :none
          proto_opts[:expiry_time] = get_expiry(options) unless options.preserve_expiry

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

          create_kv_request(proto_req, :lookup_in, options, true)
        end

        def increment_request(id, options)
          proto_opts = {
            delta: options.delta,
          }
          proto_opts[:expiry_time] = get_expiry(options) unless options.expiry[1].nil?
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
          proto_opts[:expiry_time] = get_expiry(options) unless options.expiry[1].nil?
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

          proto_req = Generated::KV::V1::PrependRequest.new(
            key: id,
            content: content,
            **location,
            **proto_opts
          )

          create_kv_request(proto_req, :prepend, options)
        end

        private

        def create_kv_request(proto_request, rpc, options, idempotent = false)
          Request.new(
            service: :kv,
            rpc: rpc,
            proto_request: proto_request,
            timeout: get_timeout(options),
            idempotent: idempotent
          )
        end

        def get_expiry(options_or_expiry)
          if options_or_expiry.respond_to? :expiry
            type, time_or_duration = options_or_expiry.expiry
          else
            type, time_or_duration = Couchbase::Utils::Time.extract_expiry_time(options_or_expiry)
          end
          seconds =
            if time_or_duration.nil?
              0
            elsif type == :duration
              ::Time.now.tv_sec + time_or_duration
            else
              time_or_duration
            end
          Google::Protobuf::Timestamp.new({:seconds => seconds})
        end

        def get_content_type(options)
          if options.transcoder.instance_of?(Couchbase::JsonTranscoder)
            :DOCUMENT_CONTENT_TYPE_JSON
          elsif options.transcoder.nil?
            :DOCUMENT_CONTENT_TYPE_BINARY
          else
            :DOCUMENT_CONTENT_TYPE_UNKNOWN
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
          else
            encoded, = options.transcoder.encode(content)
          end
          encoded
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
