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

require "couchbase/protostellar/generated/kv.v1_pb"

require "google/protobuf/well_known_types"

module Couchbase
  module Protostellar
    module RequestGenerator
      class KV
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

        def get_request(id, _options)
          proto_opts = {}

          Generated::KV::V1::GetRequest.new(
            key: id,
            **location,
            **proto_opts
          )
        end

        def get_and_touch_request(id, expiry, _options)
          proto_opts = {}
          expiry = get_expiry(expiry)

          Generated::KV::V1::GetAndTouchRequest.new(
            **location,
            key: id,
            expiry: expiry,
            **options.to_request,
            **proto_opts
          )
        end

        def upsert_request(id, content, options)
          encoded, = options.transcoder.encode(content)

          proto_opts = {
            content_type: get_content_type(options),
          }
          proto_opts[:durability_level] = get_durability_level(options) unless options.durability_level == :none
          proto_opts[:expiry] = get_expiry(options) unless options.preserve_expiry

          Generated::KV::V1::UpsertRequest.new(
            key: id,
            content: encoded,
            **location,
            **proto_opts
          )
        end

        def insert_request(id, content, options)
          encoded, = options.transcoder.encode(content)

          proto_opts = {
            content_type: get_content_type(options),
          }

          proto_opts[:durability_level] = get_durability_level(options) unless options.durability_level == :none
          proto_opts[:expiry] = get_expiry(options) unless options.expiry.nil?

          Generated::KV::V1::InsertRequest.new(
            key: id,
            content: encoded,
            **location,
            **proto_opts
          )
        end

        def replace_request(id, content, options)
          encoded, = options.transcoder.encode(content)

          proto_opts = {
            content_type: get_content_type(options),
          }

          proto_opts[:cas] = options.cas unless options.cas.nil?
          proto_opts[:durability_level] = get_durability_level(options) unless options.durability_level == :none
          proto_opts[:expiry] = get_expiry(options) unless options.preserve_expiry

          Generated::KV::V1::ReplaceRequest.new(
            key: id,
            content: encoded,
            **location,
            **proto_opts
          )
        end

        def remove_request(id, options)
          proto_opts = {}

          proto_opts[:cas] = options.cas unless options.cas.nil?
          proto_opts[:durability_level] = get_durability_level(options) unless options.durability_level == :none

          Generated::KV::V1::RemoveRequest.new(
            key: id,
            **location,
            **proto_opts
          )
        end

        def exists_request(id, _options)
          proto_opts = {}

          Generated::KV::V1::ExistsRequest.new(
            key: id,
            **proto_opts,
            **location
          )
        end

        def mutate_in_request(id, specs, options)
          proto_opts = {
            flags: get_mutate_in_flags(options),
            store_semantic: get_mutate_in_store_semantic(options),
            durability_level: get_durability_level(options),
          }
          proto_opts[:cas] = options.cas unless options.cas.nil?

          Generated::KV::V1::MutateInRequest.new(
            key: id,
            **location,
            specs: specs.map { |s| get_mutate_in_spec(s) },
            **proto_opts
          )
        end

        def lookup_in_request(id, specs, options)
          proto_opts = {
            flags: get_lookup_in_flags(options),
          }

          Generated::KV::V1::LookupInRequest.new(
            key: id,
            **location,
            specs: specs.map { |s| get_lookup_in_spec(s) },
            **proto_opts
          )
        end

        private

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
            :JSON
          else
            :UNKNOWN
          end
        end

        def get_durability_level(options)
          if options.durability_level == :none
            nil
          else
            options.durability_level.upcase
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
          case lookup_in_spec.type
          when :get_doc
            :GET
          else
            lookup_in_spec.type.upcase
          end
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
          case mutate_in_spec.type
          when :set_doc
            :REPLACE
          when :dict_add
            :INSERT
          when :remove_doc
            :REMOVE
          when :dict_upsert
            :UPSERT
          when :array_push_last
            :ARRAY_APPEND
          when :array_push_first
            :ARRAY_PREPEND
          else
            mutate_in_spec.type.upcase
          end
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
          options.store_semantics.upcase
        end
      end
    end
  end
end
