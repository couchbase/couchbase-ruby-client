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
          options.durability_level.upcase
        end
      end
    end
  end
end
