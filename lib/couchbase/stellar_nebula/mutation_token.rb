# frozen_string_literal: true

#  Copyright 2022-Present. Couchbase, Inc.
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

require_relative "generated/kv.v1_pb"

module Couchbase
  module StellarNebula
    class MutationToken
      attr_accessor :bucket_name
      attr_accessor :partition_id
      attr_accessor :partition_uuid
      attr_accessor :sequence_number

      def initialize(bucket_name: nil,
                     partition_id: nil,
                     partition_uuid: nil,
                     sequence_number: nil)
        @bucket_name = bucket_name unless bucket_name.nil?
        @partition_id = partition_id unless partition_id.nil?
        @partition_uuid = partition_uuid unless partition_uuid.nil?
        @sequence_number = sequence_number unless sequence_number.nil?

        yield self if block_given?
      end

      def to_proto
        Generated::KV::V1::MutationToken.new(
          bucket_name: @bucket_name,
          vbucket_id: @partition_id,
          vbucket_uuid: @partition_uuid,
          sequence_number: @sequence_number
        )
      end
    end
  end
end
