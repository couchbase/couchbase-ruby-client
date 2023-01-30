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
    class MutationState
      attr_accessor :tokens

      def initialize(*mutation_tokens)
        @tokens = []
        add(*mutation_tokens)
      end

      def add(*mutation_tokens)
        @tokens |= mutation_tokens
      end

      def to_a
        @tokens.map do |t|
          {
            bucket_name: t.bucket_name,
            partition_id: t.partition_id,
            partition_uuid: t.partition_uuid,
            sequence_number: t.sequence_number,
          }
        end
      end

      def to_proto
        @tokens.map(&:to_proto)
      end
    end
  end
end
