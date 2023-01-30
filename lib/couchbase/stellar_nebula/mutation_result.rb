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

module Couchbase
  module StellarNebula
    class MutationResult
      attr_reader :cas
      attr_reader :mutation_token

      def initialize(resp)
        @cas = resp.cas
        proto_token = resp.mutation_token
        @mutation_token = MutationToken.new(
          bucket_name: proto_token.bucket_name,
          partition_id: proto_token.vbucket_id,
          partition_uuid: proto_token.vbucket_uuid,
          sequence_number: proto_token.seq_no
        )
      end

      def success?
        @cas != 0
      end
    end
  end
end
