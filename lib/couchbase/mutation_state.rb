# frozen_string_literal: true

#  Copyright 2020-2021 Couchbase, Inc.
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
  class MutationToken
    # @return [Integer]
    attr_accessor :partition_id
    # @return [Integer]
    attr_accessor :partition_uuid
    # @return [Integer]
    attr_accessor :sequence_number
    # @return [String] name of the bucket
    attr_accessor :bucket_name

    # @yieldparam [MutationToken] self
    def initialize
      yield self if block_given?
    end
  end

  class MutationState
    # @return [Array<MutationToken>]
    attr_accessor :tokens

    # Create a mutation state from one or more MutationTokens
    #
    # @param [Array<MutationToken>] mutation_tokens the mutation tokens
    def initialize(*mutation_tokens)
      @tokens = []
      add(*mutation_tokens)
    end

    # Add one or more Mutation tokens to this state
    #
    # @param [Array<MutationToken>] mutation_tokens the mutation tokens
    def add(*mutation_tokens)
      @tokens |= mutation_tokens
    end

    # @api private
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
  end
end
