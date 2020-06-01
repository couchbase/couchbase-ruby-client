#    Copyright 2020 Couchbase, Inc.
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

require "couchbase/collection_options"

module Couchbase
  class BinaryCollection
    class AppendOptions < CommonOptions
      # @return [Integer] The default CAS used (0 means no CAS in this context)
      attr_accessor :cas

      def initialize
        yield self if block_given?
      end
    end

    class PrependOptions < CommonOptions
      # @return [Integer] The default CAS used (0 means no CAS in this context)
      attr_accessor :cas

      def initialize
        yield self if block_given?
      end
    end

    class IncrementOptions < CommonOptions
      # @return [Integer] the delta for the operation
      attr_reader :delta

      # @return [Integer] if present, holds the initial value
      attr_accessor :initial

      # @return [Integer] if set, holds the expiration for the operation
      attr_accessor :expiration

      # @return [:none, :majority, :majority_and_persist_to_active, :persist_to_majority] level of durability
      attr_accessor :durability_level

      def initialize
        @delta = 1
        yield self if block_given?
      end

      def delta=(value)
        if delta < 0
          raise ArgumentError, "the delta cannot be less than 0"
        end
        @delta = value
      end
    end

    class DecrementOptions < CommonOptions
      # @return [Integer] the delta for the operation
      attr_reader :delta

      # @return [Integer] if present, holds the initial value
      attr_accessor :initial

      # @return [Integer] if set, holds the expiration for the operation
      attr_accessor :expiration

      # @return [:none, :majority, :majority_and_persist_to_active, :persist_to_majority] level of durability
      attr_accessor :durability_level

      def initialize
        @delta = 1
        yield self if block_given?
      end

      def delta=(value)
        if delta < 0
          raise ArgumentError, "the delta cannot be less than 0"
        end
        @delta = value
      end
    end

    class CounterResult < ::Couchbase::Collection::MutationResult
      # @return [Integer] current value of the counter
      attr_accessor :content
    end
  end
end
