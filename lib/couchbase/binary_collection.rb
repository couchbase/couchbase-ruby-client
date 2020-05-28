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

require "collection"

module Couchbase
  class BinaryCollection
    alias_method :inspect, :to_s

    # @param [Couchbase::Collection] collection parent collection
    def initialize(collection)
      @collection = collection
    end

    # Appends binary content to the document
    #
    # @param [String] id the document id which is used to uniquely identify it
    # @param [String] content the binary content to append to the document
    # @param [AppendOptions] options custom options to customize the request
    #
    # @return [Collection::MutationResult]
    def append(id, content, options = AppendOptions.new) end

    # Prepends binary content to the document
    #
    # @param [String] id the document id which is used to uniquely identify it
    # @param [String] content the binary content to prepend to the document
    # @param [PrependOptions] options custom options to customize the request
    #
    # @return [Collection::MutationResult]
    def prepend(id, content, options = PrependOptions.new) end

    # Increments the counter document by one of the number defined in the options
    #
    # @param [String] id the document id which is used to uniquely identify it
    # @param [IncrementOptions] options custom options to customize the request
    #
    # @return [CounterResult]
    def increment(id, options = IncrementOptions.new) end

    # Decrements the counter document by one of the number defined in the options
    #
    # @param [String] id the document id which is used to uniquely identify it
    # @param [IncrementOptions] options custom options to customize the request
    #
    # @return [CounterResult]
    def decrement(id, options = DecrementOptions.new) end

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

      # @return [Integer] if set, holds the CAS value for this operation
      attr_accessor :cas

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

      # @return [Integer] if set, holds the CAS value for this operation
      attr_accessor :cas

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
  end
end
