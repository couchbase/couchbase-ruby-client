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

require "couchbase/collection"
require "couchbase/options"
require "couchbase/errors"

module Couchbase
  module Datastructures
    # A {CouchbaseQueue} is implements FIFO queue with +Enumerable+ interface and backed by {Collection} document
    # (more specifically a JSON array).
    #
    # Note: sets are restricted to containing primitive types only due to server-side comparison restrictions.
    class CouchbaseQueue
      include Enumerable

      # Create a new List, backed by the document identified by +id+ in +collection+.
      #
      # @param [String] id the id of the document to back the queue.
      # @param [Collection] collection the collection through which to interact with the document.
      # @param [Options::CouchbaseList] options customization of the datastructure
      def initialize(id, collection, options = Options::CouchbaseQueue.new)
        @id = id
        @collection = collection
        @options = options
        @cas = 0
        @observability = @collection.instance_variable_get(:@observability)
      end

      # Calls the given block once for each element in the queue, passing that element as a parameter.
      #
      # @yieldparam [Object] item
      #
      # @return [CouchbaseQueue, Enumerable]
      def each(parent_span: nil, &)
        if block_given?
          current = @observability.record_operation(Observability::OP_QUEUE_EACH, parent_span, self) do |obs_handler|
            options = @options.get_options.clone
            options.parent_span = obs_handler.op_span
            result = @collection.get(@id, options)
            @cas = result.cas
            result.content
          rescue Error::DocumentNotFound
            @cas = 0
            []
          end
          current.each(&)
          self
        else
          enum_for(:each, parent_span: parent_span)
        end
      end

      # @return [Integer] returns the number of elements in the queue.
      def length(parent_span: nil)
        @observability.record_operation(Observability::OP_QUEUE_LENGTH, parent_span, self) do |obs_handler|
          options = @options.lookup_in_options.clone
          options.parent_span = obs_handler.op_span
          result = @collection.lookup_in(@id, [
                                           LookupInSpec.count(""),
                                         ], options)
          result.content(0)
        rescue Error::DocumentNotFound
          0
        end
      end

      alias size length

      # @return [Boolean] returns true if queue is empty
      def empty?(parent_span: nil)
        size(parent_span: parent_span).zero?
      end

      # Removes all elements from the queue
      def clear(parent_span: nil)
        @observability.record_operation(Observability::OP_QUEUE_CLEAR, parent_span, self) do |obs_handler|
          options = @options.remove_options.clone
          options.parent_span = obs_handler.op_span
          @collection.remove(@id, options)
          nil
        rescue Error::DocumentNotFound
          nil
        end
      end

      # Adds the given value to the queue
      #
      # @param [Object] obj
      # @return [CouchbaseQueue]
      def push(obj, parent_span: nil)
        @observability.record_operation(Observability::OP_QUEUE_PUSH, parent_span, self) do |obs_handler|
          options = @options.mutate_in_options.clone
          options.parent_span = obs_handler.op_span
          @collection.mutate_in(@id, [
                                  MutateInSpec.array_prepend("", [obj]),
                                ], options)
        rescue Error::PathExists
          # ignore
        end
        self
      end

      alias enq push
      alias << push

      # Retrieves object from the queue
      #
      # @return [Object, nil] queue entry or nil
      def pop(parent_span: nil)
        @observability.record_operation(Observability::OP_QUEUE_POP, parent_span, self) do |obs_handler|
          obj, cas = begin
            options = @options.lookup_in_options.clone
            options.parent_span = obs_handler.op_span
            result = @collection.lookup_in(@id, [
                                             LookupInSpec.get("[-1]"),
                                           ], @options.lookup_in_options)
            [
              result.exists?(0) ? result.content(0) : nil,
              result.cas,
            ]
          end
          begin
            options = Options::MutateIn.new(
              parent_span: obs_handler.op_span,
              cas: cas,
            )
            @collection.mutate_in(@id, [
                                    MutateInSpec.remove("[-1]"),
                                  ], options)
          end
          obj
        rescue Error::CasMismatch
          retry
        rescue Error::DocumentNotFound, Error::PathNotFound
          nil
        end
      end

      alias deq pop
      alias shift pop
    end

    # @api private
    CouchbaseQueueOptions = ::Couchbase::Options::CouchbaseQueue
  end
end
