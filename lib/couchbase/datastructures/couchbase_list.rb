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
require "couchbase/errors"
require "couchbase/options"

module Couchbase
  module Datastructures
    # A {CouchbaseList} is implements +Enumerable+ interface and backed by {Collection} document (more specifically
    # a JSON array).
    #
    # Note that as such, a {CouchbaseList} is restricted to the types that JSON array can contain.
    class CouchbaseList
      include Enumerable

      # Create a new List, backed by the document identified by +id+ in +collection+.
      #
      # @param [String] id the id of the document to back the list.
      # @param [Collection] collection the Couchbase collection through which to interact with the document.
      # @param [Options::CouchbaseList] options customization of the datastructure
      def initialize(id, collection, options = Options::CouchbaseList.new)
        @id = id
        @collection = collection
        @options = options
        @cas = 0
        @observability = @collection.instance_variable_get(:@observability)
      end

      # Calls the given block once for each element in the list, passing that element as a parameter.
      #
      # @yieldparam [Object] item
      #
      # @return [CouchbaseList, Enumerable]
      def each(parent_span: nil, &)
        if block_given?
          current =
            @observability.record_operation(Observability::OP_LIST_EACH, parent_span, self) do |obs_handler|
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

      # @return [Integer] returns the number of elements in the list.
      def length(parent_span: nil)
        @observability.record_operation(Observability::OP_LIST_LENGTH, parent_span, self) do |obs_handler|
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

      # @return [Boolean] returns true if list is empty
      def empty?(parent_span: nil)
        size(parent_span: parent_span).zero?
      end

      # Appends the given object(s) on to the end of this error. This expression returns the array itself, so several
      # appends may be chained together.
      #
      # @param [Object...] obj object(s) to append
      # @return [CouchbaseList]
      def push(*obj, parent_span: nil)
        @observability.record_operation(Observability::OP_LIST_PUSH, parent_span, self) do |obs_handler|
          options = @options.mutate_in_options.clone
          options.parent_span = obs_handler.op_span
          @collection.mutate_in(@id, [
                                  MutateInSpec.array_append("", obj),
                                ], options)
        end
        self
      end

      alias append push

      # Prepends objects to the front of the list, moving other elements upwards
      #
      # @param [Object...] obj object(s) to prepend
      # @return [CouchbaseList]
      def unshift(*obj, parent_span: nil)
        @observability.record_operation(Observability::OP_LIST_UNSHIFT, parent_span, self) do |obs_handler|
          options = @options.mutate_in_options.clone
          options.parent_span = obs_handler.op_span
          @collection.mutate_in(@id, [
                                  MutateInSpec.array_prepend("", obj),
                                ], options)
        end
        self
      end

      alias prepend unshift

      # Inserts the given values before the element with the given +index+.
      #
      # @param [Integer] index
      # @param [Object...] obj object(s) to insert
      # @return [CouchbaseList]
      def insert(index, *obj, parent_span: nil)
        @observability.record_operation(Observability::OP_LIST_INSERT, parent_span, self) do |obs_handler|
          options = @options.mutate_in_options.clone
          options.parent_span = obs_handler.op_span
          @collection.mutate_in(@id, [
                                  MutateInSpec.array_insert("[#{index.to_i}]", obj),
                                ])
        end
        self
      end

      # Returns the element at +index+. A negative index counts from the end. Returns +nil+ if the index is out of range.
      #
      # @param [Integer] index
      # @return [Object, nil]
      def at(index, parent_span: nil)
        @observability.record_operation(Observability::OP_LIST_AT, parent_span, self) do |obs_handler|
          options = @options.mutate_in_options.clone
          options.parent_span = obs_handler.op_span
          result = @collection.lookup_in(@id, [
                                           LookupInSpec.get("[#{index.to_i}]"),
                                         ], options)
          result.exists?(0) ? result.content(0) : nil
        rescue Error::DocumentNotFound
          nil
        end
      end

      alias [] at

      # Deletes the element at the specified +index+, returning that element, or nil
      #
      # @param [Integer] index
      # @return [CouchbaseList]
      def delete_at(index, parent_span: nil)
        @observability.record_operation(Observability::OP_LIST_DELETE_AT, parent_span, self) do |obs_handler|
          options = Options::MutateIn.new(parent_span: obs_handler.op_span)
          @collection.mutate_in(@id, [
                                  MutateInSpec.remove("[#{index.to_i}]"),
                                ], options)
          self
        rescue Error::DocumentNotFound
          self
        end
      end

      # Removes all elements from the list
      def clear(parent_span: nil)
        @observability.record_operation(Observability::OP_LIST_CLEAR, parent_span, self) do |obs_handler|
          options = @options.remove_options.clone
          options.parent_span = obs_handler.op_span
          @collection.remove(@id, options)
          nil
        rescue Error::DocumentNotFound
          nil
        end
      end
    end

    # @api private
    CouchbaseListOptions = ::Couchbase::Options::CouchbaseList
  end
end
