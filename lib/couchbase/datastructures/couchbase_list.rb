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
      end

      # Calls the given block once for each element in the list, passing that element as a parameter.
      #
      # @yieldparam [Object] item
      #
      # @return [CouchbaseList, Enumerable]
      def each(&)
        if block_given?
          begin
            result = @collection.get(@id, @options.get_options)
            current = result.content
            @cas = result.cas
          rescue Error::DocumentNotFound
            current = []
            @cas = 0
          end
          current.each(&)
          self
        else
          enum_for(:each)
        end
      end

      # @return [Integer] returns the number of elements in the list.
      def length
        result = @collection.lookup_in(@id, [
                                         LookupInSpec.count(""),
                                       ], @options.lookup_in_options)
        result.content(0)
      rescue Error::DocumentNotFound
        0
      end

      alias size length

      # @return [Boolean] returns true if list is empty
      def empty?
        size.zero?
      end

      # Appends the given object(s) on to the end of this error. This expression returns the array itself, so several
      # appends may be chained together.
      #
      # @param [Object...] obj object(s) to append
      # @return [CouchbaseList]
      def push(*obj)
        @collection.mutate_in(@id, [
                                MutateInSpec.array_append("", obj),
                              ], @options.mutate_in_options)
        self
      end

      alias append push

      # Prepends objects to the front of the list, moving other elements upwards
      #
      # @param [Object...] obj object(s) to prepend
      # @return [CouchbaseList]
      def unshift(*obj)
        @collection.mutate_in(@id, [
                                MutateInSpec.array_prepend("", obj),
                              ], @options.mutate_in_options)
        self
      end

      alias prepend unshift

      # Inserts the given values before the element with the given +index+.
      #
      # @param [Integer] index
      # @param [Object...] obj object(s) to insert
      # @return [CouchbaseList]
      def insert(index, *obj)
        @collection.mutate_in(@id, [
                                MutateInSpec.array_insert("[#{index.to_i}]", obj),
                              ])
        self
      end

      # Returns the element at +index+. A negative index counts from the end. Returns +nil+ if the index is out of range.
      #
      # @param [Integer] index
      # @return [Object, nil]
      def at(index)
        result = @collection.lookup_in(@id, [
                                         LookupInSpec.get("[#{index.to_i}]"),
                                       ], @options.lookup_in_options)
        result.exists?(0) ? result.content(0) : nil
      rescue Error::DocumentNotFound
        nil
      end

      alias [] at

      # Deletes the element at the specified +index+, returning that element, or nil
      #
      # @param [Integer] index
      # @return [CouchbaseList]
      def delete_at(index)
        @collection.mutate_in(@id, [
                                MutateInSpec.remove("[#{index.to_i}]"),
                              ])
        self
      rescue Error::DocumentNotFound
        self
      end

      # Removes all elements from the list
      def clear
        @collection.remove(@id, @options.remove_options)
        nil
      rescue Error::DocumentNotFound
        nil
      end
    end

    # @api private
    CouchbaseListOptions = ::Couchbase::Options::CouchbaseList
  end
end
