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

require "couchbase/collection"
require "couchbase/options"
require "couchbase/errors"

module Couchbase
  module Datastructures
    # A {CouchbaseSet} is implements +Enumerable+ interface and backed by {Collection} document (more specifically
    # a JSON array).
    #
    # Note: sets are restricted to containing primitive types only due to server-side comparison restrictions.
    class CouchbaseSet
      include Enumerable

      # Create a new List, backed by the document identified by +id+ in +collection+.
      #
      # @param [String] id the id of the document to back the set.
      # @param [Collection] collection the collection through which to interact with the document.
      # @param [Options::CouchbaseSet] options customization of the datastructure
      def initialize(id, collection, options = Options::CouchbaseSet.new)
        @id = id
        @collection = collection
        @options = options
        @cas = 0
      end

      # Calls the given block once for each element in the set, passing that element as a parameter.
      #
      # @yieldparam [Object] item
      #
      # @return [CouchbaseSet, Enumerable]
      def each
        if block_given?
          begin
            result = @collection.get(@id, @options.get_options)
            current = result.content
            @cas = result.cas
          rescue Error::DocumentNotFound
            current = []
            @cas = 0
          end
          current.each do |entry|
            yield entry
          end
          self
        else
          enum_for(:each)
        end
      end

      # @return [Integer] returns the number of elements in the set.
      def length
        result = @collection.lookup_in(@id, [
                                         LookupInSpec.count(""),
                                       ], @options.lookup_in_options)
        result.content(0)
      rescue Error::DocumentNotFound
        0
      end

      alias size length

      # @return [Boolean] returns true if set is empty
      def empty?
        size.zero?
      end

      # Adds the given value to the set
      #
      # @param [Object] obj
      # @return [CouchbaseSet]
      def add(obj)
        begin
          @collection.mutate_in(@id, [
                                  MutateInSpec.array_add_unique("", obj),
                                ], @options.mutate_in_options)
        rescue Error::PathExists
          # ignore
        end
        self
      end

      # Removes all elements from the set
      def clear
        @collection.remove(@id, @options.remove_options)
        nil
      rescue Error::DocumentNotFound
        nil
      end

      # Deletes the given object from the set.
      #
      # @return [Boolean] true if the value has been removed
      def delete(obj)
        result = @collection.get(@id)
        idx = result.content.index(obj)
        return false unless idx

        options = Collection::MutateInOptions.new
        options.cas = result.cas
        @collection.mutate_in(@id, [
                                MutateInSpec.remove("[#{idx}]"),
                              ], options)
        true
      rescue Error::CasMismatch
        retry
      rescue Error::DocumentNotFound
        false
      end
    end

    # @api private
    CouchbaseSetOptions = ::Couchbase::Options::CouchbaseSet
  end
end
