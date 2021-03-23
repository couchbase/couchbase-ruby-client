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
    # A {CouchbaseMap} is implements +Enumerable+ interface and backed by {Collection} document (more specifically
    # a JSON array).
    #
    # Note that as such, a {CouchbaseMap} is restricted to the types that JSON array can contain.
    class CouchbaseMap
      include Enumerable

      # Create a new Map, backed by the document identified by +id+ in +collection+.
      #
      # @param [String] id the id of the document to back the map.
      # @param [Collection] collection the Couchbase collection through which to interact with the document.
      # @param [Options::CouchbaseMap] options customization of the datastructure
      def initialize(id, collection, options = Options::CouchbaseMap.new)
        @id = id
        @collection = collection
        @options = options
        @cas = 0
      end

      # Calls the given block once for each element in the map, passing that element as a parameter.
      #
      # @yieldparam [Object] item
      #
      # @return [CouchbaseMap, Enumerable]
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
          current.each do |key, value|
            yield key, value
          end
          self
        else
          enum_for(:each)
        end
      end

      # @return [Integer] returns the number of elements in the map.
      def length
        result = @collection.lookup_in(@id, [
                                         LookupInSpec.count(""),
                                       ], @options.lookup_in_options)
        result.content(0)
      rescue Error::DocumentNotFound
        0
      end

      alias size length

      # @return [Boolean] returns true if map is empty
      def empty?
        size.zero?
      end

      # Removes all elements from the map
      def clear
        @collection.remove(@id, @options.remove_options)
        nil
      rescue Error::DocumentNotFound
        nil
      end

      # Returns a value from the map for the given key.
      #
      # If the key cannot be found, there are several options:
      #
      # * with no other arguments, it will raise a KeyError exception
      # * if +default+ is given, then that will be returned
      # * if the optional code +block+ is specified, then that will be run and its result returned
      #
      # @overload fetch(key)
      #   Gets the value, associated with the key, or raise KeyError if key could not be found
      #   @param key [String] key
      #
      # @overload fetch(key, default)
      #   Gets the value, associated with the key, or return +default+ if key could not be found
      #   @param key [String] key
      #
      # @overload fetch(key, &block)
      #   Gets the value, associated with the key, or invoke specified +block+ and propagate its return value
      #   if key could not be found
      #   @param key [String] key
      #   @yieldreturn [Object] the default value to return in case the key could not be found
      #
      # @return [Object]
      def fetch(key, *rest)
        result = @collection.lookup_in(@id, [
                                         LookupInSpec.get(key),
                                       ], @options.lookup_in_options)
        result.content(0)
      rescue Error::DocumentNotFound, Error::PathNotFound
        return yield if block_given?
        return rest.first unless rest.empty?

        raise KeyError, "key not found: #{key}"
      end

      # Returns a value from the map for the given key.
      #
      # If the key cannot be found, +nil+ will be returned.
      #
      # @param [String] key
      #
      # @return [Object, nil] value, associated with the key or +nil+
      def [](key)
        fetch(key, nil)
      end

      # Associate the value given by +value+ with the key given by +key+.
      #
      # @param [String] key
      # @param [Object] value
      #
      # @return [void]
      def []=(key, value)
        @collection.mutate_in(@id, [
                                MutateInSpec.upsert(key, value),
                              ], @options.mutate_in_options)
      end

      # Deletes the key-value pair from the map.
      #
      # @param [String] key
      #
      # @return void
      def delete(key)
        @collection.mutate_in(@id, [
                                MutateInSpec.remove(key),
                              ])
      rescue Error::DocumentNotFound, Error::PathNotFound
        nil
      end

      # Returns +true+ if the given key is present
      #
      # @param [String] key
      # @return [Boolean]
      def key?(key)
        result = @collection.lookup_in(@id, [
                                         LookupInSpec.exists(key),
                                       ], @options.lookup_in_options)
        result.exists?(0)
      rescue Error::DocumentNotFound, Error::PathNotFound
        false
      end

      alias member? key?
      alias include? key?

      # Returns a new array populated with the keys from the map.
      #
      # @return [Array]
      def keys
        map { |key, _value| key }
      end

      # Returns a new array populated with the values from the map.
      #
      # @return [Array]
      def values
        map { |_key, value| value }
      end
    end

    # @api private
    CouchbaseMapOptions = ::Couchbase::Options::CouchbaseMap
  end
end
