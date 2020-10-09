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

require "couchbase/options"
require "couchbase/binary_collection_options"

module Couchbase
  class BinaryCollection
    alias inspect to_s

    # @param [Couchbase::Collection] collection parent collection
    def initialize(collection)
      @collection = collection
      @backend = collection.instance_variable_get("@backend")
    end

    # Appends binary content to the document
    #
    # @param [String] id the document id which is used to uniquely identify it
    # @param [String] content the binary content to append to the document
    # @param [Options::Append] options custom options to customize the request
    #
    # @return [Collection::MutationResult]
    def append(id, content, options = Options::Append.new) end

    # Prepends binary content to the document
    #
    # @param [String] id the document id which is used to uniquely identify it
    # @param [String] content the binary content to prepend to the document
    # @param [Options::Prepend] options custom options to customize the request
    #
    # @return [Collection::MutationResult]
    def prepend(id, content, options = Options::Prepend.new) end

    # Increments the counter document by one of the number defined in the options
    #
    # @param [String] id the document id which is used to uniquely identify it
    # @param [Options::Increment] options custom options to customize the request
    #
    # @example Increment value by 10, and initialize to 0 if it does not exist
    #   res = collection.binary.increment("raw_counter", Options::Increment(delta: 10, initial: 0))
    #   res.content #=> 0
    #   res = collection.binary.increment("raw_counter", Options::Increment(delta: 10, initial: 0))
    #   res.content #=> 10
    #
    # @return [CounterResult]
    def increment(id, options = Options::Increment.new)
      resp = @backend.document_increment(@collection.bucket_name, "#{@collection.scope_name}.#{@collection.name}", id,
                                         options.to_backend)
      CounterResult.new do |res|
        res.cas = resp[:cas]
        res.content = resp[:content]
        res.mutation_token = @collection.send(:extract_mutation_token, resp)
      end
    end

    # Decrements the counter document by one of the number defined in the options
    #
    # @param [String] id the document id which is used to uniquely identify it
    # @param [Options::Decrement] options custom options to customize the request
    #
    # @example Decrement value by 2, and initialize to 100 if it does not exist
    #   res = collection.binary.decrement("raw_counter", Options::Decrement(delta: 2, initial: 100))
    #   res.value #=> 100
    #   res = collection.binary.decrement("raw_counter", Options::Decrement(delta: 2, initial: 100))
    #   res.value #=> 98
    #
    # @return [CounterResult]
    def decrement(id, options = Options::Decrement.new)
      resp = @backend.document_decrement(@collection.bucket_name, "#{@collection.scope_name}.#{@collection.name}", id,
                                         options.to_backend)
      CounterResult.new do |res|
        res.cas = resp[:cas]
        res.content = resp[:content]
        res.mutation_token = @collection.send(:extract_mutation_token, resp)
      end
    end

    # @api private
    # TODO: deprecate in 3.1
    AppendOptions = ::Couchbase::Options::Append
    # @api private
    # TODO: deprecate in 3.1
    PrependOptions = ::Couchbase::Options::Prepend
    # @api private
    # TODO: deprecate in 3.1
    IncrementOptions = ::Couchbase::Options::Increment
    # @api private
    # TODO: deprecate in 3.1
    DecrementOptions = ::Couchbase::Options::Decrement
  end
end
