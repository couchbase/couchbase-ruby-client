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
    def increment(id, options = IncrementOptions.new)
      resp = @backend.document_increment(@collection.bucket_name, "#{@collection.scope_name}.#{@collection.name}", id,
                                         options.timeout,
                                         {
                                           delta: options.delta,
                                           initial_value: options.initial,
                                           expiry: options.expiry,
                                           durability_level: options.durability_level,
                                         })
      CounterResult.new do |res|
        res.cas = resp[:cas]
        res.content = resp[:content]
        res.mutation_token = @collection.send(:extract_mutation_token, resp)
      end
    end

    # Decrements the counter document by one of the number defined in the options
    #
    # @param [String] id the document id which is used to uniquely identify it
    # @param [DecrementOptions] options custom options to customize the request
    #
    # @return [CounterResult]
    def decrement(id, options = DecrementOptions.new)
      resp = @backend.document_decrement(@collection.bucket_name, "#{@collection.scope_name}.#{@collection.name}", id,
                                         options.timeout,
                                         {
                                           delta: options.delta,
                                           initial_value: options.initial,
                                           expiry: options.expiry,
                                           durability_level: options.durability_level,
                                         })
      CounterResult.new do |res|
        res.cas = resp[:cas]
        res.content = resp[:content]
        res.mutation_token = @collection.send(:extract_mutation_token, resp)
      end
    end
  end
end
