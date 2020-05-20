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

module Couchbase
  class LookupInSpec
    # Fetches the content from a field (if present) at the given path
    #
    # @param [String] path the path identifying where to get the value
    # @return [LookupInSpec]
    def self.get(path)
      new(path.empty? ? :get_doc : :get, path)
    end

    # Checks if a value at the given path exists in the document
    #
    # @param [String] path the path to check if the field exists
    # @return [LookupInSpec]
    def self.exists(path)
      new(:exists, path)
    end

    # Counts the number of values at a given path in teh document
    #
    # @param [String] path the path identifying where to count the values
    # @return [LookupInSpec]
    def self.count(path)
      new(:count, path)
    end

    def xattr
      @xattr = true
      self
    end

    def xattr?
      @xattr
    end

    attr_reader :type
    attr_reader :path

    private

    # @param [:get_doc, :get, :exists, :count] type of the lookup
    # @param [String] path
    def initialize(type, path)
      @type = type
      @path = path
    end
  end

  class MutateInSpec
    # Creates a command with the intention of replacing an existing value in a JSON document.
    #
    # If the path is empty (""), then the value will be used for the document's full body. Will
    # error if the last element of the path does not exist.
    #
    # @param [String] path the path identifying where to replace the value.
    # @param [Object] value the value to replace with.
    #
    # @return [MutateInSpec]
    def self.replace(path, value)
      new(:replace, path, value)
    end

    # Creates a command with the intention of inserting a new value in a JSON object.
    #
    # Will error if the last element of the path already exists.
    #
    # @param [String] path the path identifying where to insert the value.
    # @param [Object] value the value to insert
    #
    # @return [MutateInSpec]
    def self.dict_add(path, value)
      new(:dict_add, path, value)
    end

    # Creates a command with the intention of removing an existing value in a JSON object.
    #
    # Will error if the path does not exist.
    #
    # @param path the path identifying what to remove.
    #
    # @return [MutateInSpec]
    def self.remove(path)
      new(:remove, path, nil)
    end

    # Creates a command with the intention of upserting a value in a JSON object.
    #
    # That is, the value will be replaced if the path already exists, or inserted if not.
    #
    # @param [String] path the path identifying where to upsert the value.
    # @param [Object] value the value to upsert.
    #
    # @return [MutateInSpec]
    def self.dict_upsert(path, value)
      new(:dict_upsert, path, value)
    end

    # Creates a command with the intention of appending a value to an existing JSON array.
    #
    # Will error if the last element of the path does not exist or is not an array.
    #
    # @param [String] path the path identifying an array to which to append the value.
    # @param [Array] values the value(s) to append.
    #
    # @return [MutateInSpec]
    def self.array_append(path, values)
      new(:array_append, path, values)
    end

    # Creates a command with the intention of prepending a value to an existing JSON array.
    #
    # Will error if the last element of the path does not exist or is not an array.
    #
    # @param [String] path the path identifying an array to which to append the value.
    # @param [Array] values the value(s) to prepend.
    #
    # @return [MutateInSpec]
    def self.array_prepend(path, values)
      new(:array_prepend, path, values)
    end

    # Creates a command with the intention of inserting a value into an existing JSON array.
    #
    # Will error if the last element of the path does not exist or is not an array.
    #
    # @param [String] path the path identifying an array to which to append the value, and an index.  E.g. "foo.bar[3]"
    # @param [Array] values the value(s) to insert.
    #
    # @return [MutateInSpec]
    def self.array_insert(path, values)
      new(:array_insert, path, values)
    end

    # Creates a command with the intent of inserting a value into an existing JSON array, but only if the value
    # is not already contained in the array (by way of string comparison).
    #
    # Will error if the last element of the path does not exist or is not an array.
    #
    # @param [String] path the path identifying an array to which to append the value, and an index.  E.g. "foo.bar[3]"
    # @param [Object] value the value to insert.
    #
    # @return [MutateInSpec]
    def self.array_add_unique(path, value)
      new(:array_add_unique, path, value)
    end

    # Creates a command with the intent of incrementing a numerical field in a JSON object.
    #
    # If the field does not exist, then it is created and takes the value of +delta+
    #
    # @param [String] path the path identifying a numerical field to adjust or create
    # @param [Integer] delta the value to increment the field by
    #
    # @return [MutateInSpec]
    def self.increment(path, delta)
      new(:counter, path, delta.abs)
    end

    # Creates a command with the intent of decrementing a numerical field in a JSON object.
    #
    # If the field does not exist, then it is created and takes the value of +delta+ * -1
    #
    # @param [String] path the path identifying a numerical field to adjust or create
    # @param [Integer] delta the value to decrement the field by
    #
    # @return [MutateInSpec]
    def self.decrement(path, delta)
      new(:counter, path, -1 * delta.abs)
    end

    def xattr
      @xattr = true
      self
    end

    def create_parents
      @create_parents = true
      self
    end

    def xattr?
      @xattr
    end

    def create_parents?
      @create_parents
    end

    def expand_macros?
      @expand_macros
    end

    CAS = "${Mutation.CAS}"
    SEQ_NO = "${Mutation.seqno}"
    VALUE_CRC_32C = "${Mutation.value_crc32c}"

    attr_reader :type
    attr_reader :path
    attr_reader :param

    private

    def initialize(type, path, param)
      @type = type
      @path = path
      @expand_macros = [CAS, SEQ_NO, VALUE_CRC_32C].include?(@param)
      if !@expand_macros && !param.nil?
        @param = JSON.generate(param)
      end
    end
  end
end
