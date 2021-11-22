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

require "rubygems/deprecate"

require "couchbase/json_transcoder"
require "couchbase/subdoc"
require "couchbase/mutation_state"

module Couchbase
  class Collection
    class GetResult
      extend Gem::Deprecate

      # @return [Integer] holds the CAS value of the fetched document
      attr_accessor :cas

      # @return [Integer] the expiration if fetched and present
      attr_writer :expiry

      # @return [Error::CouchbaseError, nil] error associated with the result, or nil (used in {Collection#get_multi})
      attr_accessor :error

      # @return [Boolean] true if error was not associated with the result (useful for multi-operations)
      def success?
        !error
      end

      # @return [String] The encoded content when loading the document
      # @api private
      attr_accessor :encoded

      # Decodes the content of the document using given (or default transcoder)
      #
      # @param [JsonTranscoder] transcoder custom transcoder
      #
      # @return [Object]
      def content(transcoder = self.transcoder)
        transcoder ? transcoder.decode(@encoded, @flags) : @encoded
      end

      # @return [Time] time when the document will expire
      def expiry_time
        Time.at(@expiry) if @expiry
      end

      # @yieldparam [GetResult] self
      def initialize
        @expiry = nil
        @error = nil
        yield self if block_given?
      end

      # @return [Integer] The flags from the operation
      # @api private
      attr_accessor :flags

      # @return [JsonTranscoder] The default transcoder which should be used
      attr_accessor :transcoder

      # @deprecated Use {#expiry_time}
      # @return [Integer] the expiration if fetched and present
      def expiry # rubocop:disable Style/TrivialAccessors will be removed in next major release
        @expiry
      end
      deprecate :expiry, :expiry_time, 2021, 1
    end

    class GetReplicaResult < GetResult
      # @return [Boolean] true if this result came from a replica
      attr_accessor :is_replica
      alias replica? is_replica
    end

    class ExistsResult
      # @return [Integer] holds the CAS value of the fetched document
      attr_accessor :cas

      # @return [Boolean] true if the document was deleted
      attr_accessor :deleted

      # @return [Boolean] true if the document exists
      attr_accessor :exists
      alias exists? exists

      # @yieldparam [ExistsResult]
      def initialize
        yield self if block_given?
      end

      # @return [Integer] the expiration if fetched and present
      attr_writer :expiry

      # @return [Time] time when the document will expire
      def expiry_time
        Time.at(@expiry) if @expiry
      end

      # @api private
      # @return [Integer] flags
      attr_accessor :flags

      # @api private
      # @return [Integer] sequence_number
      attr_accessor :sequence_number

      # @api private
      # @return [Integer] datatype
      attr_accessor :datatype
    end

    class MutationResult
      # @return [Integer] holds the CAS value of the document after the mutation
      attr_accessor :cas

      # @return [MutationToken] if returned, holds the mutation token of the document after the mutation
      attr_accessor :mutation_token

      # @return [Error::CouchbaseError, nil] error or nil (used in multi-operations like {Collection#upsert_multi},
      #   {Collection#remove_multi})
      attr_accessor :error

      # @return [Boolean] true if error was not associated with the result (useful for multi-operations)
      def success?
        !error
      end

      # @yieldparam [MutationResult] self
      def initialize
        @error = nil
        yield self if block_given?
      end
    end

    class LookupInResult
      # @return [Integer] holds the CAS value of the fetched document
      attr_accessor :cas

      # Decodes the content at the given index
      #
      # @param [Integer] index the index of the subdocument value to decode
      #
      # @return [Object] the decoded
      def content(index, transcoder = self.transcoder)
        transcoder.decode(get_field_at_index(index).value, :json)
      end

      # Allows to check if a value at the given index exists
      #
      # @param [Integer] index the index of the subdocument value to check
      #
      # @return [Boolean] true if a value is present at the index, false otherwise
      def exists?(index)
        !encoded[index].nil? && encoded[index].exists
      end

      # @return [Array<SubDocumentField>] holds the encoded subdocument responses
      attr_accessor :encoded

      # @yieldparam [LookupInResult] self
      def initialize
        @deleted = false
        yield self if block_given?
      end

      # @return [JsonTranscoder] The default transcoder which should be used
      attr_accessor :transcoder

      # @api private
      #
      # @see MutateInOptions#create_as_deleted
      #
      # @return [Boolean] true if the document is a tombstone (created in deleted state)
      def deleted?
        @deleted
      end

      # @api private
      attr_accessor :deleted

      private

      def get_field_at_index(index)
        raise Error::PathInvalid, "Index is out of bounds: #{index}" unless index >= 0 && index < encoded.size

        field = encoded[index]
        raise field.error unless field.success?

        field
      end
    end

    class MutateInResult < MutationResult
      # Decodes the content at the given index
      #
      # @param [Integer] index the index of the subdocument value to decode
      #
      # @return [Object] the decoded
      def content(index, transcoder = self.transcoder)
        field = get_field_at_index(index)
        if field.type == :counter
          field.value
        else
          transcoder.decode(field.value, :json)
        end
      end

      # @yieldparam [MutateInResult] self
      def initialize
        super
        yield self if block_given?
      end

      # @api private
      def success?
        first_error_index.nil?
      end

      # @api private
      def first_error
        encoded[first_error_index].error unless success?
      end

      # @return [Array<SubDocumentField>] holds the encoded subdocument responses
      # @api private
      attr_accessor :encoded

      # @return [Integer, nil] index of first operation entry that generated an error
      # @api private
      attr_accessor :first_error_index

      # @return [JsonTranscoder] The default transcoder which should be used
      attr_accessor :transcoder

      # @api private
      #
      # @see MutateInOptions#create_as_deleted
      #
      # @return [Boolean] true if the document is a tombstone (created in deleted state)
      def deleted?
        @deleted
      end

      # @api private
      attr_accessor :deleted

      private

      def get_field_at_index(index)
        raise Error::PathInvalid, "Index is out of bounds: #{index}" unless index >= 0 && index < encoded.size

        field = encoded[index]
        raise field.error unless field.success?

        field
      end
    end

    # @api private
    class SubDocumentField
      # @return [Boolean] true if the path exists in the document
      attr_accessor :exists

      # @return [String] value
      attr_accessor :value

      # @return [Integer] index
      attr_accessor :index

      # @return [String] path
      attr_accessor :path

      # Operation type
      #
      # * +:set_doc+
      # * +:counter+
      # * +:replace+
      # * +:dict_add+
      # * +:dict_upsert+
      # * +:array_push_first+
      # * +:array_push_last+
      # * +:array_add_unique+
      # * +:array_insert+
      # * +:delete+
      # * +:get+
      # * +:exists+
      # * +:count+
      # * +:get_doc+
      #
      # @return [Symbol]
      attr_accessor :type

      # Status of the subdocument path operation.
      #
      # [+:success+] Indicates a successful response in general.
      # [+:path_not_found+] The provided path does not exist in the document
      # [+:path_mismatch+] One of path components treats a non-dictionary as a dictionary, or a non-array as an array, or value the path
      #   points to is not a number
      # [+:path_invalid+] The path's syntax was incorrect
      # [+:path_too_big+] The path provided is too large: either the string is too long, or it contains too many components
      # [+:value_cannot_insert+] The value provided will invalidate the JSON if inserted
      # [+:doc_not_json+] The existing document is not valid JSON
      # [+:num_range+] The existing number is out of the valid range for arithmetic operations
      # [+:delta_invalid+] The operation would result in a number outside the valid range
      # [+:path_exists+] The requested operation requires the path to not already exist, but it exists
      # [+:value_too_deep+] Inserting the value would cause the document to be too deep
      # [+:invalid_combo+] An invalid combination of commands was specified
      # [+:xattr_invalid_flag_combo+] An invalid combination of operations, using macros when not using extended attributes
      # [+:xattr_invalid_key_combo+] Only single xattr key may be accessed at the same time
      # [+:xattr_unknown_macro+] The server has no knowledge of the requested macro
      # [+:xattr_unknown_vattr+] Unknown virtual attribute.
      # [+:xattr_cannot_modify_vattr+] Cannot modify this virtual attribute.
      # [+:unknown+] Unknown error.
      #
      # @return [Symbol]
      attr_accessor :status

      # @return [nil, Exception]
      attr_accessor :error

      # @yieldparam [SubDocumentField] self
      def initialize
        @status = :unknown
        yield self if block_given?
      end

      # @return [Boolean] true if the path does not have associated error
      def success?
        status == :success
      end
    end
  end
end
