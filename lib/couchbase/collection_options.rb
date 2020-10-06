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

require "rubygems/deprecate"

require "couchbase/json_transcoder"
require "couchbase/common_options"
require "couchbase/subdoc"
require "couchbase/mutation_state"

module Couchbase
  class Collection
    class GetOptions < CommonOptions
      # @return [Boolean] if the expiration should also fetched with get
      attr_accessor :with_expiry

      # @return [JsonTranscoder] transcoder used for decoding
      attr_accessor :transcoder

      # @yieldparam [GetOptions] self
      def initialize
        super
        @transcoder = JsonTranscoder.new
        @preserve_array_indexes = false
        @with_expiry = nil
        @projections = nil
        yield self if block_given?
      end

      # Allows to specify a custom list paths to fetch from the document instead of the whole.
      #
      # Note that a maximum of 16 individual paths can be projected at a time due to a server limitation. If you need
      # more than that, think about fetching less-generic paths or the full document straight away.
      #
      # @param [String, Array<String>] paths a path that should be loaded if present.
      def project(*paths)
        @projections ||= []
        @projections |= paths.flatten # union with current projections
      end

      # @api private
      # @return [Boolean] whether to use sparse arrays (default false)
      attr_accessor :preserve_array_indexes

      # @api private
      # @return [Array<String>] list of paths to project
      attr_accessor :projections

      # @api private
      # @return [Boolean]
      def need_projected_get?
        @with_expiry || !@projections.nil?
      end
    end

    class GetAndLockOptions < CommonOptions
      # @return [JsonTranscoder] transcoder used for decoding
      attr_accessor :transcoder

      # @yieldparam [GetAndLockOptions] self
      def initialize
        super
        @transcoder = JsonTranscoder.new
        yield self if block_given?
      end
    end

    class GetAndTouchOptions < CommonOptions
      # @return [JsonTranscoder] transcoder used for decoding
      attr_accessor :transcoder

      # @yieldparam [GetAndTouchOptions] self
      def initialize
        super
        @transcoder = JsonTranscoder.new
        yield self if block_given?
      end
    end

    class GetResult
      extend Gem::Deprecate

      # @return [Integer] holds the CAS value of the fetched document
      attr_accessor :cas

      # @return [Integer] the expiration if fetched and present
      attr_writer :expiry

      # @return [String] The encoded content when loading the document
      # @api private
      attr_accessor :encoded

      # Decodes the content of the document using given (or default transcoder)
      #
      # @param [JsonTranscoder] transcoder custom transcoder
      #
      # @return [Object]
      def content(transcoder = self.transcoder)
        transcoder.decode(@encoded, @flags)
      end

      # @return [Time] time when the document will expire
      def expiry_time
        Time.at(@expiry) if @expiry
      end

      # @yieldparam [GetResult] self
      def initialize
        @expiry = nil
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

    class GetAllReplicasOptions < CommonOptions
      # @return [JsonTranscoder] transcoder used for decoding
      attr_accessor :transcoder

      # @yieldparam [GetAllReplicasOptions] self
      def initialize
        super
        yield self if block_given?
      end
    end

    class GetAnyReplicaOptions < CommonOptions
      # @return [JsonTranscoder] transcoder used for decoding
      attr_accessor :transcoder

      # @yieldparam [GetAnyReplicaOptions] self
      def initialize
        super
        yield self if block_given?
      end
    end

    class GetReplicaResult < GetResult
      # @return [Boolean] true if this result came from a replica
      attr_accessor :is_replica
      alias replica? is_replica
    end

    class ExistsOptions < CommonOptions
      # @yieldparam [ExistsOptions] self
      def initialize
        super
        yield self if block_given?
      end
    end

    class ExistsResult
      # @return [Integer] holds the CAS value of the fetched document
      attr_accessor :cas

      # @api private
      # @return [:found, :not_found, :persisted, :logically_deleted]
      attr_accessor :status

      def exists?
        status == :found || status == :persisted
      end

      # @yieldparam [ExistsResult]
      def initialize
        @durability_level = :none
        yield self if block_given?
      end

      # @api private
      # @return [Integer] holds the index of the partition, to which the given key is mapped
      attr_accessor :partition_id
    end

    class RemoveOptions < CommonOptions
      # @return [Integer] Specifies a CAS value that will be taken into account on the server side for optimistic concurrency
      attr_accessor :cas

      # @return [:none, :majority, :majority_and_persist_to_active, :persist_to_majority] level of durability
      attr_accessor :durability_level

      # @yieldparam [RemoveOptions]
      def initialize
        super
        @durability_level = :none
        yield self if block_given?
      end
    end

    class InsertOptions < CommonOptions
      # @return [Integer] expiration time to associate with the document
      attr_accessor :expiry

      # @return [Proc] transcoder used for encoding
      attr_accessor :transcoder

      # @return [:none, :majority, :majority_and_persist_to_active, :persist_to_majority] level of durability
      attr_accessor :durability_level

      # @yieldparam [InsertOptions]
      def initialize
        super
        @transcoder = JsonTranscoder.new
        @durability_level = :none
        yield self if block_given?
      end
    end

    class UpsertOptions < CommonOptions
      # @return [Integer] expiration time to associate with the document
      attr_accessor :expiry

      # @return [JsonTranscoder] transcoder used for encoding
      attr_accessor :transcoder

      # @return [:none, :majority, :majority_and_persist_to_active, :persist_to_majority] level of durability
      attr_accessor :durability_level

      # @yieldparam [UpsertOptions]
      def initialize
        super
        @transcoder = JsonTranscoder.new
        @durability_level = :none
        yield self if block_given?
      end
    end

    class ReplaceOptions < CommonOptions
      # @return [Integer] expiration time to associate with the document
      attr_accessor :expiry

      # @return [JsonTranscoder] transcoder used for encoding
      attr_accessor :transcoder

      # @return [Integer] Specifies a CAS value that will be taken into account on the server side for optimistic concurrency
      attr_accessor :cas

      # @return [:none, :majority, :majority_and_persist_to_active, :persist_to_majority] level of durability
      attr_accessor :durability_level

      # @yieldparam [ReplaceOptions]
      def initialize
        super
        @transcoder = JsonTranscoder.new
        @durability_level = :none
        yield self if block_given?
      end
    end

    class MutationResult
      # @return [Integer] holds the CAS value of the document after the mutation
      attr_accessor :cas

      # @return [MutationToken] if returned, holds the mutation token of the document after the mutation
      attr_accessor :mutation_token

      # @yieldparam [MutationResult] self
      def initialize
        yield self if block_given?
      end
    end

    class TouchOptions < CommonOptions
      # @yieldparam [TouchOptions] self
      def initialize
        super
        yield self if block_given?
      end
    end

    class UnlockOptions < CommonOptions
      # @yieldparam [UnlockOptions] self
      def initialize
        super
        yield self if block_given?
      end
    end

    class LookupInOptions < CommonOptions
      # @return [Boolean] For internal use only: allows access to deleted documents that are in 'tombstone' form
      attr_accessor :access_deleted

      # @return [JsonTranscoder] transcoder used for decoding
      attr_accessor :transcoder

      # @yieldparam [LookupInOptions] self
      def initialize
        super
        @transcoder = JsonTranscoder.new
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

      # @see {MutateInOptions#create_as_deleted}
      #
      # @return [Boolean] true if the document is a tombstone (created in deleted state)
      def deleted?
        @deleted
      end

      attr_accessor :deleted

      # @return [Array<SubDocumentField>] holds the encoded subdocument responses
      attr_accessor :encoded

      # @yieldparam [LookupInResult] self
      def initialize
        @deleted = false
        yield self if block_given?
      end

      # @return [JsonTranscoder] The default transcoder which should be used
      attr_accessor :transcoder

      private

      def get_field_at_index(index)
        raise Error::PathInvalid, "Index is out of bounds: #{index}" unless index >= 0 && index < encoded.size

        field = encoded[index]
        raise field.error unless field.success?

        field
      end
    end

    class MutateInOptions < CommonOptions
      # @return [Integer] expiration time to associate with the document
      attr_accessor :expiry

      # Describes how the outer document store semantics on subdoc should act
      #
      # * +:replace+: replace the document, fail if it does not exist. This is the default
      # * +:upsert+: replace the document or create if it does not exist
      # * +:insert+: create the document, fail if it exists
      #
      # @return [:replace, :upsert, :insert]
      attr_accessor :store_semantics

      # @return [Integer] Specifies a CAS value that will be taken into account on the server side for optimistic concurrency
      attr_accessor :cas

      # @return [Boolean] For internal use only: allows access to deleted documents that are in 'tombstone' form
      attr_accessor :access_deleted

      # @return [Boolean] For internal use only: allows creating documents in 'tombstone' form
      attr_accessor :create_as_deleted

      # @return [:none, :majority, :majority_and_persist_to_active, :persist_to_majority] level of durability
      attr_accessor :durability_level

      # @yieldparam [MutateInOptions]
      def initialize
        super
        @durability_level = :none
        @store_semantics = :replace
        @transcoder = JsonTranscoder.new
        @access_deleted = false
        @create_as_deleted = false
        yield self if block_given?
      end

      attr_accessor :transcoder
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

      # @see {MutateInOptions#create_as_deleted}
      #
      # @return [Boolean] true if the document is a tombstone (created in deleted state)
      def deleted?
        @deleted
      end

      attr_accessor :deleted

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
