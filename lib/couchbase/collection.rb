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

require "couchbase/json_transcoder"
require "couchbase/common_options"
require "couchbase/subdoc"
require "couchbase/mutation_state"
require "couchbase/errors"

module Couchbase
  class Collection
    attr_reader :bucket_name
    attr_reader :scope_name
    attr_reader :name

    alias_method :inspect, :to_s

    # @param [Couchbase::Backend] backend
    # @param [String] bucket_name name of the bucket
    # @param [String, :_default] scope_name name of the scope
    # @param [String, :_default] collection_name name of the collection
    def initialize(backend, bucket_name, scope_name, collection_name)
      @backend = backend
      @bucket_name = bucket_name
      @scope_name = scope_name
      @name = collection_name
    end

    # Provides access to the binary APIs, not used for JSON documents
    #
    # @return [BinaryCollection]
    def binary
      BinaryCollection.new(self)
    end

    # Fetches the full document from the collection
    #
    # @param [String] id the document id which is used to uniquely identify it
    # @param [GetOptions] options request customization
    #
    # @return [GetResult]
    def get(id, options = GetOptions.new)
      resp = @backend.document_get(bucket_name, "#{@scope_name}.#{@name}", id)
      GetResult.new do |res|
        res.transcoder = options.transcoder
        res.cas = resp[:cas]
        res.flags = resp[:flags]
        res.encoded = resp[:content]
      end
    end

    # Fetches the full document and write-locks it for the given duration
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [Integer] lock_time how long to lock the document (values over 30 seconds will be capped)
    # @param [GetAndLockOptions] options request customization
    #
    # @return [GetResult]
    def get_and_lock(id, lock_time, options = GetAndLockOptions.new)
      resp = @backend.document_get_and_lock(bucket_name, "#{@scope_name}.#{@name}", id, lock_time)
      GetResult.new do |res|
        res.transcoder = options.transcoder
        res.cas = resp[:cas]
        res.flags = resp[:flags]
        res.encoded = resp[:content]
      end
    end

    # Fetches a full document and resets its expiration time to the expiration duration provided
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [Integer] expiration the new expiration time for the document
    # @param [GetAndTouchOptions] options request customization
    #
    # @return [GetResult]
    def get_and_touch(id, expiration, options = GetAndTouchOptions.new)
      resp = @backend.document_get_and_touch(bucket_name, "#{@scope_name}.#{@name}", id, expiration)
      GetResult.new do |res|
        res.transcoder = options.transcoder
        res.cas = resp[:cas]
        res.flags = resp[:flags]
        res.encoded = resp[:content]
      end
    end

    # Reads from all available replicas and the active node and returns the results
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [GetAllReplicasOptions] options request customization
    #
    # @return [Array<GetReplicaResult>]
    def get_all_replicas(id, options = GetAllReplicasOptions.new) end

    # Reads all available replicas, and returns the first found
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [GetAnyReplicaOptions] options request customization
    #
    # @return [GetReplicaResult]
    def get_any_replica(id, options = GetAnyReplicaOptions.new) end

    # Checks if the given document ID exists on the active partition.
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [ExistsOptions] options request customization
    #
    # @return [ExistsResult]
    def exists(id, options = ExistsOptions.new)
      resp = @backend.document_exists(bucket_name, "#{@scope_name}.#{@name}", id)
      ExistsResult.new do |res|
        res.status = resp[:status]
        res.partition_id = resp[:partition_id]
        res.cas = resp[:cas] if res.status != :not_found
      end
    end

    # Removes a document from the collection
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [RemoveOptions] options request customization
    #
    # @return [MutationResult]
    def remove(id, options = RemoveOptions.new)
      resp = @backend.document_remove(bucket_name, "#{@scope_name}.#{@name}", id, {
          durability_level: options.durability_level
      })
      MutationResult.new do |res|
        res.cas = resp[:cas]
        res.mutation_token = extract_mutation_token(resp)
      end
    end

    # Inserts a full document which does not exist yet
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [Object] content the document content to insert
    # @param [InsertOptions] options request customization
    #
    # @return [MutationResult]
    def insert(id, content, options = InsertOptions.new)
      blob, flags = options.transcoder.encode(content)
      resp = @backend.document_insert(bucket_name, "#{@scope_name}.#{@name}", id, blob, flags, {
          durability_level: options.durability_level,
          expiration: options.expiration,
      })
      MutationResult.new do |res|
        res.cas = resp[:cas]
        res.mutation_token = extract_mutation_token(resp)
      end
    end

    # Upserts (inserts or updates) a full document which might or might not exist yet
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [Object] content the document content to upsert
    # @param [UpsertOptions] options request customization
    #
    # @return [MutationResult]
    def upsert(id, content, options = UpsertOptions.new)
      blob, flags = options.transcoder.encode(content)
      resp = @backend.document_upsert(bucket_name, "#{@scope_name}.#{@name}", id, blob, flags, {
          durability_level: options.durability_level,
          expiration: options.expiration,
      })
      MutationResult.new do |res|
        res.cas = resp[:cas]
        res.mutation_token = extract_mutation_token(resp)
      end
    end

    # Replaces a full document which already exists
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [Object] content the document content to upsert
    # @param [ReplaceOptions] options request customization
    #
    # @return [MutationResult]
    def replace(id, content, options = ReplaceOptions.new)
      blob, flags = options.transcoder.encode(content)
      resp = @backend.document_replace(bucket_name, "#{@scope_name}.#{@name}", id, blob, flags, {
          durability_level: options.durability_level,
          expiration: options.expiration,
          cas: options.cas,
      })
      MutationResult.new do |res|
        res.cas = resp[:cas]
        res.mutation_token = extract_mutation_token(resp)
      end
    end

    # Update the expiration of the document with the given id
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [Integer] expiration new expiration time for the document
    # @param [TouchOptions] options request customization
    #
    # @return [MutationResult]
    def touch(id, expiration, options = TouchOptions.new)
      resp = @backend.document_touch(bucket_name, "#{@scope_name}.#{@name}", id, expiration)
      MutationResult.new do |res|
        res.cas = resp[:cas]
      end
    end

    # Unlocks a document if it has been locked previously
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [Integer] cas CAS value which is needed to unlock the document
    # @param [UnlockOptions] options request customization
    #
    # @raise [Error::DocumentNotFound]
    def unlock(id, cas, options = UnlockOptions.new)
      @backend.document_unlock(bucket_name, "#{@scope_name}.#{@name}", id, cas)
    end

    # Performs lookups to document fragments
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [Array<LookupInSpec>] specs the list of specifications which describe the types of the lookups to perform
    # @param [LookupInOptions] options request customization
    #
    # @return [LookupInResult]
    def lookup_in(id, specs, options = LookupInOptions.new)
      resp = @backend.document_lookup_in(
          bucket_name, "#{@scope_name}.#{@name}", id, options.access_deleted,
          specs.map { |s| {opcode: s.type, xattr: s.xattr?, path: s.path} }
      )
      LookupInResult.new do |res|
        res.transcoder = options.transcoder
        res.cas = resp[:cas]
        res.encoded = resp[:fields].map do |field|
          SubDocumentField.new do |f|
            f.exists = field[:exists]
            f.path = field[:path]
            f.status = field[:status]
            f.value = field[:value]
            f.type = field[:opcode]
          end
        end
      end
    end

    # Performs mutations to document fragments
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [Array<MutateInSpec>] specs the list of specifications which describe the types of the lookups to perform
    # @param [MutateInOptions] options request customization
    #
    # @return [MutateInResult]
    def mutate_in(id, specs, options = MutateInOptions.new)
      resp = @backend.document_mutate_in(
          bucket_name, "#{@scope_name}.#{@name}", id, options.access_deleted,
          specs.map { |s| {opcode: s.type, path: s.path, param: s.param,
                           xattr: s.xattr?, expand_macros: s.expand_macros?, create_parents: s.create_parents?} }, {
              durability_level: options.durability_level
          }
      )
      MutateInResult.new do |res|
        res.transcoder = options.transcoder
        res.cas = resp[:cas]
        res.mutation_token = extract_mutation_token(resp)
        res.first_error_index = resp[:first_error_index]
        res.encoded = resp[:fields].map do |field|
          SubDocumentField.new do |f|
            f.exists = field[:exists]
            f.path = field[:path]
            f.status = field[:status]
            f.value = field[:value]
            f.type = field[:opcode]
          end
        end
      end
    end

    class GetOptions < CommonOptions
      # @return [Boolean] if the expiration should also fetched with get
      attr_accessor :with_expiry

      # @return [JsonTranscoder] transcoder used for decoding
      attr_accessor :transcoder

      # @yieldparam [GetOptions] self
      def initialize
        @transcoder = JsonTranscoder.new
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
        @projections |= paths # union with current projections
      end
    end

    class GetAndLockOptions < CommonOptions
      # @return [JsonTranscoder] transcoder used for decoding
      attr_accessor :transcoder

      # @yieldparam [GetAndLockOptions] self
      def initialize
        @transcoder = JsonTranscoder.new
        yield self if block_given?
      end
    end

    class GetAndTouchOptions < CommonOptions
      # @return [JsonTranscoder] transcoder used for decoding
      attr_accessor :transcoder

      # @yieldparam [GetAndTouchOptions] self
      def initialize
        @transcoder = JsonTranscoder.new
        yield self if block_given?
      end
    end

    class GetResult
      # @return [Integer] holds the CAS value of the fetched document
      attr_accessor :cas

      # @return [Integer] the expiration if fetched and present
      attr_accessor :expiration

      # @return [String] The encoded content when loading the document
      attr_accessor :encoded

      # Decodes the content of the document using given (or default transcoder)
      #
      # @param [JsonTranscoder] transcoder custom transcoder
      #
      # @return [Object]
      def content(transcoder = self.transcoder)
        transcoder.decode(@encoded, @flags)
      end

      # @yieldparam [GetResult] self
      def initialize
        yield self if block_given?
      end

      # @return [Integer] The flags from the operation
      attr_accessor :flags

      # @return [JsonTranscoder] The default transcoder which should be used
      attr_accessor :transcoder
    end

    class GetAllReplicasOptions < CommonOptions
      # @return [JsonTranscoder] transcoder used for decoding
      attr_accessor :transcoder

      # @yieldparam [GetAllReplicasOptions] self
      def initialize
        yield self if block_given?
      end
    end

    class GetAnyReplicaOptions < CommonOptions
      # @return [JsonTranscoder] transcoder used for decoding
      attr_accessor :transcoder

      # @yieldparam [GetAnyReplicaOptions] self
      def initialize
        yield self if block_given?
      end
    end

    class GetReplicaResult < GetResult
      # @return [Boolean] true if this result came from a replica
      attr_accessor :is_replica
      alias_method :replica?, :is_replica
    end

    class ExistsOptions < CommonOptions
      # @yieldparam [ExistsOptions] self
      def initialize
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
        @durability_level = :none
        yield self if block_given?
      end
    end

    class InsertOptions < CommonOptions
      # @return [Integer] expiration time to associate with the document
      attr_accessor :expiration

      # @return [Proc] transcoder used for encoding
      attr_accessor :transcoder

      # @return [:none, :majority, :majority_and_persist_to_active, :persist_to_majority] level of durability
      attr_accessor :durability_level

      # @yieldparam [InsertOptions]
      def initialize
        @transcoder = JsonTranscoder.new
        @durability_level = :none
        yield self if block_given?
      end
    end

    class UpsertOptions < CommonOptions
      # @return [Integer] expiration time to associate with the document
      attr_accessor :expiration

      # @return [JsonTranscoder] transcoder used for encoding
      attr_accessor :transcoder

      # @return [:none, :majority, :majority_and_persist_to_active, :persist_to_majority] level of durability
      attr_accessor :durability_level

      # @yieldparam [UpsertOptions]
      def initialize
        @transcoder = JsonTranscoder.new
        @durability_level = :none
        yield self if block_given?
      end
    end

    class ReplaceOptions < CommonOptions
      # @return [Integer] expiration time to associate with the document
      attr_accessor :expiration

      # @return [JsonTranscoder] transcoder used for encoding
      attr_accessor :transcoder

      # @return [Integer] Specifies a CAS value that will be taken into account on the server side for optimistic concurrency
      attr_accessor :cas

      # @return [:none, :majority, :majority_and_persist_to_active, :persist_to_majority] level of durability
      attr_accessor :durability_level

      # @yieldparam [ReplaceOptions]
      def initialize
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
        yield self if block_given?
      end
    end

    class UnlockOptions < CommonOptions
      # @yieldparam [UnlockOptions] self
      def initialize
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
        transcoder.decode(encoded[index].value, :json)
      end

      # Allows to check if a value at the given index exists
      #
      # @param [Integer] index the index of the subdocument value to check
      #
      # @return [Boolean] true if a value is present at the index, false otherwise
      def exists?(index)
        encoded[index].exists
      end

      # @return [Array<SubDocumentField>] holds the encoded subdocument responses
      attr_accessor :encoded

      # @yieldparam [LookupInResult] self
      def initialize
        yield self if block_given?
      end

      # @return [JsonTranscoder] The default transcoder which should be used
      attr_accessor :transcoder
    end

    class MutateInOptions < CommonOptions
      # @return [Integer] expiration time to associate with the document
      attr_accessor :expiration

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
        @durability_level = :none
        @store_semantics = :replace
        @transcoder = JsonTranscoder.new
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
        transcoder.decode(encoded[index].value, :json)
      end

      # @yieldparam [MutateInResult] self
      def initialize
        yield self if block_given?
      end

      def success?
        !!first_error_index
      end

      # @return [Array<SubDocumentField>] holds the encoded subdocument responses
      attr_accessor :encoded

      # @return [Integer, nil] index of first operation entry that generated an error
      attr_accessor :first_error_index

      # @return [JsonTranscoder] The default transcoder which should be used
      attr_accessor :transcoder
    end

    # @api private
    class SubDocumentField
      attr_accessor :error

      # @return [Boolean] true if the path exists in the document
      attr_accessor :exists

      # @return [String] value
      attr_accessor :value

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
      # [+:path_mismatch+] One of path components treats a non-dictionary as a dictionary, or a non-array as an array, or value the path points to is not a number
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

      def initialize
        yield self if block_given?
      end
    end

    private

    def extract_mutation_token(resp)
      MutationToken.new do |token|
        token.partition_id = resp[:mutation_token][:partition_id]
        token.partition_uuid = resp[:mutation_token][:partition_uuid]
        token.sequence_number = resp[:mutation_token][:sequence_number]
        token.bucket_name = @bucket_name
      end
    end
  end
end
