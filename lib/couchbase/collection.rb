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

require 'couchbase/common_options'
require 'couchbase/results'
require 'couchbase/subdoc'
require 'couchbase/mutation_state'

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
      resp = @backend.get(bucket_name, "#{@scope_name}.#{@name}", id)
      GetResult.new do |res|
        res.cas = resp[:cas]
        res.content = resp[:content]
      end
    end

    # Fetches the full document and write-locks it for the given duration
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [Integer] lock_time how long to lock the document (values over 30 seconds will be capped)
    # @param [GetAndLockOptions] options request customization
    #
    # @return [GetResult]
    def get_and_lock(id, lock_time, options = GetAndLockOptions.new) end

    # Fetches a full document and resets its expiration time to the expiration duration provided
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [Integer] expiration the new expiration time for the document
    # @param [GetAndTouchOptions] options request customization
    #
    # @return [GetResult]
    def get_and_touch(id, expiration, options = GetAndTouchOptions.new) end

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
    def exists(id, options = ExistsOptions.new) end

    # Removes a document from the collection
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [RemoveOptions] options request customization
    #
    # @return [MutationResult]
    def remove(id, options = RemoveOptions.new)
      resp = @backend.remove(bucket_name, "#{@scope_name}.#{@name}", id, {
          durability_level: options.durability_level
      })
      MutationResult.new do |res|
        res.cas = resp[:cas]
        res.mutation_token = MutationToken.new do |token|
          token.partition_id = resp[:mutation_token][:partition_id]
          token.partition_uuid = resp[:mutation_token][:partition_uuid]
          token.sequence_number = resp[:mutation_token][:sequence_number]
          token.bucket_name = @bucket_name
        end
      end
    end

    # Inserts a full document which does not exist yet
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [Object] content the document content to insert
    # @param [InsertOptions] options request customization
    #
    # @return [MutationResult]
    def insert(id, content, options = InsertOptions.new) end

    # Upserts (inserts or updates) a full document which might or might not exist yet
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [Object] content the document content to upsert
    # @param [UpsertOptions] options request customization
    #
    # @return [MutationResult]
    def upsert(id, content, options = UpsertOptions.new)
      resp = @backend.upsert(bucket_name, "#{@scope_name}.#{@name}", id, JSON.dump(content), {
          durability_level: options.durability_level
      })
      MutationResult.new do |res|
        res.cas = resp[:cas]
        res.mutation_token = MutationToken.new do |token|
          token.partition_id = resp[:mutation_token][:partition_id]
          token.partition_uuid = resp[:mutation_token][:partition_uuid]
          token.sequence_number = resp[:mutation_token][:sequence_number]
          token.bucket_name = @bucket_name
        end
      end
    end

    # Replaces a full document which already exists
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [Object] content the document content to upsert
    # @param [ReplaceOptions] options request customization
    #
    # @return [MutationResult]
    def replace(id, content, options = ReplaceOptions.new) end

    # Update the expiration of the document with the given id
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [Integer] expiration new expiration time for the document
    # @param [TouchOptions] options request customization
    #
    # @return [MutationResult]
    def touch(id, expiration, options = TouchOptions.new) end

    # Unlocks a document if it has been locked previously
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [Integer] cas CAS value which is needed to unlock the document
    # @param [TouchOptions] options request customization
    def unlock(id, cas, options = UnlockOptions.new) end

    # Performs lookups to document fragments
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [Array<LookupInSpec>] specs the list of specifications which describe the types of the lookups to perform
    # @param [LookupInOptions] options request customization
    #
    # @return [LookupInResult]
    def lookup_in(id, specs, options = LookupInOptions.new)
      resp = @backend.lookup_in(
          bucket_name, "#{@scope_name}.#{@name}", id, options.access_deleted,
          specs.map { |s| {opcode: s.type, xattr: s.xattr?, path: s.path} }
      )
      LookupInResult.new do |res|
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
      resp = @backend.mutate_in(
          bucket_name, "#{@scope_name}.#{@name}", id, options.access_deleted,
          specs.map { |s| {opcode: s.type, path: s.path, param: s.param,
                           xattr: s.xattr?, expand_macros: s.expand_macros?, create_parents: s.create_parents?} }, {
              durability_level: options.durability_level
          }
      )
      MutateInResult.new do |res|
        res.cas = resp[:cas]
        res.mutation_token = MutationToken.new do |token|
          token.partition_id = resp[:mutation_token][:partition_id]
          token.partition_uuid = resp[:mutation_token][:partition_uuid]
          token.sequence_number = resp[:mutation_token][:sequence_number]
          token.bucket_name = @bucket_name
        end
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

      # @return [Proc] transcoder used for decoding
      attr_accessor :transcoder

      # @yieldparam [GetOptions] self
      def initialize
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
      # @return [Proc] transcoder used for decoding
      attr_accessor :transcoder

      # @yieldparam [GetAndLockOptions] self
      def initialize
        yield self if block_given?
      end
    end

    class GetAndTouchOptions < CommonOptions
      # @return [Proc] transcoder used for decoding
      attr_accessor :transcoder

      # @yieldparam [GetAndTouchOptions] self
      def initialize
        yield self if block_given?
      end
    end

    class GetAllReplicasOptions < CommonOptions
      # @return [Proc] transcoder used for decoding
      attr_accessor :transcoder

      # @yieldparam [GetAllReplicasOptions] self
      def initialize
        yield self if block_given?
      end
    end

    class GetAnyReplicaOptions < CommonOptions
      # @return [Proc] transcoder used for decoding
      attr_accessor :transcoder

      # @yieldparam [GetAnyReplicaOptions] self
      def initialize
        yield self if block_given?
      end
    end

    class ExistsOptions < CommonOptions
      # @yieldparam [ExistsOptions] self
      def initialize
        yield self if block_given?
      end
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
        @durability_level = :none
        yield self if block_given?
      end
    end

    class UpsertOptions < CommonOptions
      # @return [Integer] expiration time to associate with the document
      attr_accessor :expiration

      # @return [Proc] transcoder used for encoding
      attr_accessor :transcoder

      # @return [:none, :majority, :majority_and_persist_to_active, :persist_to_majority] level of durability
      attr_accessor :durability_level

      # @yieldparam [UpsertOptions]
      def initialize
        @durability_level = :none
        yield self if block_given?
      end
    end

    class ReplaceOptions < CommonOptions
      # @return [Integer] expiration time to associate with the document
      attr_accessor :expiration

      # @return [Proc] transcoder used for encoding
      attr_accessor :transcoder

      # @return [Integer] Specifies a CAS value that will be taken into account on the server side for optimistic concurrency
      attr_accessor :cas

      # @return [:none, :majority, :majority_and_persist_to_active, :persist_to_majority] level of durability
      attr_accessor :durability_level

      # @yieldparam [ReplaceOptions]
      def initialize
        @durability_level = :none
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

      # @yieldparam [LookupInOptions] self
      def initialize
        yield self if block_given?
      end
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
        yield self if block_given?
      end
    end
  end
end
