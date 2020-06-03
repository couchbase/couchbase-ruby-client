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

require "couchbase/errors"
require "couchbase/collection_options"
require "couchbase/binary_collection"

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
      resp = if options.need_projected_get?
               @backend.document_get_projected(bucket_name, "#{@scope_name}.#{@name}", id,
                                               options.with_expiration, options.projections,
                                               options.preserve_array_indexes)
             else
               @backend.document_get(bucket_name, "#{@scope_name}.#{@name}", id)
             end
      GetResult.new do |res|
        res.transcoder = options.transcoder
        res.cas = resp[:cas]
        res.flags = resp[:flags]
        res.encoded = resp[:content]
        res.expiration = resp[:expiration] if resp.key?(:expiration)
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

    private

    def extract_mutation_token(resp)
      MutationToken.new do |token|
        token.partition_id = resp[:mutation_token][:partition_id]
        token.partition_uuid = resp[:mutation_token][:partition_uuid]
        token.sequence_number = resp[:mutation_token][:sequence_number]
        token.bucket_name = resp[:mutation_token][:bucket_name]
      end
    end
  end
end
