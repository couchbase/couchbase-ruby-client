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
  # Provides access to all collection APIs
  class Collection
    attr_reader :bucket_name
    attr_reader :scope_name
    attr_reader :name

    alias inspect to_s

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
    # @param [Options::Get] options request customization
    #
    # @example Get document contents
    #   res = collection.get("customer123")
    #   res.content["addresses"]
    #
    #   # {"billing"=>
    #   #   {"line1"=>"123 Any Street", "line2"=>"Anytown", "country"=>"United Kingdom"},
    #   #  "delivery"=>
    #   #   {"line1"=>"123 Any Street", "line2"=>"Anytown", "country"=>"United Kingdom"}}
    #
    # @example Get partial document using projections
    #   res = collection.get("customer123", Options::Get(projections: ["name", "addresses.billing"]))
    #   res.content
    #
    #   # {"addresses"=>
    #   #    {"billing"=>
    #   #      {"country"=>"United Kingdom",
    #   #       "line1"=>"123 Any Street",
    #   #       "line2"=>"Anytown"}},
    #   #   "name"=>"Douglas Reynholm"}
    #
    # @return [GetResult]
    def get(id, options = Options::Get.new)
      resp = if options.need_projected_get?
               @backend.document_get_projected(bucket_name, "#{@scope_name}.#{@name}", id, options.to_backend)
             else
               @backend.document_get(bucket_name, "#{@scope_name}.#{@name}", id, options.to_backend)
             end
      GetResult.new do |res|
        res.transcoder = options.transcoder
        res.cas = resp[:cas]
        res.flags = resp[:flags]
        res.encoded = resp[:content]
        res.expiry = resp[:expiry] if resp.key?(:expiry)
      end
    end

    # Fetches multiple documents from the collection.
    #
    # @note that it will not generate {Error::DocumentNotFound} exceptions in this case. The caller should check
    #  {GetResult#error} property of the result
    #
    # @param [Array<String>] ids the array of document identifiers
    # @param [Options::GetMulti] options request customization
    #
    # @example Fetch "foo" and "bar" in a batch
    #   res = collection.get(["foo", "bar"], Options::GetMulti(timeout: 3_000))
    #   res[0].content #=> content of "foo"
    #   res[1].content #=> content of "bar"
    #
    # @return [Array<GetResult>]
    def get_multi(ids, options = Options::GetMulti.new)
      collection_spec = "#{@scope_name}.#{@name}"
      resp = @backend.document_get_multi(ids.map { |id| [bucket_name, collection_spec, id] }, options.to_backend)
      resp.map do |entry|
        GetResult.new do |res|
          res.transcoder = options.transcoder
          res.cas = entry[:cas]
          res.flags = entry[:flags]
          res.encoded = entry[:content]
          res.error = entry[:error]
        end
      end
    end

    # Fetches the full document and write-locks it for the given duration
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [Integer, #in_seconds] lock_time how long to lock the document (values over 30 seconds will be capped)
    # @param [Options::GetAndLock] options request customization
    #
    # @example Retrieve document and lock for 10 seconds
    #   collection.get_and_lock("customer123", 10, Options::GetAndLock(timeout: 3_000))
    #
    # @example Update document pessimistically
    #   res = collection.get_and_lock("customer123", 10)
    #   user_data = res.content
    #   user_data["admin"] = true
    #   collection.replace("user", user_data, Options::Upsert(cas: res.cas))
    #
    # @return [GetResult]
    def get_and_lock(id, lock_time, options = Options::GetAndLock.new)
      resp = @backend.document_get_and_lock(bucket_name, "#{@scope_name}.#{@name}", id,
                                            lock_time.respond_to?(:in_seconds) ? lock_time.public_send(:in_seconds) : lock_time,
                                            options.to_backend)
      GetResult.new do |res|
        res.transcoder = options.transcoder
        res.cas = resp[:cas]
        res.flags = resp[:flags]
        res.encoded = resp[:content]
      end
    end

    # Fetches a full document and resets its expiration time to the duration provided
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [Integer, #in_seconds] expiry the new expiration time for the document
    # @param [Options::GetAndTouch] options request customization
    #
    # @example Retrieve document and prolong its expiration for another 10 seconds
    #   collection.get_and_touch("customer123", 10)
    #
    # @return [GetResult]
    def get_and_touch(id, expiry, options = Options::GetAndTouch.new)
      resp = @backend.document_get_and_touch(bucket_name, "#{@scope_name}.#{@name}", id,
                                             expiry.respond_to?(:in_seconds) ? expiry.public_send(:in_seconds) : expiry,
                                             options.to_backend)
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
    # @param [Options::GetAllReplicas] options request customization
    #
    # @return [Array<GetReplicaResult>]
    def get_all_replicas(id, options = Options::GetAllReplicas.new) end

    # Reads all available replicas, and returns the first found
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [Options::GetAnyReplica] options request customization
    #
    # @return [GetReplicaResult]
    def get_any_replica(id, options = Options::GetAnyReplica.new) end

    # Checks if the given document ID exists on the active partition.
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [Options::Exists] options request customization
    #
    # @example Check if the document exists without fetching its contents
    #   res = collection.exists("customer123")
    #   res.exists? #=> true
    #
    # @return [ExistsResult]
    def exists(id, options = Options::Exists.new)
      resp = @backend.document_exists(bucket_name, "#{@scope_name}.#{@name}", id, options.to_backend)
      ExistsResult.new do |res|
        res.status = resp[:status]
        res.partition_id = resp[:partition_id]
        res.cas = resp[:cas] if res.status != :not_found
      end
    end

    # Removes a document from the collection
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [Options::Remove] options request customization
    #
    # @example Remove the document in collection
    #   res = collection.remove("customer123")
    #   res.cas #=> 241994216651798
    #
    # @example Remove the document in collection, but apply optimistic lock
    #   res = collection.upsert("mydoc", {"foo" => 42})
    #   res.cas #=> 7751414725654
    #
    #   begin
    #     res = collection.remove("mydoc", Options::Remove(cas: 3735928559))
    #   rescue Error::CasMismatch
    #     puts "Failed to remove the document, it might be changed by other application"
    #   end
    #
    # @return [MutationResult]
    def remove(id, options = Options::Remove.new)
      resp = @backend.document_remove(bucket_name, "#{@scope_name}.#{@name}", id, options.to_backend)
      MutationResult.new do |res|
        res.cas = resp[:cas]
        res.mutation_token = extract_mutation_token(resp)
      end
    end

    # Removes a list of the documents from the collection
    #
    # @note that it will not generate {Error::DocumentNotFound} or {Error::CasMismatch} exceptions in this case.
    #  The caller should check {MutationResult#error} property of the result
    #
    # @param [Array<String, Array>] ids the array of document ids, or ID/CAS pairs +[String,Integer]+
    # @param [Options::RemoveMulti] options request customization
    #
    # @example Remove two documents in collection. For "mydoc" apply optimistic lock
    #   res = collection.upsert("mydoc", {"foo" => 42})
    #   res.cas #=> 7751414725654
    #
    #   res = collection.remove_multi(["foo", ["mydoc", res.cas]])
    #   if res[1].error.is_a?(Error::CasMismatch)
    #     puts "Failed to remove the document, it might be changed by other application"
    #   end
    #
    # @return [Array<MutationResult>]
    def remove_multi(ids, options = Options::RemoveMulti.new)
      collection_spec = "#{@scope_name}.#{@name}"
      resp = @backend.document_remove_multi(ids.map do |id|
        case id
        when String
          [bucket_name, collection_spec, id, nil]
        when Array
          [bucket_name, collection_spec, id[0], id[1]]
        else
          raise ArgumentError, "id argument of remove_multi must be a String or Array<String, Integer>, given: #{id.inspect}"
        end
      end, options.to_backend)
      resp.map do |entry|
        MutationResult.new do |res|
          res.cas = entry[:cas]
          res.mutation_token = extract_mutation_token(entry)
        end
      end
    end

    # Inserts a full document which does not exist yet
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [Object] content the document content to insert
    # @param [Options::Insert] options request customization
    #
    # @example Insert new document in collection
    #   res = collection.insert("mydoc", {"foo" => 42}, Options::Insert(expiry: 20))
    #   res.cas #=> 242287264414742
    #
    # @example Handle error when the document already exists
    #   collection.exists("mydoc").exists? #=> true
    #   begin
    #     res = collection.insert("mydoc", {"foo" => 42})
    #   rescue Error::DocumentExists
    #     puts "Failed to insert the document, it already exists in the collection"
    #   end
    #
    # @return [MutationResult]
    def insert(id, content, options = Options::Insert.new)
      blob, flags = options.transcoder ? options.transcoder.encode(content) : [content, 0]
      resp = @backend.document_insert(bucket_name, "#{@scope_name}.#{@name}", id, blob, flags, options.to_backend)
      MutationResult.new do |res|
        res.cas = resp[:cas]
        res.mutation_token = extract_mutation_token(resp)
      end
    end

    # Upserts (inserts or updates) a full document which might or might not exist yet
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [Object] content the document content to upsert
    # @param [Options::Upsert] options request customization
    #
    # @example Upsert new document in collection
    #   res = collection.upsert("mydoc", {"foo" => 42}, Options::Upsert(expiry: 20))
    #   res.cas #=> 242287264414742
    #
    # @return [MutationResult]
    def upsert(id, content, options = Options::Upsert.new)
      blob, flags = options.transcoder ? options.transcoder.encode(content) : [content, 0]
      resp = @backend.document_upsert(bucket_name, "#{@scope_name}.#{@name}", id, blob, flags, options.to_backend)
      MutationResult.new do |res|
        res.cas = resp[:cas]
        res.mutation_token = extract_mutation_token(resp)
      end
    end

    # Upserts (inserts or updates) a list of documents which might or might not exist yet
    #
    # @note that it will not generate exceptions in this case. The caller should check {MutationResult#error} property of the
    #  result
    #
    # @param [Array<Array>] id_content array of tuples +String,Object+, where first entry treated as document key,
    #   and the second as value to upsert.
    # @param [Options::UpsertMulti] options request customization
    #
    # @example Upsert two documents with IDs "foo" and "bar" into a collection
    #   res = collection.upsert_multi([
    #     "foo", {"foo" => 42},
    #     "bar", {"bar" => "some value"}
    #   ])
    #   res[0].cas #=> 7751414725654
    #   res[1].cas #=> 7751418925851
    #
    # @return [Array<MutationResult>]
    def upsert_multi(id_content, options = Options::UpsertMulti.new)
      collection_spec = "#{@scope_name}.#{@name}"
      resp = @backend.document_upsert_multi(id_content.map do |(id, content)|
        blob, flags = options.transcoder ? options.transcoder.encode(content) : [content, 0]
        [bucket_name, collection_spec, id, blob, flags]
      end, options.to_backend)
      resp.map do |entry|
        MutationResult.new do |res|
          res.cas = entry[:cas]
          res.mutation_token = extract_mutation_token(entry)
        end
      end
    end

    # Replaces a full document which already exists
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [Object] content the document content to upsert
    # @param [Options::Replace] options request customization
    #
    # @example Replace new document in collection with optimistic locking
    #   res = collection.get("mydoc")
    #   res = collection.replace("mydoc", {"foo" => 42}, Options::Replace(cas: res.cas))
    #   res.cas #=> 242287264414742
    #
    # @return [MutationResult]
    def replace(id, content, options = Options::Replace.new)
      blob, flags = options.transcoder ? options.transcoder.encode(content) : [content, 0]
      resp = @backend.document_replace(bucket_name, "#{@scope_name}.#{@name}", id, blob, flags, options.to_backend)
      MutationResult.new do |res|
        res.cas = resp[:cas]
        res.mutation_token = extract_mutation_token(resp)
      end
    end

    # Update the expiration of the document with the given id
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [Integer, #in_seconds] expiry new expiration time for the document
    # @param [Options::Touch] options request customization
    #
    # @example Reset expiration timer for document to 30 seconds
    #   res = collection.touch("customer123", 30)
    #
    # @return [MutationResult]
    def touch(id, expiry, options = Options::Touch.new)
      resp = @backend.document_touch(bucket_name, "#{@scope_name}.#{@name}", id,
                                     expiry.respond_to?(:in_seconds) ? expiry.public_send(:in_seconds) : expiry,
                                     options.to_backend)
      MutationResult.new do |res|
        res.cas = resp[:cas]
      end
    end

    # Unlocks a document if it has been locked previously
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [Integer] cas CAS value which is needed to unlock the document
    # @param [Options::Unlock] options request customization
    #
    # @example Lock (pessimistically) and unlock document
    #   res = collection.get_and_lock("customer123", 10)
    #   collection.unlock("customer123", res.cas)
    #
    # @return [void]
    #
    # @raise [Error::DocumentNotFound]
    def unlock(id, cas, options = Options::Unlock.new)
      @backend.document_unlock(bucket_name, "#{@scope_name}.#{@name}", id, cas, options.to_backend)
    end

    # Performs lookups to document fragments
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [Array<LookupInSpec>] specs the list of specifications which describe the types of the lookups to perform
    # @param [Options::LookupIn] options request customization
    #
    # @example Get list of IDs of completed purchases
    #   lookup_specs = [
    #     LookupInSpec::get("purchases.complete")
    #   ]
    #   collection.lookup_in("customer123", lookup_specs)
    #
    # @return [LookupInResult]
    def lookup_in(id, specs, options = Options::LookupIn.new)
      resp = @backend.document_lookup_in(
        bucket_name, "#{@scope_name}.#{@name}", id,
        specs.map do |s|
          {
            opcode: s.type,
            xattr: s.xattr?,
            path: s.path,
          }
        end, options.to_backend
      )
      LookupInResult.new do |res|
        res.transcoder = options.transcoder
        res.cas = resp[:cas]
        res.deleted = resp[:deleted]
        res.encoded = resp[:fields].map do |field|
          SubDocumentField.new do |f|
            f.exists = field[:exists]
            f.index = field[:index]
            f.path = field[:path]
            f.status = field[:status]
            f.error = field[:error]
            f.value = field[:value]
            f.type = field[:type]
          end
        end
      end
    end

    # Performs mutations to document fragments
    #
    # @param [String] id the document id which is used to uniquely identify it.
    # @param [Array<MutateInSpec>] specs the list of specifications which describe the types of the lookups to perform
    # @param [Options::MutateIn] options request customization
    #
    # @example Append number into subarray of the document
    #   mutation_specs = [
    #     MutateInSpec::array_append("purchases.complete", [42])
    #   ]
    #   collection.mutate_in("customer123", mutation_specs, Options::MutateIn(expiry: 10))
    #
    # @return [MutateInResult]
    def mutate_in(id, specs, options = Options::MutateIn.new)
      resp = @backend.document_mutate_in(
        bucket_name, "#{@scope_name}.#{@name}", id,
        specs.map do |s|
          {
            opcode: s.type,
            path: s.path,
            param: s.param,
            xattr: s.xattr?,
            expand_macros: s.expand_macros?,
            create_path: s.create_path?,
          }
        end, options.to_backend
      )
      result = MutateInResult.new do |res|
        res.transcoder = options.transcoder
        res.cas = resp[:cas]
        res.deleted = resp[:deleted]
        res.mutation_token = extract_mutation_token(resp)
        res.first_error_index = resp[:first_error_index]
        res.encoded = resp[:fields].map do |field|
          SubDocumentField.new do |f|
            f.exists = field[:exists]
            f.path = field[:path]
            f.status = field[:status]
            f.error = field[:error]
            f.value = field[:value]
            f.type = field[:type]
          end
        end
      end
      raise result.first_error unless result.success?

      result
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

    # @api private
    # TODO: deprecate in 3.1
    GetOptions = ::Couchbase::Options::Get
    # @api private
    # TODO: deprecate in 3.1
    GetAndLockOptions = ::Couchbase::Options::GetAndLock
    # @api private
    # TODO: deprecate in 3.1
    GetAndTouchOptions = ::Couchbase::Options::GetAndTouch
    # @api private
    # TODO: deprecate in 3.1
    LookupInOptions = ::Couchbase::Options::LookupIn
    # @api private
    # TODO: deprecate in 3.1
    MutateInOptions = ::Couchbase::Options::MutateIn
    # @api private
    # TODO: deprecate in 3.1
    UnlockOptions = ::Couchbase::Options::Unlock
    # @api private
    # TODO: deprecate in 3.1
    TouchOptions = ::Couchbase::Options::Touch
    # @api private
    # TODO: deprecate in 3.1
    ReplaceOptions = ::Couchbase::Options::Replace
    # @api private
    # TODO: deprecate in 3.1
    UpsertOptions = ::Couchbase::Options::Upsert
    # @api private
    # TODO: deprecate in 3.1
    InsertOptions = ::Couchbase::Options::Insert
    # @api private
    # TODO: deprecate in 3.1
    RemoveOptions = ::Couchbase::Options::Remove
    # @api private
    # TODO: deprecate in 3.1
    ExistsOptions = ::Couchbase::Options::Exists
    # @api private
    # TODO: deprecate in 3.1
    GetAnyReplicaOptions = ::Couchbase::Options::GetAnyReplica
    # @api private
    # TODO: deprecate in 3.1
    GetAllReplicasOptions = ::Couchbase::Options::GetAllReplicas
  end
end
