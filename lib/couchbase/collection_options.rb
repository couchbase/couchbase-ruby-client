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

      # @return [String, nil] identifier of the document (used for {Collection#get_multi})
      attr_accessor :id

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
        @id = nil
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

      # @return [String, nil] identifier of the document (used in multi-operations like {Collection#upsert_multi},
      #   {Collection#remove_multi})
      attr_accessor :id

      # @return [Boolean] true if error was not associated with the result (useful for multi-operations)
      def success?
        !error
      end

      # @yieldparam [MutationResult] self
      def initialize
        @error = nil
        @id = nil
        yield self if block_given?
      end
    end

    class LookupInResult
      # @return [Integer] holds the CAS value of the fetched document
      attr_accessor :cas

      # Decodes the content at the given index (or path)
      #
      # @param [Integer, String] path_or_index the index (or path) of the subdocument value to decode
      #
      # @return [Object] the decoded
      def content(path_or_index, transcoder = self.transcoder)
        field = get_field_at_index(path_or_index)
        raise Error::PathNotFound, "Path is not found: #{path_or_index}" unless field.exists

        transcoder.decode(field.value, :json)
      end

      # Allows to check if a value at the given index exists
      #
      # @param [Integer, String] path_or_index the index (or path) of the subdocument value to check
      #
      # @return [Boolean] true if a value is present at the index, false otherwise
      def exists?(path_or_index)
        field =
          case path_or_index
          when String
            encoded.find { |f| f.path == path_or_index }
          else
            return false unless path_or_index >= 0 && path_or_index < encoded.size

            encoded[path_or_index]
          end
        return false unless field

        field.exists
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

      def get_field_at_index(path_or_index)
        case path_or_index
        when String
          encoded.find { |field| field.path == path_or_index } or raise Error::PathInvalid, "Path is not found: #{path_or_index}"
        else
          raise Error::PathInvalid, "Index is out of bounds: #{path_or_index}" unless path_or_index >= 0 && path_or_index < encoded.size

          encoded[path_or_index]
        end
      end
    end

    class MutateInResult < MutationResult
      # Decodes the content at the given index
      #
      # @param [Integer, String] path_or_index the index (or path) of the subdocument value to decode
      #
      # @return [Object] the decoded
      def content(path_or_index, transcoder = self.transcoder)
        field = get_field_at_index(path_or_index)
        transcoder.decode(field.value, :json)
      end

      # @yieldparam [MutateInResult] self
      def initialize
        super
        yield self if block_given?
      end

      # @return [Array<SubDocumentField>] holds the encoded subdocument responses
      # @api private
      attr_accessor :encoded

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

      def get_field_at_index(path_or_index)
        case path_or_index
        when String
          encoded.find { |field| field.path == path_or_index } or raise Error::PathInvalid, "Path is not found: #{path_or_index}"
        else
          raise Error::PathInvalid, "Index is out of bounds: #{path_or_index}" unless path_or_index >= 0 && path_or_index < encoded.size

          encoded[path_or_index]
        end
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

      # @yieldparam [SubDocumentField] self
      def initialize
        yield self if block_given?
      end
    end
  end
end
