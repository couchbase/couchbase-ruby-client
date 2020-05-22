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

require 'json'

module Couchbase
  class QueryResult
    # @return [QueryMetaData] returns object representing additional metadata associated with this query
    attr_accessor :meta_data

    attr_accessor :transcoder

    # Returns all rows converted using a transcoder
    #
    # @return [Array]
    def rows(transcoder = self.transcoder)
      @rows.lazy.map do |row|
        if transcoder == :json
          JSON.parse(row)
        else
          transcoder.call(row)
        end
      end
    end

    def initialize
      yield self if block_given?
      @transcoder = :json
    end
  end

  class QueryMetaData
    # @return [String] returns the request identifier string of the query request
    attr_accessor :request_id

    # @return [String] returns the client context identifier string set of the query request
    attr_accessor :client_context_id

    # @return [Symbol] returns raw query execution status as returned by the query engine
    attr_accessor :status

    # @return [Hash] returns the signature as returned by the query engine which is then decoded as JSON object
    attr_accessor :signature

    # @return [Hash] returns the profiling information returned by the query engine which is then decoded as JSON object
    attr_accessor :profile

    # @return [QueryMetrics] metrics as returned by the query engine, if enabled
    attr_accessor :metrics

    # @return [List<QueryWarning>] list of warnings returned by the query engine
    attr_accessor :warnings

    def initialize
      yield self if block_given?
    end
  end

  class QueryMetrics
    # @return [Integer] The total time taken for the request, that is the time from when the request was received until the results were returned
    attr_accessor :elapsed_time

    # @return [Integer] The time taken for the execution of the request, that is the time from when query execution started until the results were returned
    attr_accessor :execution_time

    # @return [Integer] the total number of results selected by the engine before restriction through LIMIT clause.
    attr_accessor :sort_count

    # @return [Integer] The total number of objects in the results.
    attr_accessor :result_count

    # @return [Integer] The total number of bytes in the results.
    attr_accessor :result_size

    # @return [Integer] The number of mutations that were made during the request.
    attr_accessor :mutation_count

    # @return [Integer] The number of errors that occurred during the request.
    attr_accessor :error_count

    # @return [Integer] The number of warnings that occurred during the request.
    attr_accessor :warning_count

    def initialize
      yield self if block_given?
    end
  end

  # Represents a single warning returned from the query engine.
  class QueryWarning
    # @return [Integer]
    attr_accessor :code

    # @return [String]
    attr_accessor :message

    def initialize(code, message)
      @code = code
      @message = message
    end
  end

  class GetResult
    # @return [Integer] holds the CAS value of the fetched document
    attr_accessor :cas

    # @return [Integer] the expiration if fetched and present
    attr_accessor :expiration

    # @return [String] The encoded content when loading the document
    attr_accessor :content

    # Decodes the content of the document using given (or default transcoder)
    #
    # @param [Proc, :json] transcoder custom transcoder
    #
    # @return [Object]
    def content_as(transcoder = self.transcoder)
      if transcoder == :json
        JSON.parse(@content)
      else
        transcoder.call(@content, @flags)
      end
    end

    def initialize
      yield self if block_given?
      @transcoder = :json
    end

    protected

    # @return [Integer] The flags from the operation
    attr_accessor :flags

    # @return [Proc] The default transcoder which should be used
    attr_accessor :transcoder
  end

  class GetReplicaResult < GetResult
    # @return [Boolean] true if this result came from a replica
    attr_accessor :is_replica
    alias_method :replica?, :is_replica
  end

  class ExistsResult
    # @return [Integer] holds the CAS value of the fetched document
    attr_accessor :cas

    # @return [Boolean] Holds the boolean if the document actually exists
    attr_accessor :exists
    alias_method :exists?, :exists
  end

  class MutationResult
    # @return [Integer] holds the CAS value of the document after the mutation
    attr_accessor :cas

    # @return [MutationToken] if returned, holds the mutation token of the document after the mutation
    attr_accessor :mutation_token

    def initialize
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
      if transcoder == :json
        return if encoded[index].value&.empty?
        JSON.parse(encoded[index].value)
      else
        transcoder.call(encoded[index].value)
      end
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

    def initialize
      yield self if block_given?
      @transcoder = :json
    end

    private

    # @return [Proc] The default transcoder which should be used
    attr_accessor :transcoder
  end

  class MutateInResult < MutationResult
    # Decodes the content at the given index
    #
    # @param [Integer] index the index of the subdocument value to decode
    #
    # @return [Object] the decoded
    def content(index, transcoder = self.transcoder)
      if transcoder == :json
        JSON.parse(encoded[index].value)
      else
        transcoder.call(encoded[index].value)
      end
    end

    def initialize
      yield self if block_given?
      @transcoder = :json
    end

    def success?
      !!first_error_index
    end

    # @return [Array<SubDocumentField>] holds the encoded subdocument responses
    attr_accessor :encoded

    # @return [Integer, nil] index of first operation entry that generated an error
    attr_accessor :first_error_index

    private

    # @return [Proc] The default transcoder which should be used
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
end
