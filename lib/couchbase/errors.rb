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

require "json"

module Couchbase
  # This namespace contains all error types that the library might raise.
  module Error
    class CouchbaseError < StandardError
      # @return [Hash] attributes associated with the error
      attr_reader :context

      def to_s
        defined?(@context) ? "#{super}, context=#{JSON.generate(@context)}" : super
      end
    end

    class InvalidArgument < ArgumentError
      # @return [Hash] attributes associated with the error
      attr_reader :context

      def to_s
        defined?(@context) ? "#{super}, context=#{JSON.generate(@context)}" : super
      end
    end

    # Common exceptions

    class RequestCanceled < CouchbaseError
    end

    class ServiceNotAvailable < CouchbaseError
    end

    # Indicates an operation failed because there has been an internal error in the server.
    class InternalServerFailure < CouchbaseError
    end

    # Every exception that has to do with authentication problems should either instantiate or subclass from this type.
    class AuthenticationFailure < CouchbaseError
    end

    class TemporaryFailure < CouchbaseError
    end

    # Indicates an operation failed because parsing of the input returned with an error.
    class ParsingFailure < CouchbaseError
    end

    # Indicates an optimistic locking failure.
    #
    # The operation failed because the specified compare and swap (CAS) value differs from the document's actual CAS
    # value. This means the document was modified since the original CAS value was acquired.
    #
    # The application should usually respond by fetching a fresh version of the document and repeating the failed
    # operation.
    class CasMismatch < CouchbaseError
    end

    class BucketNotFound < CouchbaseError
    end

    class CollectionNotFound < CouchbaseError
    end

    class ScopeNotFound < CouchbaseError
    end

    class IndexNotFound < CouchbaseError
    end

    class IndexExists < CouchbaseError
    end

    # Raised when provided content could not be successfully encoded.
    class EncodingFailure < CouchbaseError
    end

    # Raised when provided content could not be successfully decoded.
    class DecodingFailure < CouchbaseError
    end

    class UnsupportedOperation < CouchbaseError
    end

    # The {Timeout} signals that an operation timed out before it could be completed.
    #
    # It is important to understand that the timeout itself is always just the effect an underlying cause, never the
    # issue itself. The root cause might not even be on the application side, also the network and server need to be
    # taken into account.
    #
    # Right now the SDK can throw two different implementations of this class:
    #
    # {AmbiguousTimeout}::
    #   The operation might have caused a side effect on the server and should not be retried without
    #   actions and checks.
    #
    # {UnambiguousTimeout}::
    #   The operation has not caused a side effect on the server and is safe to retry. This is always the case for
    #   idempotent operations. For non-idempotent operations it depends on the state the operation was in at the time of
    #   cancellation.
    class Timeout < CouchbaseError
    end

    # This is a special case of the timeout exception, signaling that the timeout happened with an ambiguous cause.
    class AmbiguousTimeout < Timeout
    end

    # This is a special case of the timeout exception, signaling that the timeout happened with no ambiguous cause.
    class UnambiguousTimeout < Timeout
    end

    # Exception which states that the feature is not available.
    class FeatureNotAvailable < CouchbaseError
    end

    # KeyValue exceptions

    # Indicates an operation failed because the key does not exist.
    class DocumentNotFound < CouchbaseError
    end

    # Indicates an operation completed but no successful document was retrievable.
    class DocumentIrretrievable < CouchbaseError
    end

    # Thrown when the server reports a temporary failure that is very likely to be lock-related (like an already locked
    # key or a bad cas used for unlock).
    #
    # See https://issues.couchbase.com/browse/MB-13087 for an explanation of why this is only _likely_ to be
    # lock-related.
    class DocumentLocked < CouchbaseError
    end

    # Thrown when the request is too big for some reason.
    class ValueTooLarge < CouchbaseError
    end

    # Indicates an operation failed because the key already exists.
    class DocumentExists < CouchbaseError
    end

    # This exception is raised when a durability level has been requested that is not available on the server.
    class DurabilityLevelNotAvailable < CouchbaseError
    end

    # The given durability requirements are currently impossible to achieve, as not enough configured replicas are
    # currently available.
    class DurabilityImpossible < CouchbaseError
    end

    # The synchronous replication durability work can return an ambiguous error (or we timeout waiting for the response,
    # which is effectively the same).  Here we know the change is on a majority of replicas, or it's on none.
    class DurabilityAmbiguous < CouchbaseError
    end

    # Returned if an attempt is made to mutate a key which already has a durable write pending.
    class DurableWriteInProgress < CouchbaseError
    end

    # The requested key has a SyncWrite which is being re-committed.
    class DurableWriteReCommitInProgress < CouchbaseError
    end

    # Subdocument exception thrown when a path does not exist in the document. The exact meaning of path existence
    # depends on the operation and inputs.
    class PathNotFound < CouchbaseError
    end

    # Subdocument exception thrown when the path structure conflicts with the document structure (for example, if a
    # path mentions foo.bar[0].baz, but foo.bar is actually a JSON object).
    class PathMismatch < CouchbaseError
    end

    # Subdocument exception thrown when path has a syntax error, or path syntax is incorrect for the operation (for
    # example, if operation requires an array index).
    class PathInvalid < CouchbaseError
    end

    # Subdocument exception thrown when path is too deep to parse. Depth of a path is determined by how many components
    # (or levels) it contains.
    #
    # The current limitation is there to ensure a single parse does not consume too much memory (overloading the
    # server). This error is similar to other TooDeep errors, which all relate to various validation stages to ensure
    # the server does not consume too much memory when parsing a single document.
    class PathTooDeep < CouchbaseError
    end

    class PathTooBig < CouchbaseError
    end

    # Subdocument exception thrown when proposed value would make the document too deep to parse.
    #
    # The current limitation is there to ensure a single parse does not consume too much memory (overloading the
    # server). This error is similar to other TooDeep errors, which all relate to various validation stages to ensure
    # the server does not consume too much memory when parsing a single document.
    class ValueTooDeep < CouchbaseError
    end

    # Subdocument exception thrown when the provided value cannot be inserted at the given path.
    #
    # It is actually thrown when the delta in an counter operation is valid, but applying that delta would
    # result in an out-of-range number (server interprets numbers as 64-bit integers).
    class ValueInvalid < CouchbaseError
    end

    # Subdocument exception thrown when the targeted enclosing document itself is not JSON.
    class DocumentNotJson < CouchbaseError
    end

    # Subdocument exception thrown when existing number value in document is too big.
    #
    # The value is interpreted as 64 bit on the server side.
    class NumberTooBig < CouchbaseError
    end

    # Subdocument exception thrown when the delta in an arithmetic operation (eg counter) is invalid. In this SDK, this
    # is equivalent to saying that the delta is zero.
    #
    # Note that the server also returns the corresponding error code when the delta value itself is too big, or not a
    # number, but since the SDK enforces deltas to be of type long, these cases shouldn't come up.
    class DeltaInvalid < CouchbaseError
    end

    # Subdocument exception thrown when a path already exists and it shouldn't
    class PathExists < CouchbaseError
    end

    # Subdocument exception thrown when a macro has been requested which is not recognised by the server.
    class XattrUnknownMacro < CouchbaseError
    end

    # Subdocument exception thrown when more than one xattr key has been requested.
    class XattrInvalidKeyCombo < CouchbaseError
    end

    # Subdocument exception thrown when a virtual attribute has been requested which is not recognised by the server.
    class XattrUnknownVirtualAttribute < CouchbaseError
    end

    # Subdocument exception thrown when the virtual attribute cannot be modified.
    class XattrCannotModifyVirtualAttribute < CouchbaseError
    end

    # Query exceptions

    # Indicates an operation failed because there has been an issue with the query planner.
    class PlanningFailure < CouchbaseError
    end

    # Indicates an operation failed because there has been an issue with the query planner or similar.
    class IndexFailure < CouchbaseError
    end

    # Indicates an operation failed because there has been an issue with query prepared statements.
    class PreparedStatementFailure < CouchbaseError
    end

    # Analytics exceptions

    # The analytics query failed to compile.
    class CompilationFailure < CouchbaseError
    end

    # Indicates the analytics server job queue is full
    class JobQueueFull < CouchbaseError
    end

    # The queried dataset is not found on the server.
    class DatasetNotFound < CouchbaseError
    end

    class DatasetExists < CouchbaseError
    end

    class DataverseExists < CouchbaseError
    end

    class DataverseNotFound < CouchbaseError
    end

    class LinkNotFound < CouchbaseError
    end

    class LinkExists < CouchbaseError
    end

    # Search exceptions

    class IndexNotReady < CouchbaseError
    end

    class ConsistencyMismatch < CouchbaseError
    end

    # View exceptions

    class DesignDocumentNotFound < CouchbaseError
    end

    # The queried view is not found on the server
    class ViewNotFound < CouchbaseError
    end

    # Management exceptions

    class CollectionExists < CouchbaseError
    end

    class ScopeExists < CouchbaseError
    end

    class UserExists < CouchbaseError
    end

    class BucketExists < CouchbaseError
    end

    class BucketNotFlushable < CouchbaseError
    end

    class GroupNotFound < CouchbaseError
    end

    class UserNotFound < CouchbaseError
    end

    # Library-specific exceptions

    class BackendError < CouchbaseError
    end

    # Environment name string cannot be determined
    class NoEnvironment < CouchbaseError
    end
  end
end
