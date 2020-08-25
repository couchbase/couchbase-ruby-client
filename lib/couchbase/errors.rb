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
  module Error
    class CouchbaseError < StandardError
    end

    # Common exceptions

    class RequestCanceled < CouchbaseError
    end

    class InvalidArgument < ArgumentError
    end

    class ServiceNotAvailable < CouchbaseError
    end

    class InternalServerFailure < CouchbaseError
    end

    class AuthenticationFailure < CouchbaseError
    end

    class TemporaryFailure < CouchbaseError
    end

    class ParsingFailure < CouchbaseError
    end

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

    class EncodingFailure < CouchbaseError
    end

    class DecodingFailure < CouchbaseError
    end

    class UnsupportedOperation < CouchbaseError
    end

    class AmbiguousTimeout < CouchbaseError
    end

    class UnambiguousTimeout < CouchbaseError
    end

    class FeatureNotAvailable < CouchbaseError
    end

    # KeyValue exceptions

    class DocumentNotFound < CouchbaseError
    end

    class DocumentIrretrievable < CouchbaseError
    end

    class DocumentLocked < CouchbaseError
    end

    class ValueTooLarge < CouchbaseError
    end

    class DocumentExists < CouchbaseError
    end

    class DurabilityLevelNotAvailable < CouchbaseError
    end

    class DurabilityImpossible < CouchbaseError
    end

    class DurabilityAmbiguous < CouchbaseError
    end

    class DurableWriteInProgress < CouchbaseError
    end

    class DurableWriteReCommitInProgress < CouchbaseError
    end

    class PathNotFound < CouchbaseError
    end

    class PathMismatch < CouchbaseError
    end

    class PathInvalid < CouchbaseError
    end

    class PathTooDeep < CouchbaseError
    end

    class PathTooBig < CouchbaseError
    end

    class ValueTooDeep < CouchbaseError
    end

    class ValueInvalid < CouchbaseError
    end

    class DocumentNotJson < CouchbaseError
    end

    class NumberTooBig < CouchbaseError
    end

    class DeltaInvalid < CouchbaseError
    end

    class PathExists < CouchbaseError
    end

    class XattrUnknownMacro < CouchbaseError
    end

    class XattrInvalidKeyCombo < CouchbaseError
    end

    class XattrUnknownVirtualAttribute < CouchbaseError
    end

    class XattrCannotModifyVirtualAttribute < CouchbaseError
    end

    # Query exceptions

    class PlanningFailure < CouchbaseError
    end

    class IndexFailure < CouchbaseError
    end

    class PreparedStatementFailure < CouchbaseError
    end

    # Analytics exceptions

    class CompilationFailure < CouchbaseError
    end

    class JobQueueFull < CouchbaseError
    end

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

    # Search exceptions

    class IndexNotReady < CouchbaseError
    end

    # View exceptions

    class DesignDocumentNotFound < CouchbaseError
    end

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
  end
end
