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
    # Common exceptions

    class RequestCanceled < StandardError
    end

    class InvalidArgumentError < ArgumentError
    end

    class ServiceNotAvailable < StandardError
    end

    class InternalServerFailure < StandardError
    end

    class AuthenticationFailure < StandardError
    end

    class TemporaryFailure < StandardError
    end

    class ParsingFailure < StandardError
    end

    class CasMismatch < StandardError
    end

    class BucketNotFound < StandardError
    end

    class CollectionNotFound < StandardError
    end

    class ScopeNotFound < StandardError
    end

    class IndexNotFound < StandardError
    end

    class IndexExists < StandardError
    end

    class EncodingFailure < StandardError
    end

    class DecodingFailure < StandardError
    end

    class UnsupportedOperation < StandardError
    end

    class AmbiguousTimeout < StandardError
    end

    class UnambiguousTimeout < StandardError
    end

    class FeatureNotAvailable < StandardError
    end

    # KeyValue exceptions

    class DocumentNotFound < StandardError
    end

    class DocumentIrretrievable < StandardError
    end

    class DocumentLocked < StandardError
    end

    class ValueTooLarge < StandardError
    end

    class DocumentExists < StandardError
    end

    class DurabilityLevelNotAvailable < StandardError
    end

    class DurabilityImpossible < StandardError
    end

    class DurabilityAmbiguous < StandardError
    end

    class DurableWriteInProgress < StandardError
    end

    class DurableWriteReCommitInProgress < StandardError
    end

    class PathNotFound < StandardError
    end

    class PathMismatch < StandardError
    end

    class PathInvalid < StandardError
    end

    class PathTooDeep < StandardError
    end

    class PathTooBig < StandardError
    end

    class ValueTooDeep < StandardError
    end

    class ValueInvalid < StandardError
    end

    class DocumentNotJson < StandardError
    end

    class NumberTooBig < StandardError
    end

    class DeltaInvalid < StandardError
    end

    class PathExists < StandardError
    end

    class XattrUnknownMacro < StandardError
    end

    class XattrInvalidKeyCombo < StandardError
    end

    class XattrUnknownVirtualAttribute < StandardError
    end

    class XattrCannotModifyVirtualAttribute < StandardError
    end

    # Query exceptions

    class PlanningFailure < StandardError
    end

    class IndexFailure < StandardError
    end

    class PreparedStatementFailure < StandardError
    end


    # Analytics exceptions

    class CompilationFailure < StandardError
    end

    class JobQueueFull < StandardError
    end

    class DatasetNotFound < StandardError
    end

    class DatasetExists < StandardError
    end

    class DataverseExists < StandardError
    end

    class DataverseNotFound < StandardError
    end

    class LinkNotFound < StandardError
    end

    # View exceptions

    class DesignDocumentNotFound < StandardError
    end

    class ViewNotFound < StandardError
    end

    # Management exceptions

    class CollectionExists < StandardError
    end

    class ScopeExists < StandardError
    end

    class UserExists < StandardError
    end

    class BucketExists < StandardError
    end

    class BucketNotFlushable < StandardError
    end

    class GroupNotFound < StandardError
    end

    class UserNotFound < StandardError
    end

    # Library-specific exceptions

    class BackendError < StandardError
    end
  end
end
