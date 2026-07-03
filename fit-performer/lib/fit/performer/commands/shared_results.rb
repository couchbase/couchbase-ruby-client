# frozen_string_literal: true

#  Copyright 2026-Present Couchbase, Inc.
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

require 'couchbase/errors'

require 'fit/protocol/sdk.workload_pb'
require 'fit/protocol/run.top_level_pb'
require 'fit/protocol/shared.exceptions_pb'
require 'fit/protocol/shared.content_pb'

module FIT
  module Performer
    module Commands
      class SharedResults
        def self.as_success(initiated:)
          begin
            start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            yield
            end_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          rescue StandardError => e
            return to_exception(error: e, initiated: initiated)
          end
          nanos = ((end_time - start_time) * (10**9)).round
          to_success(elapsed_nanos: nanos, initiated: initiated)
        end

        def self.to_exception(error:, initiated: nil, unwrapped: false)
          name = error.class.to_s
          serialized = error.to_s
          pb_exception =
            if error.is_a?(Couchbase::Error::CouchbaseError) || error.is_a?(Couchbase::Error::InvalidArgument)
              FIT::Protocol::Shared::Exception.new(
                couchbase: FIT::Protocol::Shared::CouchbaseExceptionEx.new(
                  name: name,
                  serialized: serialized,
                  type: ERROR_MAP[error.class],
                ),
              )
            else
              FIT::Protocol::Shared::Exception.new(
                other: FIT::Protocol::Shared::ExceptionOther.new(name: name, serialized: serialized),
              )
            end

          return pb_exception if unwrapped # Returns the protobuf exception object only - used for errors in streams

          sdk_result = FIT::Protocol::SDK::Result.new(exception: pb_exception)
          create_run_result(sdk_result: sdk_result, initiated: initiated)
        end

        def self.to_success(elapsed_nanos:, initiated:)
          sdk_result = FIT::Protocol::SDK::Result.new(success: true)
          create_run_result(sdk_result: sdk_result, initiated: initiated, elapsed_nanos: elapsed_nanos)
        end

        def self.create_run_result(sdk_result:, initiated:, elapsed_nanos: nil)
          kwargs = {
            initiated: initiated,
            sdk: sdk_result,
          }
          kwargs[:elapsedNanos] = elapsed_nanos unless elapsed_nanos.nil?
          FIT::Protocol::Run::Result.new(**kwargs)
        end

        def self.get_content(content:, content_as: nil)
          res_content = FIT::Protocol::Shared::ContentTypes.new
          as = content_as&.as
          case as
          when :as_string
            res_content.content_as_string =
              if content.is_a?(String)
                content
              else
                content.to_json
              end
          when :as_byte_array
            res_content.content_as_bytes =
              if content.is_a?(String)
                content.b
              else
                content.to_json.b
              end
          when :as_json_object, :as_json_array, nil
            res_content.content_as_bytes = content.to_json.b
          when :as_boolean
            res_content.content_as_bool = !content.nil? && content != false
          when :as_integer
            res_content.content_as_int64 = content.to_i
          when :as_floating_point
            res_content.content_as_double = content.to_f
          else
            raise PerformerError, "ContentAs type #{content_as.as} not supported"
          end
          res_content
        end

        ERROR_MAP = {
          Couchbase::Error::CouchbaseError => 0,
          Couchbase::Error::Timeout => 1,
          Couchbase::Error::RequestCanceled => 2,
          Couchbase::Error::InvalidArgument => 3,
          Couchbase::Error::ServiceNotAvailable => 4,
          Couchbase::Error::InternalServerFailure => 5,
          Couchbase::Error::AuthenticationFailure => 6,
          Couchbase::Error::TemporaryFailure => 7,
          Couchbase::Error::ParsingFailure => 8,
          Couchbase::Error::CasMismatch => 9,
          Couchbase::Error::BucketNotFound => 10,
          Couchbase::Error::CollectionNotFound => 11,
          Couchbase::Error::UnsupportedOperation => 12,
          Couchbase::Error::AmbiguousTimeout => 13,
          Couchbase::Error::UnambiguousTimeout => 14,
          Couchbase::Error::FeatureNotAvailable => 15,
          Couchbase::Error::ScopeNotFound => 16,
          Couchbase::Error::IndexNotFound => 17,
          Couchbase::Error::IndexExists => 18,
          Couchbase::Error::EncodingFailure => 19,
          Couchbase::Error::DecodingFailure => 20,
          Couchbase::Error::RateLimited => 21,
          Couchbase::Error::QuotaLimited => 22,
          Couchbase::Error::DocumentNotFound => 101,
          Couchbase::Error::DocumentIrretrievable => 102,
          Couchbase::Error::DocumentLocked => 103,
          Couchbase::Error::ValueTooLarge => 104,
          Couchbase::Error::DocumentExists => 105,
          Couchbase::Error::DurabilityLevelNotAvailable => 107,
          Couchbase::Error::DurabilityImpossible => 108,
          Couchbase::Error::DurabilityAmbiguous => 109,
          Couchbase::Error::DurableWriteInProgress => 110,
          Couchbase::Error::DurableWriteReCommitInProgress => 111,
          Couchbase::Error::PathNotFound => 113,
          Couchbase::Error::PathMismatch => 114,
          Couchbase::Error::PathInvalid => 115,
          Couchbase::Error::PathTooBig => 116,
          Couchbase::Error::PathTooDeep => 117,
          Couchbase::Error::ValueTooDeep => 118,
          Couchbase::Error::ValueInvalid => 119,
          Couchbase::Error::DocumentNotJson => 120,
          Couchbase::Error::NumberTooBig => 121,
          Couchbase::Error::DeltaInvalid => 122,
          Couchbase::Error::PathExists => 123,
          Couchbase::Error::XattrUnknownMacro => 124,
          Couchbase::Error::XattrInvalidKeyCombo => 126,
          Couchbase::Error::XattrUnknownVirtualAttribute => 127,
          Couchbase::Error::XattrCannotModifyVirtualAttribute => 128,
          Couchbase::Error::XattrNoAccess => 130,
          Couchbase::Error::DocumentNotLocked => 131,
          Couchbase::Error::PlanningFailure => 201,
          Couchbase::Error::IndexFailure => 202,
          Couchbase::Error::PreparedStatementFailure => 203,
          Couchbase::Error::DmlFailure => 204,
          Couchbase::Error::CompilationFailure => 301,
          Couchbase::Error::JobQueueFull => 302,
          Couchbase::Error::DatasetNotFound => 303,
          Couchbase::Error::DataverseNotFound => 304,
          Couchbase::Error::DatasetExists => 305,
          Couchbase::Error::DataverseExists => 306,
          Couchbase::Error::LinkNotFound => 307,
          Couchbase::Error::ViewNotFound => 501,
          Couchbase::Error::DesignDocumentNotFound => 502,
          Couchbase::Error::CollectionExists => 601,
          Couchbase::Error::ScopeExists => 602,
          Couchbase::Error::UserNotFound => 603,
          Couchbase::Error::GroupNotFound => 604,
          Couchbase::Error::BucketExists => 605,
          Couchbase::Error::UserExists => 606,
          Couchbase::Error::BucketNotFlushable => 607,
        }.freeze
      end
    end
  end
end
