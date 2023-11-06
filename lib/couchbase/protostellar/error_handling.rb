# frozen_string_literal: true

#  Copyright 2023. Couchbase, Inc.
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
require "google/rpc/error_details_pb"
require "google/rpc/code_pb"

require_relative "request_behaviour"
require_relative "retry/orchestrator"
require_relative "retry/reason"

module Couchbase
  module Protostellar
    module ErrorHandling
      TYPE_URL_PRECONDITION_FAILURE = "type.googleapis.com/google.rpc.PreconditionFailure"
      TYPE_URL_RESOURCE_INFO = "type.googleapis.com/google.rpc.ResourceInfo"
      TYPE_URL_ERROR_INFO = "type.googleapis.com/google.rpc.ErrorInfo"
      TYPE_URL_BAD_REQUEST = "type.googleapis.com/google.rpc.BadRequest"

      def self.convert_rpc_status(rpc_status, request)
        handle_rpc_status(rpc_status, request, set_context: false).error
      end

      def self.handle_grpc_error(grpc_error, request, set_context: true)
        if set_context
          request.context = {} if request.context.nil?

          request.context[:grpc_error] = {
            type: grpc_error.class,
            code: grpc_error.code,
          }
        end

        rpc_status = grpc_error.to_rpc_status
        if rpc_status.nil?
          # Binary RPC status not provided, create an RPC status with the code and message from the error
          rpc_status = Google::Rpc::Status.new(code: grpc_error.code, message: grpc_error.details)
        end

        handle_rpc_status(rpc_status, request, set_context: set_context) unless rpc_status.nil?
      end

      def self.populate_context(rpc_status, detail_block, request)
        request.context[:server] = rpc_status.message

        unless detail_block[:precondition_failure].nil?
          request.context[:precondition_violation] = detail_block[:precondition_failure].violations[0].type
        end

        unless detail_block[:resource_info].nil?
          request.context[:resource_type] = detail_block[:resource_info].resource_type
          request.context[:resource_name] = detail_block[:resource_info].resource_name unless detail_block[:resource_info].resource_name.empty?
        end

        unless detail_block[:error_info].nil?
          request.context[:reason] = detail_block[:error_info].reason
        end
      end

      def self.handle_rpc_status(rpc_status, request, set_context: true)
        detail_block = {}
        message = rpc_status.message

        # Decode the detail block
        rpc_status.details&.each do |d|
          type_url = d.type_url

          if type_url == TYPE_URL_PRECONDITION_FAILURE
            precondition_failure = Google::Rpc::PreconditionFailure.decode(d.value)
            detail_block[:precondition_failure] = precondition_failure
          end

          if type_url == TYPE_URL_RESOURCE_INFO
            resource_info = Google::Rpc::ResourceInfo.decode(d.value)
            detail_block[:resource_info] = resource_info
          end

          if type_url == TYPE_URL_ERROR_INFO
            error_info = Google::Rpc::ErrorInfo.decode(d.value)
            detail_block[:error_info] = error_info
          end

          if type_url == TYPE_URL_BAD_REQUEST
            bad_request = Google::Rpc::BadRequest.decode(d.value)
            detail_block[:bad_request] = bad_request
          end
        end

        populate_context(rpc_status, detail_block, request) if set_context

        behaviour =
          case rpc_status.code
          when Google::Rpc::Code::NOT_FOUND
            case detail_block[:resource_info].resource_type
            when "document"
              RequestBehaviour.fail(Couchbase::Error::DocumentNotFound.new(message, request.error_context))
            when "queryindex", "searchindex"
              RequestBehaviour.fail(Couchbase::Error::IndexNotFound.new(message, request.error_context))
            when "bucket"
              RequestBehaviour.fail(Couchbase::Error::BucketNotFound.new(message, request.error_context))
            when "scope"
              RequestBehaviour.fail(Couchbase::Error::ScopeNotFound.new(message, request.error_context))
            when "collection"
              RequestBehaviour.fail(Couchbase::Error::CollectionNotFound.new(message, request.error_context))
            when "path"
              RequestBehaviour.fail(Couchbase::Error::PathNotFound.new(message, request.error_context))
            else
              nil
            end

          when Google::Rpc::Code::ALREADY_EXISTS
            case detail_block[:resource_info].resource_type
            when "document"
              RequestBehaviour.fail(Couchbase::Error::DocumentExists.new(message, request.error_context))
            when "queryindex", "searchindex"
              RequestBehaviour.fail(Couchbase::Error::IndexExists.new(message, request.error_context))
            when "bucket"
              RequestBehaviour.fail(Couchbase::Error::BucketExists.new(message, request.error_context))
            when "scope"
              RequestBehaviour.fail(Couchbase::Error::ScopeExists.new(message, request.error_context))
            when "collection"
              RequestBehaviour.fail(Couchbase::Error::CollectionExists.new(message, request.error_context))
            when "path"
              RequestBehaviour.fail(Couchbase::Error::PathExists.new(message, request.error_context))
            else
              nil
            end

          when Google::Rpc::Code::INVALID_ARGUMENT
            RequestBehaviour.fail(Couchbase::Error::InvalidArgument.new(message, request.error_context))

          when Google::Rpc::Code::ABORTED
            case detail_block[:error_info].reason
            when "CAS_MISMATCH"
              RequestBehaviour.fail(Couchbase::Error::CasMismatch.new(message, request.error_context))
            else
              nil
            end

          when Google::Rpc::Code::FAILED_PRECONDITION
            case detail_block[:precondition_failure].violations[0].type
            when "LOCKED"
              Retry::Orchestrator.maybe_retry(request, Retry::Reason::KV_LOCKED)
            when "DOC_TOO_DEEP"
              RequestBehaviour.fail(Couchbase::Error::PathTooDeep.new(message, request.error_context))
            when "DOC_NOT_JSON"
              RequestBehaviour.fail(Couchbase::Error::DocumentNotJson.new(message, request.error_context))
            when "PATH_MISMATCH"
              RequestBehaviour.fail(Couchbase::Error::PathMismatch.new(message, request.error_context))
            when "WOULD_INVALIDATE_JSON"
              RequestBehaviour.fail(Couchbase::Error::ValueInvalid.new(message, request.error_context))
            when "PATH_VALUE_OUT_OF_RANGE"
              RequestBehaviour.fail(Couchbase::Error::NumberTooBig.new(message, request.error_context))
            when "VALUE_TOO_LARGE"
              RequestBehaviour.fail(Couchbase::Error::ValueTooLarge.new(message, request.error_context))
            else
              nil
            end

          when Google::Rpc::Code::UNIMPLEMENTED
            RequestBehaviour.fail(Couchbase::Error::FeatureNotAvailable.new(message, request.error_context))

          when Google::Rpc::Code::UNAUTHENTICATED
            RequestBehaviour.fail(Couchbase::Error::AuthenticationFailure.new(message, request.error_context))

          when Google::Rpc::Code::PERMISSION_DENIED
            case detail_block[:resource_info].resource_type
            when "anything_but_user"
              RequestBehaviour.fail(Couchbase::Error::PermissionDenied.new(message, request.error_context))
            else
              nil
            end

          when Google::Rpc::Code::CANCELLED
            RequestBehaviour.fail(Couchbase::Error::RequestCanceled.new(message, request.error_context))

          when Google::Rpc::Code::DEADLINE_EXCEEDED
            if request.idempotent
              RequestBehaviour.fail(Couchbase::Error::UnambiguousTimeout.new(message, request.error_context))
            else
              RequestBehaviour.fail(Couchbase::Error::AmbiguousTimeout.new(message, request.error_context))
            end

          when Google::Rpc::Code::INTERNAL
            RequestBehaviour.fail(Couchbase::Error::InternalServerFailure.new(message, request.error_context))

          when Google::Rpc::Code::UNAVAILABLE
            Retry::Orchestrator.maybe_retry(request, Retry::Reason::SOCKET_NOT_AVAILABLE)

          when Google::Rpc::Code::OK
            RequestBehaviour.success
          end

        if behaviour.nil?
          RequestBehaviour.fail(Couchbase::Error::CouchbaseError.new(message, request.error_context))
        else
          behaviour
        end
      end
    end
  end
end
