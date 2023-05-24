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

      def self.handle_grpc_error(grpc_error, request)
        request.context = {} if request.context.nil?

        request.context[:grpc_error] = {
          type: grpc_error.class,
          code: grpc_error.code,
        }

        rpc_status = grpc_error.to_rpc_status
        detail_block = {}
        message = grpc_error.message.partition(":").last.split("..").first

        # Decode the detail block
        rpc_status&.details&.each do |d|
          type_url = d.type_url

          if type_url == TYPE_URL_PRECONDITION_FAILURE
            precondition_failure = Google::Rpc::PreconditionFailure.decode(d.value)
            detail_block[:precondition_failure] = precondition_failure
            request.context[:precondition_violations] = precondition_failure.violations.map do |v|
              {
                violation_type: v.type,
                violation_subject: v.subject,
                violation_description: v.description,
              }
            end
          end

          if type_url == TYPE_URL_RESOURCE_INFO
            resource_info = Google::Rpc::ResourceInfo.decode(d.value)
            detail_block[:resource_info] = resource_info
            request.context[:resource_type] = resource_info.resource_type
            request.context[:resource_name] = resource_info.resource_name
          end

          if type_url == TYPE_URL_ERROR_INFO
            error_info = Google::Rpc::ErrorInfo.decode(d.value)
            detail_block[:error_info] = error_info
            request.context[:error_reason] = error_info.reason
            request.context[:error_domain] = error_info.domain
            request.context[:error_metadata] = error_info.metadata.to_h
          end

          if type_url == TYPE_URL_BAD_REQUEST
            bad_request = Google::Rpc::BadRequest.decode(d.value)
            detail_block[:bad_request] = bad_request
            request.context[:field_violations] = bad_request.field_violations.map do |v|
              {
                field: v.field,
                description: v.description,
              }
            end
          end
        end

        case request.service
        when :kv
          handle_kv_error(grpc_error, detail_block, message, request)
        when :query
          handle_query_error(grpc_error, detail_block, message, request)
        else
          handle_generic_error(grpc_error, detail_block, message, request)
        end
      end

      def self.handle_generic_error(grpc_error, detail_block, message, request)
        case grpc_error
        when GRPC::DeadlineExceeded
          if request.idempotent
            RequestBehaviour.fail(Couchbase::Error::UnambiguousTimeout.new(message, request.error_context))
          else
            RequestBehaviour.fail(Couchbase::Error::AmbiguousTimeout.new(message, request.error_context))
          end

        when GRPC::Cancelled
          RequestBehaviour.fail(Couchbase::Error::RequestCanceled.new(message, request.error_context))

        when GRPC::FailedPrecondition
          case detail_block[:precondition_failure].violations[0].type
          when "CAS"
            RequestBehaviour.fail(Couchbase::Error::CasMismatch.new(message, request.context))
          end

        when GRPC::NotFound
          case detail_block[:resource_info].resource_type
          when "bucket"
            RequestBehaviour.fail(Couchbase::Error::BucketNotFound.new(message, request.error_context))
          when "scope"
            RequestBehaviour.fail(Couchbase::Error::ScopeNotFound.new(message, request.error_context))
          when "collection"
            RequestBehaviour.fail(Couchbase::Error::CollectionNotFound.new(message, request.error_context))
          end

        when GRPC::AlreadyExists
          case detail_block[:resource_info].resource_type
          when "bucket"
            RequestBehaviour.fail(Couchbase::Error::BucketExists.new(message, request.error_context))
          when "scope"
            RequestBehaviour.fail(Couchbase::Error::ScopeExists.new(message, request.error_context))
          when "collection"
            RequestBehaviour.fail(Couchbase::Error::CollectionExists.new(message, request.error_context))
          end

        when GRPC::PermissionDenied, GRPC::Unauthenticated
          RequestBehaviour.fail(Couchbase::Error::AuthenticationFailure.new(message, request.error_context))

        when GRPC::InvalidArgument
          RequestBehaviour.fail(Couchbase::Error::InvalidArgument.new(message, request.error_context))

        else
          RequestBehaviour.fail(Couchbase::Error::CouchbaseError.new(message, request.error_context))
        end
      end

      def self.handle_kv_error(grpc_error, detail_block, message, request)
        behaviour =
          case grpc_error
          when GRPC::NotFound
            case detail_block[:resource_info].resource_type
            when "document"
              RequestBehaviour.fail(Couchbase::Error::DocumentNotFound.new(message, request.error_context))
            when "path"
              RequestBehaviour.fail(Couchbase::Error::PathNotFound.new(message, request.error_context))
            end

          when GRPC::AlreadyExists
            case detail_block[:resource_info].resource_type
            when "document"
              RequestBehaviour.fail(Couchbase::Error::DocumentExists.new(message, request.error_context))
            when "path"
              RequestBehaviour.fail(Couchbase::Error::PathExists.new(message, request.error_context))
            end

          when GRPC::FailedPrecondition
            case detail_block[:precondition_failure].violations[0].type
            when "LOCKED"
              Retry::Orchestrator.maybe_retry(request, Retry::Reason::KV_LOCKED)
            when "VALUE_TOO_LARGE"
              RequestBehaviour.fail(Couchbase::Error::ValueTooLarge.new(message, request.error_context))
            when "DURABILITY_IMPOSSIBLE"
              RequestBehaviour.fail(Couchbase::Error::DurabilityImpossible.new(message, request.error_context))
            when "PATH_MISMATCH"
              RequestBehaviour.fail(Couchbase::Error::PathMismatch.new(message, request.error_context))
            when "VALUE_TOO_DEEP"
              RequestBehaviour.fail(Couchbase::Error::ValueTooDeep.new(message, request.error_context))
            when "DOC_TOO_DEEP"
              RequestBehaviour.fail(Couchbase::Error::PathTooDeep.new(message, request.error_context))
            when "WOULD_INVALIDATE_JSON"
              RequestBehaviour.fail(Couchbase::Error::ValueInvalid.new(message, request.error_context))
            when "DOC_NOT_JSON"
              RequestBehaviour.fail(Couchbase::Error::DocumentNotJson.new(message, request.error_context))
            when "PATH_VALUE_OUT_OF_RANGE"
              RequestBehaviour.fail(Couchbase::Error::NumberTooBig.new(message, request.error_context))
            end
          end

        if behaviour.nil?
          handle_generic_error(grpc_error, detail_block, message, request)
        else
          behaviour
        end
      end

      def self.handle_query_error(grpc_error, detail_block, message, request)
        handle_generic_error(grpc_error, detail_block, message, request)
      end
    end
  end
end
