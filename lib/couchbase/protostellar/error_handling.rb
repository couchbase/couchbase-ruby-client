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

module Couchbase
  module Protostellar
    module ErrorHandling
      TYPE_URL_PRECONDITION_FAILURE = "type.googleapis.com/google.rpc.PreconditionFailure"
      TYPE_URL_RESOURCE_INFO = "type.googleapis.com/google.rpc.ResourceInfo"
      TYPE_URL_ERROR_INFO = "type.googleapis.com/google.rpc.ErrorInfo"
      TYPE_URL_BAD_REQUEST = "type.googleapis.com/google.rpc.BadRequest"

      def self.handle_grpc_error(grpc_error, request)
        request.context = {} if request.context.nil?
        detail_block = {}

        rpc_status = grpc_error.to_rpc_status
        request.context[:grpc_error] = {type: grpc_error.class}
        request.context[:grpc_error][:code] = rpc_status.code unless rpc_status.nil?

        # Decode the detail block
        rpc_status&.details&.each do |d|
          type_url = d.type_url

          if type_url == TYPE_URL_PRECONDITION_FAILURE
            precondition_failure = Google::Rpc::PreconditionFailure.decode(d.value)
            detail_block[:precondition_failure] = precondition_failure
            context[:precondition_violations] = precondition_failure.violations.map do |v|
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
          handle_kv_error(grpc_error, detail_block, request)
        when :query
          handle_query_error(grpc_error, detail_block, request)
        else
          handle_generic_error(grpc_error, detail_block, request)
        end
      end

      def self.handle_generic_error(grpc_error, detail_block, request)
        case grpc_error
        when GRPC::DeadlineExceeded
          RequestBehaviour.fail(Couchbase::Error::Timeout.new(nil, request.context))
        when GRPC::Cancelled
          RequestBehaviour.fail(Couchbase::Error::RequestCanceled.new(nil, request.context))
        when GRPC::Aborted
          if detail_block[:error_info].reason == "CAS_MISMATCH" && detail_block[:resource_info].resource_type == "document"
            RequestBehaviour.fail(Couchbase::Error::CasMismatch.new("CAS mismatch", request.context))
          end
        when GRPC::NotFound
          case detail_block[:resource_info].resource_type
          when "collection"
            RequestBehaviour.fail(Couchbase::Error::CollectionNotFound.new("Collection not found", request.context))
          when "bucket"
            RequestBehaviour.fail(Couchbase::Error::BucketNotFound.new("Bucket not found", request.context))
          when "scope"
            RequestBehaviour.fail(Couchbase::Error::ScopeNotFound.new("Scope not found", request.context))
          end
        else
          RequestBehaviour.fail(Couchbase::Error::CouchbaseError.new(nil, request.context))
        end
      end

      def self.handle_kv_error(grpc_error, detail_block, request)
        behaviour =
          case grpc_error
          when GRPC::NotFound
            if detail_block[:resource_info].resource_type == "document"
              RequestBehaviour.fail(Couchbase::Error::DocumentNotFound.new("Document not found", request.context))
            end
          when GRPC::AlreadyExists
            if detail_block[:resource_info].resource_type == "document"
              RequestBehaviour.fail(Couchbase::Error::DocumentExists.new("Document already exists", request.context))
            end
          when GRPC::FailedPrecondition
            if detail_block[:precondition_failure].violations[0].type == "DOCUMENT_LOCKED"
              Retry::Orchestrator.maybe_retry(request, Retry::Reason::KV_LOCKED)
            end
          end
        if behaviour.nil?
          handle_generic_error(grpc_error, detail_block, request)
        else
          behaviour
        end
      end

      def self.handle_query_error(grpc_error, detail_block, request)
        handle_generic_error(grpc_error, detail_block, request)
      end
    end
  end
end
