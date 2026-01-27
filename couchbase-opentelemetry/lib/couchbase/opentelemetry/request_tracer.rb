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

require "opentelemetry-api"

require "couchbase/tracing/request_tracer"
require "couchbase/errors"
require_relative "request_span"

module Couchbase
  module OpenTelemetry
    class RequestTracer < ::Couchbase::Tracing::RequestTracer
      # Initializes a Couchbase OpenTelemetry Request Tracer
      #
      # @param [::OpenTelemetry::Trace::TracerProvider] tracer_provider The OpenTelemetry tracer provider
      #
      # @raise [Couchbase::Error::TracerError] if the tracer cannot be created for any reason
      #
      # @example Initializing a Couchbase OpenTelemetry Request Tracer with an OTLP Exporter
      #   require "opentelemetry-sdk"
      #
      #   # Initialize a trqacer provider
      #   tracer_provider = ::OpenTelemetry::SDK::Trace::TracerProvider.new
      #   tracer_provider.add_span_processor(
      #       ::OpenTelemetry::SDK::Trace::Export::BatchSpanProcessor.new(
      #           exporter: ::OpenTelemetry::Exporter::OTLP::Exporter.new(
      #               endpoint: "https://<hostname>:<port>/v1/traces"
      #          )
      #       )
      #   )
      #   # Initialize the Couchbase OpenTelemetry Request Tracer
      #   tracer = Couchbase::OpenTelemetry::RequestTracer.new(tracer_provider)
      #
      #   # Set the tracer in the cluster options
      #   options = Couchbase::Options::Cluster.new(
      #      authenticator: Couchbase::PasswordAuthenticator.new("Administrator", "password")
      #      tracer: tracer
      #   )
      #
      # @see https://www.rubydoc.info/gems/opentelemetry-sdk/OpenTelemetry/SDK/Trace/TracerProvider
      #  <tt>opentelemetry-sdk</tt> API Reference
      def initialize(tracer_provider)
        super()
        begin
          @wrapped = tracer_provider.tracer("com.couchbase.client/ruby")
        rescue StandardError => e
          raise Error::TracerError.new("Failed to create OpenTelemetry tracer: #{e.message}", nil, e)
        end
      end

      def request_span(name, parent: nil, start_timestamp: nil)
        parent_context = parent.nil? ? nil : ::OpenTelemetry::Trace.context_with_span(parent.instance_variable_get(:@wrapped))
        RequestSpan.new(
          @wrapped.start_span(
            name,
            with_parent: parent_context,
            start_timestamp: start_timestamp,
            kind: :client,
          ),
        )
      rescue StandardError => e
        raise Error::TracerError.new("Failed to create OpenTelemetry span: #{e.message}", nil, e)
      end
    end
  end
end
