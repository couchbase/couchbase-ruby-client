# frozen_string_literal: true

#  Copyright 2025-Present Couchbase, Inc.
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

require "couchbase/tracing/request_span"
require "couchbase/tracing/request_tracer"

require_relative "observability_constants"

module Couchbase
  module Observability
    class ClusterLabelListener
      attr_reader :cluster_name
      attr_reader :cluster_uuid
    end

    class Wrapper
      attr_accessor :tracer
      attr_accessor :meter

      def initialize
        yield self if block_given?
      end

      def record_operation(op_name, parent_span, receiver, service = nil)
        handler = Handler.new(op_name, parent_span, receiver, @tracer, @meter)
        handler.add_service(service) unless service.nil?
        begin
          res = yield(handler)
        ensure
          handler.finish
        end
        res
      end

      def close
        @tracer&.close
      end
    end

    class Handler
      attr_reader :op_span

      def initialize(op_name, parent_span, receiver, tracer, meter)
        @tracer = tracer
        @meter = meter
        @op_span = create_span(op_name, parent_span)
        add_receiver_attributes(receiver)
      end

      def with_request_encoding_span
        span = create_span(Observability::STEP_REQUEST_ENCODING, @op_span)
        begin
          res = yield
        ensure
          span.finish
        end
        res
      end

      def add_service(service)
        service_str =
          case service
          when :kv
            ATTR_VALUE_SERVICE_KV
          when :query
            ATTR_VALUE_SERVICE_QUERY
          when :analytics
            ATTR_VALUE_SERVICE_ANALYTICS
          when :search
            ATTR_VALUE_SERVICE_SEARCH
          when :management
            ATTR_VALUE_SERVICE_MANAGEMENT
          when :views
            ATTR_VALUE_SERVICE_VIEWS
          end
        @op_span.set_attribute(ATTR_SERVICE, service_str) unless service_str.nil?
      end

      def add_bucket_name(name)
        @op_span.set_attribute(ATTR_BUCKET_NAME, name)
      end

      def add_scope_name(name)
        @op_span.set_attribute(ATTR_SCOPE_NAME, name)
      end

      def add_collection_name(name)
        @op_span.set_attribute(ATTR_COLLECTION_NAME, name)
      end

      def add_durability_level(level)
        durability_str =
          case level
          when :majority
            ATTR_VALUE_DURABILITY_MAJORITY
          when :majority_and_persist_to_active
            ATTR_VALUE_DURABILITY_MAJORITY_AND_PERSIST_TO_ACTIVE
          when :persist_to_majority
            ATTR_VALUE_DURABILITY_PERSIST_TO_MAJORITY
          end
        @op_span.set_attribute(ATTR_DURABILITY, durability_str) unless durability_str.nil?
      end

      def add_retries(retries)
        return unless retries.positive?

        @op_span.set_attribute(ATTR_RETRIES, retries.to_i)
      end

      def add_query_statement(statement, options)
        pos_params = options.instance_variable_get(:@positional_parameters)
        named_params = options.instance_variable_get(:@named_parameters)

        # The statement attribute is added only if positional or named parameters are in use.
        return if pos_params.nil? || named_params.nil? || pos_params.empty? || named_params.empty?

        @op_span.set_attribute(ATTR_QUERY_STATEMENT, statement)
      end

      def add_spans_from_backend(backend_spans)
        retries = -1
        backend_spans.each do |backend_span|
          retries += 1 if backend_span[:name] == STEP_DISPATCH_TO_SERVER
          add_backend_span(backend_span, @op_span)
        end

        @op_span.set_attribute(ATTR_RETRIES, retries) if retries.positive?
      end

      def finish
        @op_span.finish
      end

      private

      def add_backend_span(backend_span, parent)
        span = @tracer.request_span(
          backend_span[:name],
          parent: parent,
          start_timestamp: convert_backend_timestamp(backend_span[:start_timestamp]),
        )
        backend_span[:attributes].each do |k, v|
          span.set_attribute(k, v)
        end
        if backend_span.key?(:children)
          backend_span[:children].each do |c|
            add_backend_span(c, span)
          end
        end
        span.finish(end_timestamp: convert_backend_timestamp(backend_span[:end_timestamp]))
      end

      def convert_backend_timestamp(backend_timestamp)
        Time.at(backend_timestamp / (10**6), backend_timestamp % (10**6))
      end

      def create_span(name, parent, start_timestamp: nil)
        span = @tracer.request_span(name, parent: parent, start_timestamp: start_timestamp)
        span.set_attribute(ATTR_SYSTEM_NAME, ATTR_VALUE_SYSTEM_NAME)
        span
      end

      def add_receiver_attributes(receiver)
        if receiver.instance_variable_defined?(:@collection)
          collection = receiver.instance_variable_get(:@collection)
          add_bucket_name(collection.bucket_name)
          add_scope_name(collection.scope_name)
          add_collection_name(collection.name)
          return
        end

        if receiver.instance_variable_defined?(:@bucket_name)
          add_bucket_name(receiver.instance_variable_get(:@bucket_name))
        elsif receiver.instance_variable_defined?(:@name)
          add_bucket_name(receiver.instance_variable_get(:@name))
          return
        end

        if receiver.instance_variable_defined?(:@scope_name)
          add_scope_name(receiver.instance_variable_get(:@scope_name))
        elsif receiver.instance_variable_defined?(:@name)
          add_scope_name(receiver.instance_variable_get(:@name))
          return
        end

        if receiver.instance_variable_defined?(:@collection_name)
          add_collection_name(receiver.instance_variable_get(:@collection_name))
        elsif receiver.instance_variable_defined?(:@name)
          add_collection_name(receiver.instance_variable_get(:@name))
        end
      end
    end
  end
end
