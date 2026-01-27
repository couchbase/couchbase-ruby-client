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

require "couchbase/tracing/request_span"

require "opentelemetry-api"

module Couchbase
  module OpenTelemetry
    class RequestSpan < ::Couchbase::Tracing::RequestSpan
      def initialize(span)
        super()

        @wrapped = span
      end

      def set_attribute(key, value)
        @wrapped.set_attribute(key, value)
      end

      def status=(status_code)
        @wrapped.status = if status_code == :ok
                            ::OpenTelemetry::Trace::Status.ok
                          elsif status_code == :error
                            ::OpenTelemetry::Trace::Status.error
                          else
                            ::OpenTelemetry::Trace::Status.unset
                          end
      end

      def finish(end_timestamp: nil)
        @wrapped.finish(end_timestamp: end_timestamp)
      end
    end
  end
end
