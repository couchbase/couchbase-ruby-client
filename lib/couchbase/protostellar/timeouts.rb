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

require_relative "timeout_defaults"

module Couchbase
  module Protostellar
    class Timeouts
      attr_reader :key_value_timeout
      attr_reader :view_timeout
      attr_reader :query_timeout
      attr_reader :analytics_timeout
      attr_reader :search_timeout
      attr_reader :management_timeout

      def initialize(
        key_value_timeout: nil,
        view_timeout: nil,
        query_timeout: nil,
        analytics_timeout: nil,
        search_timeout: nil,
        management_timeout: nil
      )
        @key_value_timeout = key_value_timeout.nil? ? TimeoutDefaults::KEY_VALUE : key_value_timeout
        @view_timeout = view_timeout.nil? ? TimeoutDefaults::VIEW : view_timeout
        @query_timeout = query_timeout.nil? ? TimeoutDefaults::QUERY : query_timeout
        @analytics_timeout = analytics_timeout.nil? ? TimeoutDefaults::ANALYTICS : analytics_timeout
        @search_timeout = search_timeout.nil? ? TimeoutDefaults::SEARCH : search_timeout
        @management_timeout = management_timeout.nil? ? TimeoutDefaults::MANAGEMENT : management_timeout
      end

      def timeout_for_service(service)
        case service
        when :analytics
          @analytics_timeout
        when :kv
          @key_value_timeout
        when :query
          @query_timeout
        when :search
          @search_timeout
        when :view
          @view_timeout
        when :bucket_admin, :collection_admin
          @management_timeout
        end
      end

      def self.from_cluster_options(options)
        Timeouts.new(
          key_value_timeout: options.key_value_timeout,
          view_timeout: options.view_timeout,
          query_timeout: options.query_timeout,
          analytics_timeout: options.analytics_timeout,
          search_timeout: options.search_timeout,
          management_timeout: options.management_timeout
        )
      end
    end
  end
end
