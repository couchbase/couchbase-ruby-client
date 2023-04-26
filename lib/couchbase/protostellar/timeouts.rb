# frozen_string_literal: true

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
        if service == :analytics
          @analytics_timeout
        elsif service == :kv
          @key_value_timeout
        elsif service == :query
          @query_timeout
        elsif service == :search
          @search_timeout
        elsif service == :view
          @view_timeout
        elsif [:bucket_admin, :collection_admin].include? service
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
