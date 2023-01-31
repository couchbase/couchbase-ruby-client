# frozen_string_literal: true

require "singleton"

require "couchbase/errors"

module Couchbase
  class ClusterRegistry
    include Singleton

    def initialize
      @handlers = {}
    end

    def connect(connection_string, *options)
      @handlers.each do |regexp, cluster_class|
        return cluster_class.connect(connection_string, *options) if regexp.match?(connection_string)
      end
      raise(Error::FeatureNotAvailable, "Connection string '#{connection_string}' not supported.")
    end

    def register_connection_handler(regexp, cluster_class)
      @handlers[regexp] = cluster_class
    end

    def deregister_connection_handler(regexp)
      @handlers.delete(regexp)
    end
  end
end
