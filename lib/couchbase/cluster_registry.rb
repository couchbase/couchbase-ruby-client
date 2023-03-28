# frozen_string_literal: true

#  Copyright 2023 Couchbase, Inc.
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

require "singleton"

require "couchbase/errors"

module Couchbase
  class ClusterRegistry
    include Singleton

    def initialize
      @handlers = {}
    end

    def connect(connection_string_or_config, *options)
      connection_string = if connection_string_or_config.is_a?(Configuration)
                            connection_string_or_config.connection_string
                          else
                            connection_string_or_config
                          end
      @handlers.each do |regexp, cluster_class|
        return cluster_class.connect(connection_string_or_config, *options) if regexp.match?(connection_string)
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
