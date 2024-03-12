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

# frozen_string_literal: true

require_relative "protostellar/cluster"

module Couchbase
  # @api private
  module Protostellar
    NAME = "couchbase2"
    SCHEMES = %w[couchbase2 protostellar].freeze

    SCHEMES.each do |s|
      Couchbase::ClusterRegistry.instance.register_connection_handler(/^#{s}:\/\/.*$/i, Protostellar::Cluster)
    end
  end
end
