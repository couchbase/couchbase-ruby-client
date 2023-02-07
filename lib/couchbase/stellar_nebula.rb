# frozen_string_literal: true

#  Copyright 2022-Present Couchbase, Inc.
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

require "couchbase"

require_relative "stellar_nebula/version"
require_relative "stellar_nebula/cluster"

module Couchbase
  module StellarNebula
    Couchbase::ClusterRegistry.instance.register_connection_handler(/^protostellar:\/\/.*$/i, StellarNebula::Cluster)
  end
end
