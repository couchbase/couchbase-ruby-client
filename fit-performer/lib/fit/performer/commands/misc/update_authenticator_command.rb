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

require 'fit/performer/commands/shared_results'

module FIT
  module Performer
    module Commands
      module Misc
        class UpdateAuthenticatorCommand
          def initialize(authenticator:, cluster:, initiated:, return_result:, get_span_fn:)
            @authenticator = authenticator
            @cluster = cluster
            @initiated = initiated
            @return_result = return_result
            @get_span_fn = get_span_fn
          end

          def execute_command
            SharedResults.as_success(initiated: @initiated) do
              @cluster.update_authenticator(@authenticator)
            end
          end

          def self.create_command(...)
            new(...)
          end
        end
      end
    end
  end
end
