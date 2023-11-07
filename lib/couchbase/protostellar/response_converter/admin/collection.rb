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

require "couchbase/management/collection_manager"

module Couchbase
  module Protostellar
    module ResponseConverter
      module Admin
        class Collection
          def self.to_scope_spec_array(resp)
            resp.scopes.map do |s|
              Couchbase::Management::ScopeSpec.new do |scope_spec|
                scope_spec.name = s.name
                scope_spec.collections = s.collections.map do |c|
                  Couchbase::Management::CollectionSpec.new do |coll_spec|
                    coll_spec.name = c.name
                    coll_spec.scope_name = s.name
                    coll_spec.max_expiry = c.max_expiry_secs if c.has_max_expiry_secs?
                  end
                end
              end
            end
          end
        end
      end
    end
  end
end
