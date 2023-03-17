# frozen_string_literal: true

require "couchbase/management/collection_manager"

module Couchbase
  module Protostellar
    module ResponseConverter
      module Admin
        class Collection
          def self.from_list_collections_response(resp)
            resp.scopes.map do |s|
              Couchbase::Management::ScopeSpec.new do |scope_spec|
                scope_spec.name = s.name
                scope_spec.collections = s.collections.map do |c|
                  Couchbase::Management::CollectionSpec.new do |coll_spec|
                    coll_spec.name = c.name
                    coll_spec.scope_name = s.name
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
