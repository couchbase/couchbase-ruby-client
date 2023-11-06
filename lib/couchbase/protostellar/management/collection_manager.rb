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

require "couchbase/management/collection_manager"

require_relative "../request_generator/admin/collection"
require_relative "../response_converter/admin/collection"

module Couchbase
  module Protostellar
    module Management
      class CollectionManager
        def initialize(client:, bucket_name:)
          @client = client
          @bucket_name = bucket_name
          @request_generator = RequestGenerator::Admin::Collection.new(bucket_name: @bucket_name)
        end

        def get_all_scopes(options = Couchbase::Management::Options::Collection::GetAllScopes.new)
          req = @request_generator.list_collections_request(options)
          resp = @client.send_request(req)
          ResponseConverter::Admin::Collection.to_scope_spec_array(resp)
        end

        def create_scope(scope_name, options = Couchbase::Management::Options::Collection::CreateScope.new)
          req = @request_generator.create_scope_request(scope_name, options)
          @client.send_request(req)
        end

        def drop_scope(scope_name, options = Couchbase::Management::Options::Collection::DropScope.new)
          req = @request_generator.delete_scope_request(scope_name, options)
          @client.send_request(req)
        end

        def create_collection(*args)
          req =
            if args[0].is_a?(Couchbase::Management::CollectionSpec)
              collection = args[0]
              options = args[1] || Couchbase::Management::Options::Collection::CreateCollection::DEFAULT
              settings = Couchbase::Management::CreateCollectionSettings.new(max_expiry: collection.max_expiry, history: collection.history)

              warn "Calling create_collection with a CollectionSpec object has been deprecated, supply scope name, " \
                 "collection name and optionally a CreateCollectionSettings instance"

              @request_generator.create_collection_request(collection.scope_name, collection.name, settings, options)
            else
              scope_name = args[0]
              collection_name = args[1]
              settings = args[2] || Couchbase::Management::CreateCollectionSettings::DEFAULT
              options = args[3] || Couchbase::Management::Options::Collection::CreateCollection::DEFAULT
              @request_generator.create_collection_request(scope_name, collection_name, settings, options)
            end
          @client.send_request(req)
        end

        def drop_collection(*args)
          req =
            if args[0].is_a?(Couchbase::Management::CollectionSpec)
              collection = args[0]
              options = args[1] || Couchbase::Management::Options::Collection::CreateCollection::DEFAULT

              warn "Calling drop_collection with a CollectionSpec object has been deprecated, supply scope name and collection name"

              @request_generator.delete_collection_request(collection.scope_name, collection.name, options)
            else
              scope_name = args[0]
              collection_name = args[1]
              options = args[2] || Couchbase::Management::Options::Collection::CreateCollection::DEFAULT
              @request_generator.delete_collection_request(scope_name, collection_name, options)
            end
          @client.send_request(req)
        end

        def update_collection(_scope_name, _collection_name, _settings = UpdateCollectionSettings::DEFAULT,
                              _options = Options::Collection::UpdateCollection::DEFAULT)
          raise Error::FeatureNotAvailable, "The #{Protostellar::NAME} protocol does not support update_collection"
        end
      end
    end
  end
end
