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
          ResponseConverter::Admin::Collection.from_list_collections_response(resp)
        end

        def create_scope(scope_name, options = Couchbase::Management::Options::Collection::CreateScope.new)
          req = @request_generator.create_scope_request(scope_name, options)
          @client.send_request(req)
        end

        def drop_scope(scope_name, options = Couchbase::Management::Options::Collection::DropScope.new)
          req = @request_generator.delete_scope_request(scope_name, options)
          @client.send_request(req)
        end

        def create_collection(collection_spec, options = Couchbase::Management::Options::Collection::CreateCollection.new)
          req = @request_generator.create_collection_request(collection_spec, options)
          @client.send_request(req)
        end

        def drop_collection(collection_spec, options = Couchbase::Management::Options::Collection::DropCollection.new)
          req = @request_generator.delete_collection_request(collection_spec, options)
          @client.send_request(req)
        end
      end
    end
  end
end
