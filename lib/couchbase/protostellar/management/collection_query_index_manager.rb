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

require "couchbase/management/query_index_manager"

require_relative "../request_generator/admin/query"
require_relative "../response_converter/admin/query"

module Couchbase
  module Protostellar
    module Management
      class CollectionQueryIndexManager
        def initialize(client:, bucket_name:, scope_name:, collection_name:)
          @client = client
          @bucket_name = bucket_name
          @scope_name = scope_name
          @collection_name = collection_name
          @request_generator = RequestGenerator::Admin::Query.new(
            bucket_name: @bucket_name, scope_name: @scope_name, collection_name: @collection_name
          )
        end

        def get_all_indexes(options = Couchbase::Management::Options::Query::GetAllIndexes.new)
          validate_options(options)

          req = @request_generator.get_all_indexes_request(options)
          resp = @client.send_request(req)
          ResponseConverter::Admin::Query.to_query_index_array(resp)
        end

        def create_index(index_name, fields, options = Couchbase::Management::Options::Query::CreateIndex.new)
          validate_options(options)

          req = @request_generator.create_index_request(index_name, fields, options)
          @client.send_request(req)
        end

        def create_primary_index(options = Couchbase::Management::Options::Query::CreatePrimaryIndex.new)
          validate_options(options)

          req = @request_generator.create_primary_index_request(options)
          @client.send_request(req)
        end

        def drop_index(index_name, options = Couchbase::Management::Options::Query::DropIndex.new)
          validate_options(options)

          req = @request_generator.drop_index_request(index_name, options)
          @client.send_request(req)
        end

        def drop_primary_index(options = Couchbase::Management::Options::Query::DropPrimaryIndex.new)
          validate_options(options)

          req = @request_generator.drop_primary_index_request(options)
          @client.send_request(req)
        end

        def build_deferred_indexes(options = Couchbase::Management::Options::Query::BuildDeferredIndexes.new)
          validate_options(options)

          req = @request_generator.build_deferred_indexes_request(options)
          @client.send_request(req)
        end

        def watch_indexes(index_names, timeout, options = Couchbase::Management::Options::Query::WatchIndexes.new)
          validate_options(options)

          # TODO: Will this be implemented in the gateway instead?

          index_names.append("#primary") if options.watch_primary

          interval_millis = 50
          deadline = Time.now + (Utils::Time.extract_duration(timeout) * 0.001)

          while Time.now <= deadline
            get_all_opts = Couchbase::Management::Options::Query::GetAllIndexes.new(timeout: ((deadline - Time.now) * 1000).round)
            indexes = get_all_indexes(get_all_opts).select { |idx| index_names.include? idx.name }
            indexes_not_found = index_names - indexes.map(&:name)
            unless indexes_not_found.empty?
              raise Couchbase::Error::IndexNotFound,
                    "Failed to find the indexes: #{indexes_not_found.join(', ')}"
            end

            all_online = indexes.all? { |idx| idx.state == :online }
            return if all_online

            sleep(interval_millis / 1000)
            interval_millis += 500
            interval_millis = 1000 if interval_millis > 1000
          end
          raise Couchbase::Error::UnambiguousTimeout, "Failed to find all indexes online within the allotted time"
        end

        private

        def validate_options(options)
          unless options.scope_name.nil?
            raise Couchbase::Error::InvalidArgument,
                  "Scope name cannot be set in the options when using the Query Index manager at the collection level"
          end

          return if options.collection_name.nil?

          raise Couchbase::Error::InvalidArgument,
                "Collection name cannot be set in the options when using the Query Index manager at the collection level"
        end
      end
    end
  end
end
