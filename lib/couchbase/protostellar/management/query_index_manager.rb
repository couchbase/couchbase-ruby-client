# frozen_string_literal: true

require "couchbase/management/query_index_manager"

require_relative "../request_generator/admin/query"
require_relative "../response_converter/admin/query"

module Couchbase
  module Protostellar
    module Management
      class QueryIndexManager
        def initialize(client:)
          @client = client
          @request_generator = RequestGenerator::Admin::Query.new
        end

        def get_all_indexes(bucket_name, options)
          validate_options(options)

          req = @request_generator.get_all_indexes_request(options, bucket_name)
          resp = @client.send_request(req)
          ResponseConverter::Admin::Query.to_query_index_array(resp)
        end

        def create_index(bucket_name, index_name, fields, options)
          validate_options(options)

          req = @request_generator.create_index_request(index_name, fields, options, bucket_name)
          @client.send_request(req)
        end

        def create_primary_index(bucket_name, options)
          validate_options(options)

          req = @request_generator.create_primary_index_request(options, bucket_name)
          @client.send_request(req)
        end

        def drop_index(bucket_name, index_name, options)
          validate_options(options)

          req = @request_generator.drop_index_request(index_name, options, bucket_name)
          @client.send_request(req)
        end

        def build_deferred_indexes(bucket_name, options)
          validate_options(options)

          req = @request_generator.build_deferred_indexes_request(options, bucket_name)
          @client.send_request(req)
        end

        def watch_indexes(bucket_name, index_names, timeout, options)
          validate_options(options)

          # TODO: Will this be implemented in the gateway instead?

          index_names.append("#primary") if options.watch_primary

          interval_millis = 50
          deadline = Time.now + (Utils::Time.extract_duration(timeout) * 0.001)
          while Time.now <= deadline
            get_all_opts = Options::Query::GetAllIndexes.new(timeout: ((deadline - Time.now) * 1000).round)
            indexes = get_all_indexes(bucket_name, get_all_opts).select { |idx| index_names.include? idx.name }
            indexes_not_found = index_names - indexes.map(&:name)
            raise Couchbase::Error::IndexNotFound, "Failed to find the indexes: #{indexes_not_found.join(', ')}" unless indexes_not_found.empty?

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
          return if options.scope_name.nil? && options.collection_name.nil?

          warn "The attributes 'scope_name' and 'collection_name' have been deprecated. Use 'collection.query_indexes' instead"
        end
      end
    end
  end
end
