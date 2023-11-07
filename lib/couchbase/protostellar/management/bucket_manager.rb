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

require "couchbase/management/bucket_manager"
require "couchbase/options"

require_relative "../request_generator/admin/bucket"
require_relative "../response_converter/admin/bucket"

module Couchbase
  module Protostellar
    module Management
      class BucketManager
        def initialize(client)
          @client = client
          @request_generator = RequestGenerator::Admin::Bucket.new
        end

        def create_bucket(settings, options = Couchbase::Management::Options::Bucket::CreateBucket.new)
          req = @request_generator.create_bucket_request(settings, options)
          @client.send_request(req)
        end

        def update_bucket(settings, options = Couchbase::Management::Options::Bucket::UpdateBucket.new)
          req = @request_generator.update_bucket_request(settings, options)
          @client.send_request(req)
        end

        def drop_bucket(settings, options = Couchbase::Management::Options::Bucket::DropBucket.new)
          req = @request_generator.delete_bucket_request(settings, options)
          @client.send_request(req)
        end

        def get_all_buckets(options = Couchbase::Management::Options::Bucket::GetAllBuckets.new)
          req = @request_generator.list_buckets_request(options)
          resp = @client.send_request(req)
          ResponseConverter::Admin::Bucket.to_bucket_settings_array(resp)
        end

        def get_bucket(bucket_name, options = Couchbase::Management::Options::Bucket::GetBucket.new)
          buckets = get_all_buckets(options)
          bucket = buckets.find { |b| b.name == bucket_name }
          raise Couchbase::Error::BucketNotFound, "Could not find bucket '#{bucket_name}'" if bucket.nil?

          bucket
        end

        def flush_bucket(_bucket_name, _options = Couchbase::Management::Options::Bucket::FlushBucket.new)
          raise Couchbase::Error::FeatureNotAvailable, "The #{Protostellar::NAME} protocol does not support flush_bucket yet"
        end
      end
    end
  end
end
