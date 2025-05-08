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

require_relative "test_helper"

module Couchbase
  class BucketManagerTest < Minitest::Test
    include TestUtilities

    def get_bucket_name
      name = uniq_id(:bucket_mgr_test)
      @used_buckets << name
      name
    end

    def setup
      connect
      @used_buckets = []
      @bucket_manager = @cluster.buckets
    end

    def teardown
      @used_buckets.each do |bucket_name|
        @bucket_manager.drop_bucket(bucket_name)
      rescue Error::BucketNotFound
        # Ignored
      end
      disconnect
    end

    def test_create_bucket_history_retention
      skip("#{name}: Server does not support history retention") unless env.server_version.supports_history_retention?
      skip("#{name}: The #{Couchbase::Protostellar::NAME} protocol does not support history retention") if env.protostellar?

      bucket_name = get_bucket_name
      @bucket_manager.create_bucket(
        Couchbase::Management::BucketSettings.new do |s|
          s.name = bucket_name
          s.storage_backend = :magma
          s.ram_quota_mb = 1024
          s.history_retention_collection_default = false
          s.history_retention_bytes = 2147483648
          s.history_retention_duration = 600
        end,
      )

      env.consistency.wait_until_bucket_present(bucket_name)

      res = @bucket_manager.get_bucket(bucket_name)

      refute res.history_retention_collection_default
      assert_equal 2**31, res.history_retention_bytes
      assert_equal 600, res.history_retention_duration
    end

    def test_create_bucket_history_retention_unsupported
      skip("#{name}: Server does not support history retention") unless env.server_version.supports_history_retention?
      skip("#{name}: The #{Couchbase::Protostellar::NAME} protocol does not support history retention") if env.protostellar?

      bucket_name = get_bucket_name
      assert_raises(Error::InvalidArgument) do
        @bucket_manager.create_bucket(
          Couchbase::Management::BucketSettings.new do |s|
            s.name = bucket_name
            s.storage_backend = :couchstore
            s.history_retention_collection_default = false
            s.history_retention_bytes = 2**31
            s.history_retention_duration = 600
          end,
        )
      end
    end

    def test_update_bucket_history_retention
      skip("#{name}: Server does not support history retention") unless env.server_version.supports_history_retention?
      skip("#{name}: The #{Couchbase::Protostellar::NAME} protocol does not support history retention") if env.protostellar?

      bucket_name = get_bucket_name
      @bucket_manager.create_bucket(
        Couchbase::Management::BucketSettings.new do |s|
          s.name = bucket_name
          s.storage_backend = :magma
          s.ram_quota_mb = 1024
        end,
      )

      env.consistency.wait_until_bucket_present(bucket_name)

      res = @bucket_manager.get_bucket(bucket_name)

      assert res.history_retention_collection_default
      assert_equal 0, res.history_retention_bytes
      assert_equal 0, res.history_retention_duration

      @bucket_manager.update_bucket(
        Couchbase::Management::BucketSettings.new do |s|
          s.name = bucket_name
          s.storage_backend = :magma
          s.ram_quota_mb = 1024
          s.history_retention_collection_default = false
          s.history_retention_bytes = 2147483648
          s.history_retention_duration = 600
        end,
      )
      res = @bucket_manager.get_bucket(bucket_name)

      refute res.history_retention_collection_default
      assert_equal 2**31, res.history_retention_bytes
      assert_equal 600, res.history_retention_duration
    end

    def test_update_bucket_history_retention_unsupported
      skip("#{name}: Server does not support history retention") unless env.server_version.supports_history_retention?
      skip("#{name}: The #{Couchbase::Protostellar::NAME} protocol does not support history retention") if env.protostellar?

      bucket_name = get_bucket_name
      @bucket_manager.create_bucket(
        Couchbase::Management::BucketSettings.new do |s|
          s.name = bucket_name
          s.storage_backend = :couchstore
        end,
      )

      env.consistency.wait_until_bucket_present(bucket_name)

      res = @bucket_manager.get_bucket(bucket_name)

      assert_nil res.history_retention_collection_default
      assert_nil res.history_retention_bytes
      assert_nil res.history_retention_duration

      assert_raises(Error::InvalidArgument) do
        @bucket_manager.update_bucket(
          Couchbase::Management::BucketSettings.new do |s|
            s.name = bucket_name
            s.storage_backend = :couchstore
            s.history_retention_collection_default = false
            s.history_retention_bytes = 2147483648
            s.history_retention_duration = 600
          end,
        )
      end
    end
  end
end
