#  Copyright 2023 Couchbase, Inc.
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

require "securerandom"
require_relative "test_helper"

module Couchbase
  class QueryIndexManagerTest < Minitest::Test
    include TestUtilities

    def setup
      connect
      @bucket_name = 'query-idx-test-bucket'
      @cluster.buckets.create_bucket(
        Couchbase::Management::BucketSettings.new do |s|
          s.name = @bucket_name
          s.ram_quota_mb = 256
        end
      )
      retry_for_duration(expected_errors: [Error::BucketNotFound]) do
        @bucket = @cluster.bucket('query-idx-test-bucket')
      end

      # Add a scope in the bucket to verify it has been created
      retry_for_duration(expected_errors: [Error::BucketNotFound]) do
        @bucket.collections.create_scope("test-scope")
      end

      @idx_mgr = @cluster.query_indexes
      sleep(2)
    end

    def teardown
      @cluster.buckets.drop_bucket(@bucket_name)
      disconnect
    end

    def test_get_all_indexes
      index_names = [uniq_id(:foo), uniq_id(:bar)]
      index_names.each { |idx_name| @idx_mgr.create_index(@bucket_name, idx_name, ["foo"]) }

      res = @idx_mgr.get_all_indexes(@bucket_name)

      assert_predicate res.length, :positive?
      index_names.each do |idx_name|
        assert_includes(res.map(&:name), idx_name)
      end
    end

    def test_query_indexes
      @idx_mgr.create_primary_index(@bucket_name)
      @idx_mgr.create_index(@bucket_name, "test_index", ["test"])
      res1 = @idx_mgr.get_all_indexes(@bucket_name)

      assert_empty %w[test_index #primary] - res1.map(&:name)
      assert_equal 2, res1.size

      @idx_mgr.create_index(@bucket_name, "test_index_deferred", ["something"], Management::Options::Query::CreateIndex.new(deferred: true))
      res2 = @idx_mgr.get_all_indexes(@bucket_name)

      assert_equal 3, res2.size
      assert_equal :deferred, res2.find { |idx| idx.name == "test_index_deferred" }.state

      @idx_mgr.build_deferred_indexes(@bucket_name)
      @idx_mgr.watch_indexes(@bucket_name, ["test_index_deferred"], 50000)
      res3 = @idx_mgr.get_all_indexes(@bucket_name)

      assert_equal 3, res3.size
      assert_equal :online, res3.find { |idx| idx.name == "test_index_deferred" }.state

      assert_raises(Error::IndexNotFound) { @idx_mgr.watch_indexes(@bucket_name, ["non_existent_index"], 50000) }

      @idx_mgr.drop_primary_index(@bucket_name)
      @idx_mgr.drop_index(@bucket_name, "test_index_deferred")
      res4 = @idx_mgr.get_all_indexes(@bucket_name)

      assert_equal 1, res4.size
    end
  end
end
