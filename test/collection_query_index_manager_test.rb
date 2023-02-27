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

require_relative "test_helper"

module Couchbase
  class CollectionQueryIndexManagerTest < Minitest::Test
    include TestUtilities

    def setup
      unless env.server_version.supports_collections?
        skip("skipped for (#{env.server_version}) as the CollectionQueryIndexManager requires collection support")
      end

      connect
      @bucket = @cluster.bucket(env.bucket)

      @scope_name = uniq_id(:scope).tr(".", "")
      collection_name = uniq_id(:collection).tr(".", "")

      collection_mgr = @bucket.collections
      collection_mgr.create_scope(@scope_name)

      collection_spec = Management::CollectionSpec.new do |s|
        s.name = collection_name
        s.scope_name = @scope_name
        s.max_expiry = nil
      end

      # Retry a few times in case the scope needs time to be created
      deadline = Time.now + 5
      success = false
      while Time.now <= deadline
        begin
          collection_mgr.create_collection(collection_spec)
          success = true
          break
        rescue Error::CouchbaseError
          sleep(1)
        end
      end
      raise "Failed to create scope/collection" unless success

      @collection = @bucket.scope(@scope_name).collection(collection_name)

      # Upsert something in the collection to make sure it's been created
      deadline = Time.now + 5
      success = false
      while Time.now <= deadline
        begin
          @collection.upsert("foo", {"test" => 10})
          success = true
          break
        rescue Error::CouchbaseError
          sleep(1)
        end
      end
      raise "Failed to create collection" unless success

      @idx_mgr = @collection.query_indexes
    end

    def teardown
      @bucket.collections.drop_scope(@scope_name) if defined? @bucket
      disconnect
    end

    def test_collection_query_indexes
      @idx_mgr.create_primary_index
      @idx_mgr.create_index("test_index", ["test"])
      res1 = @idx_mgr.get_all_indexes

      assert_empty %w[test_index #primary] - res1.map(&:name)
      assert_equal 2, res1.size

      @idx_mgr.create_index("test_index_deferred", ["something"], Management::Options::Query::CreateIndex.new(deferred: true))
      res2 = @idx_mgr.get_all_indexes

      assert_equal 3, res2.size
      assert_equal :deferred, res2.find { |idx| idx.name == "test_index_deferred" }.state

      @idx_mgr.build_deferred_indexes
      @idx_mgr.watch_indexes(["test_index_deferred"], 50000)
      res3 = @idx_mgr.get_all_indexes

      assert_equal 3, res3.size
      assert_equal :online, res3.find { |idx| idx.name == "test_index_deferred" }.state

      assert_raises(Error::IndexNotFound) { @idx_mgr.watch_indexes(["non_existent_index"], 50000) }

      @idx_mgr.drop_primary_index
      @idx_mgr.drop_index("test_index_deferred")
      res4 = @idx_mgr.get_all_indexes

      assert_equal 1, res4.size
    end
  end
end
