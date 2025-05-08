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

require "securerandom"
require "test_helper"

module Couchbase
  class CollectionManagerTest < Minitest::Test
    include TestUtilities

    TEST_MAGMA_BUCKET_NAME = 'test-magma-bucket'

    def create_magma_bucket
      @cluster.buckets.create_bucket(
        Management::BucketSettings.new do |s|
          s.name = TEST_MAGMA_BUCKET_NAME
          s.storage_backend = :magma
          s.ram_quota_mb = 1024
        end,
      )

      env.consistency.wait_until_bucket_present(TEST_MAGMA_BUCKET_NAME)

      retry_for_duration(expected_errors: [Error::BucketNotFound]) do
        @magma_bucket = @cluster.bucket(TEST_MAGMA_BUCKET_NAME)
      end
      @magma_collection_manager = @magma_bucket.collections
    end

    def get_scope_name
      name = Random.uuid
      @used_scopes << name
      name
    end

    def get_scope(scope_name, mgr = nil)
      mgr ||= @collection_manager
      mgr.get_all_scopes.find { |scope| scope.name == scope_name }
    end

    def get_collection(scope_name, collection_name, mgr = nil)
      mgr ||= @collection_manager
      mgr.get_all_scopes
         .find { |scope| scope.name == scope_name }
         .collections
         .find { |collection| collection.name == collection_name }
    end

    def setup
      connect
      @used_scopes = []
      @bucket = @cluster.bucket(env.bucket)
      @collection_manager = @bucket.collections
    end

    def teardown
      @used_scopes.each do |scope_name|
        @collection_manager.drop_scope(scope_name)
      rescue Error::ScopeNotFound
        # Ignored
      end
      @used_scopes = []

      unless @magma_bucket.nil?
        begin
          @cluster.buckets.drop_bucket(TEST_MAGMA_BUCKET_NAME)
        rescue Error::BucketNotFound
          # Ignored
        end
      end
      @magma_bucket = nil
      @magma_collection_manager = nil
      disconnect
    end

    def test_create_scope
      skip("#{name}: The server does not support collections") unless use_caves? || env.server_version.supports_collections?

      scope_name = get_scope_name
      @collection_manager.create_scope(scope_name)
      env.consistency.wait_until_scope_present(env.bucket, scope_name)
      scope = get_scope(scope_name)

      refute_nil scope, "scope #{scope_name} should be available by now"
    end

    def test_drop_scope
      skip("#{name}: The server does not support collections") unless use_caves? || env.server_version.supports_collections?

      scope_name = get_scope_name
      @collection_manager.create_scope(scope_name)
      env.consistency.wait_until_scope_present(env.bucket, scope_name)
      scope = get_scope(scope_name)

      refute_nil scope

      @collection_manager.drop_scope(scope_name)
      env.consistency.wait_until_scope_dropped(env.bucket, scope_name)
      scope = get_scope(scope_name)

      assert_nil scope
    end

    def test_create_collection
      skip("#{name}: The server does not support collections") unless use_caves? || env.server_version.supports_collections?

      coll_names = %w[coll-1 coll-2 coll-3]
      scope_name = get_scope_name
      @collection_manager.create_scope(scope_name)
      env.consistency.wait_until_scope_present(env.bucket, scope_name)

      coll_names.each do |coll_name|
        @collection_manager.create_collection(scope_name, coll_name)
        env.consistency.wait_until_collection_present(env.bucket, scope_name, coll_name)
      end
      scope = get_scope(scope_name)

      refute_nil scope
      assert_equal coll_names.sort, scope.collections.map(&:name).sort
    end

    def test_drop_collection
      skip("#{name}: The server does not support collections") unless use_caves? || env.server_version.supports_collections?

      coll_names = %w[coll-1 coll-2 coll-3]
      scope_name = get_scope_name
      @collection_manager.create_scope(scope_name)
      env.consistency.wait_until_scope_present(env.bucket, scope_name)

      coll_names.each do |coll_name|
        @collection_manager.create_collection(scope_name, coll_name)
        env.consistency.wait_until_collection_present(env.bucket, scope_name, coll_name)
      end
      scope = get_scope(scope_name)

      refute_nil scope
      assert_equal coll_names.sort, scope.collections.map(&:name).sort

      @collection_manager.drop_collection(scope_name, 'coll-1')
      env.consistency.wait_until_collection_dropped(env.bucket, scope_name, 'coll-1')

      scope = get_scope(scope_name)

      refute_includes scope.collections.map(&:name), 'coll-1'
    end

    def test_create_collection_already_exists
      skip("#{name}: The server does not support collections") unless use_caves? || env.server_version.supports_collections?

      coll_name = 'coll-1'
      scope_name = get_scope_name
      @collection_manager.create_scope(scope_name)
      env.consistency.wait_until_scope_present(env.bucket, scope_name)

      @collection_manager.create_collection(scope_name, coll_name)
      env.consistency.wait_until_collection_present(env.bucket, scope_name, coll_name)

      refute_nil get_collection(scope_name, coll_name)

      assert_raises(Error::CollectionExists) do
        @collection_manager.create_collection(scope_name, coll_name)
      end
    end

    def test_create_collection_scope_does_not_exist
      skip("#{name}: The server does not support collections") unless use_caves? || env.server_version.supports_collections?

      coll_name = 'coll-1'
      scope_name = 'does-not-exist'

      if use_caves?
        # FIXME: caves should conform to the spec
        exc =
          assert_raises(Error::BucketNotFound) do
            @collection_manager.create_collection(scope_name, coll_name)
          end

        assert_match(/scope_not_found/, exc.message)
      else
        assert_raises(Error::ScopeNotFound) do
          @collection_manager.create_collection(scope_name, coll_name)
        end
      end
    end

    def test_create_scope_already_exists
      skip("#{name}: The server does not support collections") unless use_caves? || env.server_version.supports_collections?

      scope_name = get_scope_name
      @collection_manager.create_scope(scope_name)
      env.consistency.wait_until_scope_present(env.bucket, scope_name)

      refute_nil get_scope(scope_name)

      assert_raises(Error::ScopeExists) do
        @collection_manager.create_scope(scope_name)
      end
    end

    def test_drop_scope_does_not_exist
      skip("#{name}: The server does not support collections") unless use_caves? || env.server_version.supports_collections?

      scope_name = 'does-not-exist'

      assert_raises(Error::ScopeNotFound) do
        @collection_manager.drop_scope(scope_name)
      end
    end

    def test_drop_collection_does_not_exist
      skip("#{name}: The server does not support collections") unless use_caves? || env.server_version.supports_collections?

      scope_name = get_scope_name
      coll_name = 'does-not-exist'
      @collection_manager.create_scope(scope_name)
      env.consistency.wait_until_scope_present(env.bucket, scope_name)

      refute_nil get_scope(scope_name)

      assert_raises(Error::CollectionNotFound) do
        @collection_manager.drop_collection(scope_name, coll_name)
      end
    end

    def test_drop_collection_scope_does_not_exist
      skip("#{name}: The server does not support collections") unless use_caves? || env.server_version.supports_collections?

      scope_name = 'does-not-exist'
      coll_name = 'does-not-exist'

      assert_raises(Error::ScopeNotFound) do
        @collection_manager.drop_collection(scope_name, coll_name)
      end
    end

    def test_create_collection_history_retention
      skip("#{name}: CAVES does not support history retention") if use_caves?
      skip("#{name}: Server does not support history retention") unless env.server_version.supports_history_retention?
      skip("#{name}: The #{Couchbase::Protostellar::NAME} protocol does not support history retention") if env.protostellar?

      create_magma_bucket
      scope_name = get_scope_name
      collection_name = 'test-coll'
      @magma_collection_manager.create_scope(scope_name)
      env.consistency.wait_until_scope_present(TEST_MAGMA_BUCKET_NAME, scope_name)
      scope = get_scope(scope_name, @magma_collection_manager)

      assert scope

      settings = Management::CreateCollectionSettings.new(history: true)
      @magma_collection_manager.create_collection(scope_name, collection_name, settings)
      env.consistency.wait_until_collection_present(TEST_MAGMA_BUCKET_NAME, scope_name, collection_name)

      coll = get_collection(scope_name, collection_name, @magma_collection_manager)

      assert coll
      assert coll.history
    end

    def test_update_collection_history_retention
      skip("#{name}: CAVES does not support update_collection & history retention") if use_caves?
      skip("#{name}: The #{Couchbase::Protostellar::NAME} protocol does not support update_collection") if env.protostellar?
      skip("#{name}: Server does not support history retention") unless env.server_version.supports_history_retention?
      skip("#{name}: Server does not support update_collection") unless env.server_version.supports_update_collection?

      create_magma_bucket
      scope_name = get_scope_name
      collection_name = 'test-coll'
      @magma_collection_manager.create_scope(scope_name)
      env.consistency.wait_until_scope_present(TEST_MAGMA_BUCKET_NAME, scope_name)

      scope = get_scope(scope_name, @magma_collection_manager)

      assert scope

      settings = Management::CreateCollectionSettings.new(history: false)
      @magma_collection_manager.create_collection(scope_name, collection_name, settings)
      env.consistency.wait_until_collection_present(TEST_MAGMA_BUCKET_NAME, scope_name, collection_name)

      coll = get_collection(scope_name, collection_name, @magma_collection_manager)

      assert coll
      refute coll.history

      settings = Management::UpdateCollectionSettings.new(history: true)
      @magma_collection_manager.update_collection(scope_name, collection_name, settings)

      coll = get_collection(scope_name, collection_name, @magma_collection_manager)

      assert coll
      assert coll.history
    end

    def test_create_collection_history_retention_unsupported
      skip("#{name}: CAVES does not support history retention") if use_caves?
      skip("#{name}: The #{Couchbase::Protostellar::NAME} protocol does not support history retention") if env.protostellar?
      skip("#{name}: Server does not support history retention") unless env.server_version.supports_history_retention?

      scope_name = get_scope_name
      collection_name = 'test-coll'
      @collection_manager.create_scope(scope_name)
      env.consistency.wait_until_scope_present(env.bucket, scope_name)

      scope = get_scope(scope_name)

      assert scope

      settings = Management::CreateCollectionSettings.new(history: true)
      assert_raises(Error::FeatureNotAvailable) do
        @collection_manager.create_collection(scope_name, collection_name, settings)
      end
    end

    def test_update_collection_history_retention_unsupported
      skip("#{name}: CAVES does not support update_collection & history retention") if use_caves?
      skip("#{name}: The #{Couchbase::Protostellar::NAME} protocol does not support update_collection") if env.protostellar?
      skip("#{name}: Server does not support history retention") unless env.server_version.supports_history_retention?
      skip("#{name}: Server does not support update_collection") unless env.server_version.supports_update_collection?

      scope_name = get_scope_name
      collection_name = 'test-coll'
      @collection_manager.create_scope(scope_name)
      env.consistency.wait_until_scope_present(env.bucket, scope_name)

      scope = get_scope(scope_name)

      assert scope

      @collection_manager.create_collection(scope_name, collection_name)

      coll = get_collection(scope_name, collection_name)

      assert coll
      refute coll.history

      settings = Management::UpdateCollectionSettings.new(history: true)

      assert_raises(Error::FeatureNotAvailable) do
        @collection_manager.update_collection(scope_name, collection_name, settings)
      end
    end

    def test_create_collection_max_expiry
      skip("#{name}: The server does not support collections") unless use_caves? || env.server_version.supports_collections?

      scope_name = get_scope_name
      collection_name = 'testcoll'
      @collection_manager.create_scope(scope_name)
      env.consistency.wait_until_scope_present(env.bucket, scope_name)
      scope = get_scope(scope_name)

      assert scope

      settings = Management::CreateCollectionSettings.new(max_expiry: 5)
      @collection_manager.create_collection(scope_name, collection_name, settings)
      env.consistency.wait_until_collection_present(env.bucket, scope_name, collection_name)

      coll_spec = get_collection(scope_name, collection_name)

      assert coll_spec
      assert_equal 5, coll_spec.max_expiry

      # Upsert a document and verify that it cannot be found after 5 seconds
      sleep(1)
      key = 'test-doc'
      content = {'foo' => 'bar'}
      coll = @bucket.scope(scope_name).collection(collection_name)
      retry_for_duration(expected_errors: [Error::CollectionNotFound, Error::ScopeNotFound], duration: 10) do
        coll.upsert(key, content)
      end

      assert_equal content, coll.get(key).content

      sleep(4)

      retry_until_error(error: Error::DocumentNotFound) do
        coll.get(key)
      end
    end

    def test_create_collection_max_expiry_no_expiry
      skip("#{name}: The server does not support collections") if use_caves?
      skip("#{name}: The #{Couchbase::Protostellar::NAME} protocol does not support setting max_expiry to -1 yet") if env.protostellar?
      unless env.server_version.supports_collection_max_expiry_set_to_no_expiry?
        skip("#{name}: The server does not support setting max_expiry to -1")
      end

      scope_name = get_scope_name
      collection_name = 'testcoll'
      @collection_manager.create_scope(scope_name)
      env.consistency.wait_until_scope_present(env.bucket, scope_name)
      scope = get_scope(scope_name)

      assert scope

      settings = Management::CreateCollectionSettings.new(max_expiry: -1)

      @collection_manager.create_collection(scope_name, collection_name, settings)
      env.consistency.wait_until_collection_present(env.bucket, scope_name, collection_name)

      coll_spec = get_collection(scope_name, collection_name)

      assert coll_spec
      assert_equal(-1, coll_spec.max_expiry)
    end

    def test_create_collection_max_expiry_no_expiry_not_supported
      skip("#{name}: The server supports setting max_expiry to -1") if env.server_version.supports_collection_max_expiry_set_to_no_expiry?
      skip("#{name}: CAVES allows to -1") if use_caves?

      scope_name = get_scope_name
      collection_name = 'testcoll'
      @collection_manager.create_scope(scope_name)
      env.consistency.wait_until_scope_present(env.bucket, scope_name)
      scope = get_scope(scope_name)

      assert scope, "the scope \"#{scope_name}\" must exist"

      settings = Management::CreateCollectionSettings.new(max_expiry: -1)

      assert_raises(Error::InvalidArgument) do
        @collection_manager.create_collection(scope_name, collection_name, settings)
      end
    end

    def test_create_collection_max_expiry_invalid
      skip("#{name}: The server does not support collections") unless use_caves? || env.server_version.supports_collections?

      scope_name = get_scope_name
      collection_name = 'testcoll'
      @collection_manager.create_scope(scope_name)
      env.consistency.wait_until_scope_present(env.bucket, scope_name)
      scope = get_scope(scope_name)

      assert scope

      settings = Management::CreateCollectionSettings.new(max_expiry: -10)
      assert_raises(Error::InvalidArgument) do
        @collection_manager.create_collection(scope_name, collection_name, settings)
      end
    end

    def test_update_collection_max_expiry
      skip("#{name}: CAVES does not support update_collection") if use_caves?
      skip("#{name}: The #{Couchbase::Protostellar::NAME} protocol does not support update_collection") if env.protostellar?
      unless env.server_version.supports_update_collection_max_expiry?
        skip("#{name}: Server does not support update_collection with max_expiry")
      end

      scope_name = get_scope_name
      collection_name = 'test-coll'
      @collection_manager.create_scope(scope_name)
      env.consistency.wait_until_scope_present(env.bucket, scope_name)
      scope = get_scope(scope_name)

      assert scope

      settings = Management::CreateCollectionSettings.new(max_expiry: 600)
      @collection_manager.create_collection(scope_name, collection_name, settings)
      env.consistency.wait_until_collection_present(env.bucket, scope_name, collection_name)

      coll_spec = get_collection(scope_name, collection_name)

      assert coll_spec
      assert_equal 600, coll_spec.max_expiry

      settings = Management::UpdateCollectionSettings.new(max_expiry: 1)
      @collection_manager.update_collection(scope_name, collection_name, settings)

      coll_spec = get_collection(scope_name, collection_name)

      assert coll_spec
      assert_equal 1, coll_spec.max_expiry

      # Upsert a document and verify that it cannot be found after 1 second
      key = 'test-doc'
      content = {'foo' => 'bar'}
      coll = @bucket.scope(scope_name).collection(collection_name)
      retry_for_duration(expected_errors: [Error::CollectionNotFound, Error::ScopeNotFound, Error::Timeout], duration: 10) do
        coll.upsert(key, content)
      end

      assert_equal content, coll.get(key).content

      sleep(1)

      retry_until_error(error: Error::DocumentNotFound) do
        coll.get(key)
      end
    end

    def test_update_collection_max_expiry_no_expiry
      skip("#{name}: CAVES does not support update_collection") if use_caves?
      skip("#{name}: The #{Couchbase::Protostellar::NAME} protocol does not support update_collection") if env.protostellar?
      unless env.server_version.supports_collection_max_expiry_set_to_no_expiry?
        skip("#{name}: The server does not support setting max_expiry to -1")
      end

      scope_name = get_scope_name
      collection_name = 'testcoll'
      @collection_manager.create_scope(scope_name)
      env.consistency.wait_until_scope_present(env.bucket, scope_name)
      scope = get_scope(scope_name)

      assert scope

      settings = Management::CreateCollectionSettings.new(max_expiry: 600)
      @collection_manager.create_collection(scope_name, collection_name, settings)
      env.consistency.wait_until_collection_present(env.bucket, scope_name, collection_name)

      coll_spec = get_collection(scope_name, collection_name)

      assert coll_spec
      assert_equal 600, coll_spec.max_expiry

      settings = Management::UpdateCollectionSettings.new(max_expiry: -1)

      @collection_manager.update_collection(scope_name, collection_name, settings)

      coll_spec = get_collection(scope_name, collection_name)

      assert coll_spec
      assert_equal(-1, coll_spec.max_expiry)
    end

    def test_update_collection_max_expiry_no_expiry_not_supported
      skip("#{name}: CAVES does not support update_collection") if use_caves?
      unless env.server_version.supports_update_collection_max_expiry?
        skip("#{name}: The server does not support update_collection with max_expiry")
      end
      skip("#{name}: The #{Couchbase::Protostellar::NAME} protocol does not support update_collection") if env.protostellar?
      skip("#{name}: The server supports setting max_expiry to -1") if env.server_version.supports_collection_max_expiry_set_to_no_expiry?

      scope_name = get_scope_name
      collection_name = 'testcoll'
      @collection_manager.create_scope(scope_name)
      env.consistency.wait_until_scope_present(env.bucket, scope_name)
      scope = get_scope(scope_name)

      assert scope

      settings = Management::CreateCollectionSettings.new(max_expiry: 600)
      @collection_manager.create_collection(scope_name, collection_name, settings)
      env.consistency.wait_until_collection_present(env.bucket, scope_name, collection_name)

      coll_spec = get_collection(scope_name, collection_name)

      assert coll_spec
      assert_equal 600, coll_spec.max_expiry

      settings = Management::UpdateCollectionSettings.new(max_expiry: -1)

      assert_raises(Error::InvalidArgument) do
        @collection_manager.update_collection(scope_name, collection_name, settings)
      end
    end

    def test_update_collection_max_expiry_invalid
      skip("#{name}: CAVES does not support update_collection") if use_caves?
      skip("#{name}: The server does not support collections") unless env.server_version.supports_collections?
      skip("#{name}: The #{Couchbase::Protostellar::NAME} protocol does not support update_collection") if env.protostellar?

      scope_name = get_scope_name
      collection_name = 'testcoll'
      @collection_manager.create_scope(scope_name)
      env.consistency.wait_until_scope_present(env.bucket, scope_name)
      scope = get_scope(scope_name)

      assert scope

      settings = Management::CreateCollectionSettings.new(max_expiry: 600)
      @collection_manager.create_collection(scope_name, collection_name, settings)
      env.consistency.wait_until_collection_present(env.bucket, scope_name, collection_name)

      coll_spec = get_collection(scope_name, collection_name)

      assert coll_spec
      assert_equal 600, coll_spec.max_expiry

      settings = Management::UpdateCollectionSettings.new(max_expiry: -10)
      assert_raises(Error::InvalidArgument) do
        @collection_manager.update_collection(scope_name, collection_name, settings)
      end
    end

    def test_update_collection_does_not_exist
      skip("#{name}: CAVES does not support update_collection") if use_caves?
      skip("#{name}: The #{Couchbase::Protostellar::NAME} protocol does not support update_collection") if env.protostellar?
      unless env.server_version.supports_update_collection_max_expiry?
        skip("#{name}: Server does not support update_collection with max_expiry")
      end

      scope_name = get_scope_name
      coll_name = 'does-not-exist'
      @collection_manager.create_scope(scope_name)
      env.consistency.wait_until_scope_present(env.bucket, scope_name)

      refute_nil get_scope(scope_name)

      settings = Management::UpdateCollectionSettings.new(max_expiry: 10)

      assert_raises(Error::CollectionNotFound) do
        @collection_manager.update_collection(scope_name, coll_name, settings)
      end
    end

    def test_update_collection_scope_does_not_exist
      skip("#{name}: CAVES does not support update_collection") if use_caves?
      skip("#{name}: The #{Couchbase::Protostellar::NAME} protocol does not support update_collection") if env.protostellar?
      unless env.server_version.supports_update_collection_max_expiry?
        skip("#{name}: Server does not support update_collection with max_expiry")
      end

      scope_name = 'does-not-exist'
      coll_name = 'does-not-exist'
      settings = Management::UpdateCollectionSettings.new(max_expiry: 10)

      assert_raises(Error::ScopeNotFound) do
        @collection_manager.update_collection(scope_name, coll_name, settings)
      end
    end

    def test_create_collection_deprecated
      skip("#{name}: The server does not support collections") unless use_caves? || env.server_version.supports_collections?

      scope_name = get_scope_name
      coll_name = 'coll-1'
      @collection_manager.create_scope(scope_name)
      env.consistency.wait_until_scope_present(env.bucket, scope_name)

      @collection_manager.create_collection(
        Management::CollectionSpec.new do |spec|
          spec.name = coll_name
          spec.scope_name = scope_name
        end,
        Management::Options::Collection::CreateCollection.new(timeout: 80_000),
      )
      env.consistency.wait_until_collection_present(env.bucket, scope_name, coll_name)

      refute_nil get_collection(scope_name, coll_name)
    end

    def test_drop_collection_deprecated
      skip("#{name}: The server does not support collections") unless use_caves? || env.server_version.supports_collections?

      scope_name = get_scope_name
      coll_name = 'coll-1'
      @collection_manager.create_scope(scope_name)
      env.consistency.wait_until_scope_present(env.bucket, scope_name)

      @collection_manager.create_collection(
        Management::CollectionSpec.new do |spec|
          spec.name = coll_name
          spec.scope_name = scope_name
        end,
      )
      env.consistency.wait_until_collection_present(env.bucket, scope_name, coll_name)

      refute_nil get_collection(scope_name, coll_name)

      @collection_manager.drop_collection(
        Management::CollectionSpec.new do |spec|
          spec.name = coll_name
          spec.scope_name = scope_name
        end,
        Management::Options::Collection::DropCollection.new(timeout: 80_000),
      )
      env.consistency.wait_until_collection_dropped(env.bucket, scope_name, coll_name)

      assert_nil get_collection(scope_name, coll_name)
    end
  end
end
