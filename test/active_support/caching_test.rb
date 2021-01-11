#    Copyright 2020 Couchbase, Inc.
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

require_relative "../test_helper"

require "rack"

require "active_support"
require "active_support/cache"
require "active_support/test_case"
require "active_support/testing/method_call_assertions"

require_relative "behaviors"

module Couchbase
  class CachingTest < ActiveSupport::TestCase
    include ActiveSupport::Testing::MethodCallAssertions

    def lookup_store(options = {})
      ActiveSupport::Cache.lookup_store(:couchbase_store, {
        connection_string: TEST_CONNECTION_STRING,
        username: TEST_USERNAME,
        password: TEST_PASSWORD,
        bucket: TEST_BUCKET,
        namespace: @namespace,
      }.merge(options))
    end

    def setup
      @namespace = "test-#{SecureRandom.hex}"

      @cache = lookup_store(expires_in: 60.seconds)
    end
  end

  class HealthyStoreTest < CachingTest
    include CacheDeleteMatchedBehavior
    include CacheIncrementDecrementBehavior
    include CacheInstrumentationBehavior
    include CacheStoreBehavior
    include CacheStoreCoderBehavior
    include CacheStoreVersionBehavior
    include EncodedKeyCacheBehavior
    include LocalCacheBehavior

    def setup
      super

      # for LocalCacheBehavior
      @peek = lookup_store(expires_in: 60.seconds)
    end
  end

  class UnavailableCouchbaseCluster < ::Couchbase::Cluster
    def self.connect(*)
      raise ::Couchbase::Error::UnambiguousTimeout
    end
  end

  class UnhealthyStoreTest < CachingTest
    include FailureSafetyBehavior

    private

    def emulating_unavailability
      old_client = ::Couchbase.send(:remove_const, :Cluster)
      ::Couchbase.const_set(:Cluster, UnavailableCouchbaseCluster)
      yield lookup_store
    ensure
      ::Couchbase.send(:remove_const, :Cluster)
      ::Couchbase.const_set(:Cluster, old_client)
    end
  end
end
