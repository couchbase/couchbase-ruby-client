# frozen_string_literal: true

#  Copyright 2020-2021 Couchbase, Inc.
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

require "active_support"
require "active_support/cache/couchbase_store"

module Couchbase
  class MutationTrackingTest < Minitest::Test
    include TestUtilities

    def token(partition_id, sequence_number, partition_uuid: 1, bucket_name: env.bucket)
      MutationToken.new do |t|
        t.bucket_name = bucket_name
        t.partition_id = partition_id
        t.partition_uuid = partition_uuid
        t.sequence_number = sequence_number
      end
    end

    def sequences(state)
      state.tokens.to_h { |t| [t.partition_id, t.sequence_number] }
    end

    def test_last_mutation_keeps_the_newest_token_of_the_last_write
      tracker = ActiveSupport::Cache::CouchbaseStore::LastMutation.new

      assert_nil tracker.mutation_state

      tracker.record(token(1, 10))
      tracker.record(token(2, 5), token(3, 7))
      tracker.record

      assert_equal({3 => 7}, sequences(tracker.mutation_state))
    end

    def test_all_mutations_keeps_one_token_per_vbucket
      tracker = ActiveSupport::Cache::CouchbaseStore::AllMutations.new

      assert_nil tracker.mutation_state

      tracker.record(token(1, 10), token(2, 5))
      tracker.record(token(1, 12), token(3, 1))
      tracker.record(token(1, 4))
      tracker.record(token(1, 99, bucket_name: "#{env.bucket}-other"))

      state = tracker.mutation_state

      assert_equal 4, state.tokens.size
      assert_equal [12, 5, 1, 99], state.tokens.map(&:sequence_number)
    end

    def test_all_mutations_replaces_a_token_after_failover
      tracker = ActiveSupport::Cache::CouchbaseStore::AllMutations.new
      tracker.record(token(1, 10, partition_uuid: 1))
      tracker.record(token(1, 3, partition_uuid: 2))

      assert_equal([[2, 3]], tracker.mutation_state.tokens.map { |t| [t.partition_uuid, t.sequence_number] })
    end

    def test_all_mutations_keeps_the_newest_token_under_concurrent_writers
      tracker = ActiveSupport::Cache::CouchbaseStore::AllMutations.new
      # The first env call may start CAVES, which is not thread-safe.
      bucket = env.bucket
      threads = Array.new(8) do |n|
        Thread.new { 500.downto(1) { |seq| tracker.record(token(seq % 16, (seq * 8) + n, bucket_name: bucket)) } } # rubocop:disable ThreadSafety/NewThread
      end
      threads.each(&:join)

      newest = (0...16).to_h { |vb| [vb, ((1..500).select { |seq| seq % 16 == vb }.max * 8) + 7] }

      assert_equal newest, sequences(tracker.mutation_state)
    end

    def store(**)
      ActiveSupport::Cache.lookup_store(:couchbase_store, connection_string: env.connection_string,
                                                          username: env.username, password: env.password, bucket: env.bucket, **)
    end

    def test_store_selects_the_tracker
      default = store
      all = store(mutation_tracking: :all)
      from_config = store(mutation_tracking: "all")
      custom = Object.new.tap do |o|
        def o.record(*); end
        def o.mutation_state; end
      end
      given = store(mutation_tracking: custom)

      assert_instance_of ActiveSupport::Cache::CouchbaseStore::LastMutation, default.instance_variable_get(:@mutations)
      assert_instance_of ActiveSupport::Cache::CouchbaseStore::AllMutations, all.instance_variable_get(:@mutations)
      assert_instance_of ActiveSupport::Cache::CouchbaseStore::AllMutations, from_config.instance_variable_get(:@mutations)
      assert_same custom, given.instance_variable_get(:@mutations)
      assert_raises(ArgumentError) { store(mutation_tracking: :some) }
    end
  end
end
