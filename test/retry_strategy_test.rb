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

require "test_helper"

require "couchbase/protostellar/request_generator/kv"

class RetryStrategyTest < Minitest::Test
  KV_OPTIONS = [
    Couchbase::Options::Get, Couchbase::Options::GetAndLock, Couchbase::Options::GetAndTouch,
    Couchbase::Options::Exists, Couchbase::Options::Touch, Couchbase::Options::Unlock,
    Couchbase::Options::Insert, Couchbase::Options::Upsert, Couchbase::Options::Replace,
    Couchbase::Options::Remove, Couchbase::Options::Append, Couchbase::Options::Prepend,
    Couchbase::Options::Increment, Couchbase::Options::Decrement, Couchbase::Options::LookupIn,
    Couchbase::Options::MutateIn, Couchbase::Options::GetMulti, Couchbase::Options::UpsertMulti,
    Couchbase::Options::RemoveMulti, Couchbase::Options::GetReplica
  ].freeze

  def test_kv_options_pass_retry_strategy_to_backend
    KV_OPTIONS.each do |klass|
      assert_equal :fail_fast, klass.new(retry_strategy: :fail_fast).to_backend[:retry_strategy], klass.name
      assert_nil klass.new.to_backend[:retry_strategy], klass.name
    end
  end

  def generator
    Couchbase::Protostellar::RequestGenerator::KV.new("bucket", "scope", "collection")
  end

  def test_protostellar_maps_symbols_to_strategies
    strategies = Couchbase::Protostellar::Retry::Strategies

    assert_same strategies::FailFast::DEFAULT,
                generator.get_request("id", Couchbase::Options::Get.new(retry_strategy: :fail_fast)).retry_strategy
    assert_same strategies::BestEffort::DEFAULT,
                generator.get_request("id", Couchbase::Options::Get.new(retry_strategy: :best_effort)).retry_strategy
    assert_same strategies::BestEffort::DEFAULT, generator.get_request("id", Couchbase::Options::Get.new).retry_strategy
  end

  def test_protostellar_rejects_other_values
    assert_raises(Couchbase::Error::InvalidArgument) do
      generator.get_request("id", Couchbase::Options::Get.new(retry_strategy: proc {}))
    end
  end

  def test_protostellar_replica_reads_ignore_retry_strategy
    request = generator.get_all_replicas_request("id", Couchbase::Options::GetAllReplicas.new(retry_strategy: proc {}))

    assert_same Couchbase::Protostellar::Retry::Strategies::BestEffort::DEFAULT, request.retry_strategy
  end
end
