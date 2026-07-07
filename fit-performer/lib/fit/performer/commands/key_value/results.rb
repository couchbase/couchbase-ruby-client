# frozen_string_literal: true

#  Copyright 2026-Present Couchbase, Inc.
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

require 'json'

require 'fit/protocol/sdk.workload_pb'
require 'fit/protocol/sdk.kv.commands_pb'
require 'fit/protocol/sdk.kv.rangescan.top_level_pb'
require 'fit/protocol/shared.basic_pb'
require 'fit/protocol/shared.content_pb'

require_relative '../shared_results'

module FIT
  module Performer
    module Commands
      module KeyValue
        class Results < SharedResults
          def self.as_get_result(return_result:, initiated:, content_as:)
            begin
              start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
              result = yield
              end_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            rescue StandardError => e
              return to_exception(error: e, initiated: initiated)
            end
            nanos = ((end_time - start_time) * (10**9)).round
            if return_result
              to_get_result(result: result, initiated: initiated, elapsed_nanos: nanos, content_as: content_as)
            else
              to_success(elapsed_nanos: nanos, initiated: initiated)
            end
          end

          def self.as_get_replica_result(return_result:, initiated:, content_as:)
            begin
              start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
              result = yield
              end_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            rescue StandardError => e
              return to_exception(error: e, initiated: initiated)
            end
            nanos = ((end_time - start_time) * (10**9)).round
            if return_result
              to_get_replica_result(result: result, initiated: initiated, content_as: content_as, elapsed_nanos: nanos)
            else
              to_success(elapsed_nanos: nanos, initiated: initiated)
            end
          end

          def self.as_get_all_replicas_result_stream(initiated:, stream_id:, content_as:)
            begin
              results = yield
            rescue StandardError => e
              return to_exception(error: e, initiated: initiated)
            end

            to_get_all_replicas_result_enumerator(
              enumerator: results.each, initiated: initiated, stream_id: stream_id, content_as: content_as,
            )
          end

          def self.as_mutation_result(return_result:, initiated:)
            begin
              start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
              result = yield
              end_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            rescue StandardError => e
              return to_exception(error: e, initiated: initiated)
            end
            nanos = ((end_time - start_time) * (10**9)).round
            if return_result
              to_mutation_result(result: result, initiated: initiated, elapsed_nanos: nanos)
            else
              to_success(elapsed_nanos: nanos, initiated: initiated)
            end
          end

          def self.as_exists_result(return_result:, initiated:)
            begin
              start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
              result = yield
              end_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            rescue StandardError => e
              return to_exception(error: e, initiated: initiated)
            end
            nanos = ((end_time - start_time) * (10**9)).round
            if return_result
              to_exists_result(result: result, initiated: initiated, elapsed_nanos: nanos)
            else
              to_success(elapsed_nanos: nanos, initiated: initiated)
            end
          end

          def self.as_counter_result(return_result:, initiated:)
            begin
              start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
              result = yield
              end_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            rescue StandardError => e
              return to_exception(error: e, initiated: initiated)
            end
            nanos = ((end_time - start_time) * (10**9)).round
            if return_result
              to_counter_result(result: result, initiated: initiated, elapsed_nanos: nanos)
            else
              to_success(elapsed_nanos: nanos, initiated: initiated)
            end
          end

          def self.as_scan_result_stream(initiated:, stream_id:, content_as:)
            begin
              results = yield
            rescue StandardError => e
              return to_exception(error: e, initiated: initiated)
            end

            to_scan_result_enumerator(
              enumerator: results.each, initiated: initiated, stream_id: stream_id, content_as: content_as,
            )
          end

          def self.as_lookup_in_result(return_result:, initiated:, raw_specs:)
            begin
              start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
              result = yield
              end_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            rescue StandardError => e
              return to_exception(error: e, initiated: initiated)
            end

            nanos = ((end_time - start_time) * (10**9)).round
            if return_result
              to_lookup_in_result(result: result, initiated: initiated, elapsed_nanos: nanos, raw_specs: raw_specs)
            else
              to_success(elapsed_nanos: nanos, initiated: initiated)
            end
          end

          def self.as_lookup_in_replica_result(return_result:, initiated:, raw_specs:)
            begin
              start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
              result = yield
              end_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            rescue StandardError => e
              return to_exception(error: e, initiated: initiated)
            end

            nanos = ((end_time - start_time) * (10**9)).round
            if return_result
              to_lookup_in_result(
                result: result, initiated: initiated, elapsed_nanos: nanos, raw_specs: raw_specs, from_replica: true,
              )
            else
              to_success(elapsed_nanos: nanos, initiated: initiated)
            end
          end

          def self.as_lookup_in_all_replicas_result_stream(initiated:, stream_id:, raw_specs:)
            begin
              results = yield
            rescue StandardError => e
              return to_exception(error: e, initiated: initiated)
            end

            to_lookup_in_all_replicas_result_enumerator(
              enumerator: results.each, initiated: initiated, stream_id: stream_id, raw_specs: raw_specs,
            )
          end

          def self.as_mutate_in_result(return_result:, initiated:, raw_specs:)
            begin
              start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
              result = yield
              end_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            rescue StandardError => e
              return to_exception(error: e, initiated: initiated)
            end

            nanos = ((end_time - start_time) * (10**9)).round
            if return_result
              to_mutate_in_result(result: result, initiated: initiated, elapsed_nanos: nanos, raw_specs: raw_specs)
            else
              to_success(elapsed_nanos: nanos, initiated: initiated)
            end
          end

          def self.to_get_replica_result(result:, initiated:, content_as:, elapsed_nanos: 0)
            get_result = FIT::Protocol::SDK::KV::GetReplicaResult.new(
              cas: result.cas,
              content: get_content(content: result.content, content_as: content_as),
              is_replica: result.is_replica,
            )

            get_result.expiry_time = result.expiry_time.to_i unless result.expiry_time.nil?

            create_run_result(
              sdk_result: FIT::Protocol::SDK::Result.new(get_replica_result: get_result),
              initiated: initiated, elapsed_nanos: elapsed_nanos
            )
          end

          def self.to_get_result(result:, elapsed_nanos:, initiated:, content_as:)
            get_result = FIT::Protocol::SDK::KV::GetResult.new(
              cas: result.cas,
              content: get_content(content: result.content, content_as: content_as),
            )
            get_result.expiry_time = result.expiry_time.to_i unless result.expiry_time.nil?

            create_run_result(
              sdk_result: FIT::Protocol::SDK::Result.new(get_result: get_result),
              initiated: initiated, elapsed_nanos: elapsed_nanos
            )
          end

          def self.to_get_all_replicas_result_enumerator(enumerator:, initiated:, stream_id:, content_as:)
            Enumerator.new do |y|
              loop do
                result = enumerator.next
                y << to_get_all_replicas_result(
                  result: result, initiated: initiated, stream_id: stream_id, content_as: content_as,
                )
              rescue StopIteration
                break
              rescue StandardError => e
                y << to_exception(error: e, initiated: initiated, unwrapped: true)
                break
              end
            end
          end

          def self.to_get_all_replicas_result(result:, initiated:, stream_id:, content_as:)
            get_all_replicas_result = FIT::Protocol::SDK::KV::GetReplicaResult.new(
              cas: result.cas,
              content: get_content(content: result.content, content_as: content_as),
              is_replica: result.is_replica,
              stream_id: stream_id,
            )

            create_run_result(
              sdk_result: FIT::Protocol::SDK::Result.new(get_replica_result: get_all_replicas_result),
              initiated: initiated,
            )
          end

          def self.to_mutation_result(result:, elapsed_nanos:, initiated:)
            mut_result = FIT::Protocol::SDK::KV::MutationResult.new(cas: result.cas)
            mut_result.mutation_token = extract_mutation_token(result) unless result.mutation_token.nil?

            create_run_result(
              sdk_result: FIT::Protocol::SDK::Result.new(mutation_result: mut_result),
              initiated: initiated, elapsed_nanos: elapsed_nanos
            )
          end

          def self.to_exists_result(result:, elapsed_nanos:, initiated:)
            exists_result = FIT::Protocol::SDK::KV::ExistsResult.new(cas: result.cas, exists: result.exists?)

            create_run_result(
              sdk_result: FIT::Protocol::SDK::Result.new(exists_result: exists_result),
              initiated: initiated, elapsed_nanos: elapsed_nanos
            )
          end

          def self.to_counter_result(result:, elapsed_nanos:, initiated:)
            counter_result = FIT::Protocol::SDK::KV::CounterResult.new(cas: result.cas, content: result.content)
            counter_result.mutation_token = extract_mutation_token(result) unless result.mutation_token.nil?

            create_run_result(
              sdk_result: FIT::Protocol::SDK::Result.new(counter_result: counter_result),
              initiated: initiated, elapsed_nanos: elapsed_nanos
            )
          end

          def self.to_scan_result_enumerator(enumerator:, initiated:, stream_id:, content_as:)
            Enumerator.new do |y|
              loop do
                result = enumerator.next
                y << to_scan_result(
                  result: result, initiated: initiated, stream_id: stream_id, content_as: content_as,
                )
              rescue StopIteration
                break
              rescue StandardError => e
                y << to_exception(error: e, initiated: initiated, unwrapped: true)
                break
              end
            end
          end

          def self.to_scan_result(result:, initiated:, stream_id:, content_as:)
            scan_result =
              if result.id_only
                FIT::Protocol::SDK::KV::RangeScan::ScanResult.new(
                  id: result.id, id_only: result.id_only, stream_id: stream_id,
                )
              else
                FIT::Protocol::SDK::KV::RangeScan::ScanResult.new(
                  id: result.id,
                  id_only: result.id_only,
                  cas: result.cas,
                  content: get_content(content: result.content, content_as: content_as),
                  expiry_time: result.expiry,
                  stream_id: stream_id,
                )
              end

            create_run_result(
              sdk_result: FIT::Protocol::SDK::Result.new(range_scan_result: scan_result),
              initiated: initiated,
            )
          end

          def self.to_lookup_in_result(result:, initiated:, raw_specs:, from_replica: false, elapsed_nanos: 0, unwrapped: false)
            lookup_in_result =
              if from_replica
                FIT::Protocol::SDK::KV::LookupIn::LookupInReplicaResult.new
              else
                FIT::Protocol::SDK::KV::LookupIn::LookupInResult.new
              end

            raw_specs.each_with_index do |raw_spec, idx|
              content_or_error = FIT::Protocol::Shared::ContentOrError.new
              begin
                content_or_error.content = get_content(content: result.content(idx), content_as: raw_spec.content_as)
              rescue PerformerError => e
                raise e
              rescue StandardError => e
                content_or_error.exception = to_exception(error: e, unwrapped: true)
              end

              exists_or_error = FIT::Protocol::SDK::KV::LookupIn::BooleanOrError.new
              begin
                exists_or_error.value = result.exists?(idx)
              rescue StandardError => e
                exists_or_error.exception = to_exception(error: e, unwrapped: true)
              end

              lookup_in_result.results << FIT::Protocol::SDK::KV::LookupIn::LookupInSpecResult.new(
                content_as_result: content_or_error,
                exists_result: exists_or_error,
              )
            end

            lookup_in_result.is_replica = result.replica? if from_replica

            return lookup_in_result if unwrapped

            if from_replica
              create_run_result(
                sdk_result: FIT::Protocol::SDK::Result.new(lookup_in_any_replica_result: lookup_in_result),
                initiated: initiated, elapsed_nanos: elapsed_nanos
              )
            else
              create_run_result(
                sdk_result: FIT::Protocol::SDK::Result.new(lookup_in_result: lookup_in_result),
                initiated: initiated, elapsed_nanos: elapsed_nanos
              )
            end
          end

          def self.to_lookup_in_all_replicas_result_enumerator(enumerator:, initiated:, stream_id:, raw_specs:)
            Enumerator.new do |y|
              loop do
                result = enumerator.next
                y << to_lookup_in_all_replicas_result(
                  result: result, initiated: initiated, stream_id: stream_id, raw_specs: raw_specs,
                )
              rescue StopIteration
                break
              rescue StandardError => e
                y << to_exception(error: e, initiated: initiated, unwrapped: true)
                break
              end
            end
          end

          def self.to_lookup_in_all_replicas_result(result:, initiated:, stream_id:, raw_specs:)
            lookup_in_replica_result = to_lookup_in_result(
              result: result, initiated: initiated, raw_specs: raw_specs, from_replica: true, unwrapped: true,
            )
            lookup_in_all_replicas_result = FIT::Protocol::SDK::KV::LookupIn::LookupInAllReplicasResult.new(
              lookup_in_replica_result: lookup_in_replica_result,
              stream_id: stream_id,
            )
            create_run_result(
              sdk_result: FIT::Protocol::SDK::Result.new(lookup_in_all_replicas_result: lookup_in_all_replicas_result),
              initiated: initiated,
            )
          end

          def self.to_mutate_in_result(result:, initiated:, elapsed_nanos:, raw_specs:)
            mutate_in_result = FIT::Protocol::SDK::KV::MutateIn::MutateInResult.new(cas: result.cas)
            mutate_in_result.mutation_token = extract_mutation_token(result) unless result.mutation_token.nil?

            raw_specs.each_with_index do |raw_spec, idx|
              content_or_error = FIT::Protocol::Shared::ContentOrError.new
              begin
                content_or_error.content = get_content(content: result.content(idx), content_as: raw_spec.content_as)
              rescue PerformerError => e
                raise e
              rescue StandardError => e
                content_or_error.exception = to_exception(error: e, unwrapped: true)
              end

              mutate_in_result.results << FIT::Protocol::SDK::KV::MutateIn::MutateInSpecResult.new(
                content_as_result: content_or_error,
              )
            end

            create_run_result(
              sdk_result: FIT::Protocol::SDK::Result.new(mutate_in_result: mutate_in_result),
              initiated: initiated, elapsed_nanos: elapsed_nanos
            )
          end

          def self.extract_mutation_token(sdk_result)
            FIT::Protocol::Shared::MutationToken.new(
              partition_id: sdk_result.mutation_token.partition_id,
              partition_uuid: sdk_result.mutation_token.partition_uuid,
              sequence_number: sdk_result.mutation_token.sequence_number,
              bucket_name: sdk_result.mutation_token.bucket_name,
            )
          end
        end
      end
    end
  end
end
