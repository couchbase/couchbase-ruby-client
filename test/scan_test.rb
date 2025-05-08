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
  class ScanTest < Minitest::Test
    include TestUtilities

    BATCH_BYTE_LIMIT_VALUES = [0, 1, 25, 100].freeze
    BATCH_ITEM_LIMIT_VALUES = [0, 1, 25, 100].freeze
    CONCURRENCY_VALUES = [2, 4, 8, 32, 128].freeze

    def setup
      skip("#{name}: CAVES does not support range scan") if use_caves?
      skip("#{name}: Server does not support range scan (#{env.server_version})") unless env.server_version.supports_range_scan?
      skip("#{name}: The #{Couchbase::Protostellar::NAME} protocol does not support history retention") if env.protostellar?

      connect
      @bucket = @cluster.bucket(env.bucket)
      @collection = @bucket.default_collection
      @shared_prefix = "scan-test"
      @test_ids = Set.new

      @mutation_state = MutationState.new

      100.times do |i|
        s = i.to_s.rjust(2, "0")
        id = "#{@shared_prefix}-#{s}"
        res = @collection.upsert(id, {num: i})
        @mutation_state.add(res.mutation_token)
        @test_ids << id
      end
    end

    def teardown
      return unless defined? @test_ids

      @test_ids.each do |id|
        @collection.remove(id)
      end
    end

    def validate_scan(scan_result, expected_ids, ids_only: false)
      items = []
      scan_result.each do |item|
        items << item

        assert_equal(ids_only, item.id_only)
      end
      test_ids_returned = items.to_set(&:id) & @test_ids

      assert_equal(expected_ids.to_set, test_ids_returned)
      assert_equal(expected_ids.size, test_ids_returned.size)

      return if ids_only

      items.each do |item|
        if test_ids_returned.include? item.id
          refute_equal(0, item.cas)
          assert_equal(item.id, "#{@shared_prefix}-#{item.content['num'].to_s.rjust(2, '0')}")
        end
      end
    end

    def validate_sampling_scan(scan_result, limit, ids_only: false)
      items = []
      scan_result.each do |item|
        items << item

        assert_equal(ids_only, item.id_only)
      end
      assert_operator(items.size, :<=, limit)

      return if ids_only

      test_ids_returned = items.to_set(&:id) & @test_ids
      items.each do |item|
        if test_ids_returned.include? item.id
          refute_equal(0, item.cas)
          assert_equal(item.id, "#{@shared_prefix}-#{item.content['num'].to_s.rjust(2, '0')}")
        end
      end
    end

    def test_simple_prefix_scan
      expected_ids = (0..9).map { |i| "#{@shared_prefix}-1#{i}" }
      scan_result = @collection.scan(PrefixScan.new("#{@shared_prefix}-1"), Options::Scan(mutation_state: @mutation_state))
      validate_scan(scan_result, expected_ids)
    end

    def test_simple_range_scan
      expected_ids = (0..9).map { |i| "#{@shared_prefix}-1#{i}" } + (0..9).map { |i| "#{@shared_prefix}-2#{i}" }
      scan_result = @collection.scan(
        RangeScan.new(
          from: ScanTerm.new("#{@shared_prefix}-10"),
          to: ScanTerm.new("#{@shared_prefix}-29"),
        ), Options::Scan(mutation_state: @mutation_state)
      )
      validate_scan(scan_result, expected_ids)
    end

    def test_simple_sampling_scan
      limit = 20
      scan_result = @collection.scan(SamplingScan.new(limit), Options::Scan(mutation_state: @mutation_state))
      validate_sampling_scan(scan_result, limit)
    end

    def test_range_scan_exclusive_from
      expected_ids = (1..9).map { |i| "#{@shared_prefix}-1#{i}" } + (0..9).map { |i| "#{@shared_prefix}-2#{i}" }
      scan_result = @collection.scan(
        RangeScan.new(
          from: ScanTerm.new("#{@shared_prefix}-10", exclusive: true),
          to: ScanTerm.new("#{@shared_prefix}-29"),
        ), Options::Scan(mutation_state: @mutation_state)
      )
      validate_scan(scan_result, expected_ids)
    end

    def test_range_scan_exclusive_to
      expected_ids = (0..9).map { |i| "#{@shared_prefix}-1#{i}" } + (0..8).map { |i| "#{@shared_prefix}-2#{i}" }
      scan_result = @collection.scan(
        RangeScan.new(
          from: ScanTerm.new("#{@shared_prefix}-10", exclusive: false),
          to: ScanTerm.new("#{@shared_prefix}-29", exclusive: true),
        ), Options::Scan(mutation_state: @mutation_state)
      )
      validate_scan(scan_result, expected_ids)
    end

    def test_range_scan_both_exclusive
      expected_ids = (1..9).map { |i| "#{@shared_prefix}-1#{i}" } + (0..8).map { |i| "#{@shared_prefix}-2#{i}" }
      scan_result = @collection.scan(
        RangeScan.new(
          from: ScanTerm.new("#{@shared_prefix}-10", exclusive: true),
          to: ScanTerm.new("#{@shared_prefix}-29", exclusive: true),
        ), Options::Scan(mutation_state: @mutation_state)
      )
      validate_scan(scan_result, expected_ids)
    end

    def test_range_scan_default_from
      expected_ids = (0..9).map { |i| "#{@shared_prefix}-0#{i}" }
      scan_result = @collection.scan(
        RangeScan.new(
          to: ScanTerm.new("#{@shared_prefix}-09"),
        ), Options::Scan(mutation_state: @mutation_state)
      )
      validate_scan(scan_result, expected_ids)
    end

    def test_range_scan_default_to
      expected_ids = (0..9).map { |i| "#{@shared_prefix}-9#{i}" }
      scan_result = @collection.scan(
        RangeScan.new(
          from: ScanTerm.new("#{@shared_prefix}-90"),
        ), Options::Scan(mutation_state: @mutation_state)
      )
      validate_scan(scan_result, expected_ids)
    end

    def test_range_scan_both_default
      scan_result = @collection.scan(RangeScan.new, Options::Scan(mutation_state: @mutation_state))
      validate_scan(scan_result, @test_ids)
    end

    def test_range_scan_ids_only
      expected_ids = (0..9).map { |i| "#{@shared_prefix}-1#{i}" } + (0..9).map { |i| "#{@shared_prefix}-2#{i}" }
      scan_result = @collection.scan(
        RangeScan.new(
          from: ScanTerm.new("#{@shared_prefix}-10"),
          to: ScanTerm.new("#{@shared_prefix}-29"),
        ),
        Options::Scan.new(ids_only: true, mutation_state: @mutation_state),
      )
      validate_scan(scan_result, expected_ids, ids_only: true)
    end

    def test_range_scan_explicitly_with_content
      expected_ids = (0..9).map { |i| "#{@shared_prefix}-1#{i}" } + (0..9).map { |i| "#{@shared_prefix}-2#{i}" }
      scan_result = @collection.scan(
        RangeScan.new(
          from: ScanTerm.new("#{@shared_prefix}-10"),
          to: ScanTerm.new("#{@shared_prefix}-29"),
        ),
        Options::Scan.new(ids_only: false, mutation_state: @mutation_state),
      )
      validate_scan(scan_result, expected_ids)
    end

    def test_prefix_scan_ids_only
      expected_ids = (0..9).map { |i| "#{@shared_prefix}-1#{i}" }
      scan_result = @collection.scan(PrefixScan.new("#{@shared_prefix}-1"),
                                     Options::Scan.new(ids_only: true, mutation_state: @mutation_state))
      validate_scan(scan_result, expected_ids, ids_only: true)
    end

    def test_sampling_scan_ids_only
      limit = 20
      scan_result = @collection.scan(SamplingScan.new(limit), Options::Scan.new(ids_only: true, mutation_state: @mutation_state))
      validate_sampling_scan(scan_result, limit, ids_only: true)
    end

    def test_sampling_scan_with_seed
      limit = 20
      scan_result = @collection.scan(SamplingScan.new(limit, 42), Options::Scan.new(ids_only: true, mutation_state: @mutation_state))
      validate_sampling_scan(scan_result, limit, ids_only: true)
    end

    def test_range_scan_batch_byte_limit
      BATCH_BYTE_LIMIT_VALUES.each do |b|
        expected_ids = (0..9).map { |i| "#{@shared_prefix}-1#{i}" } + (0..9).map { |i| "#{@shared_prefix}-2#{i}" }
        scan_result = @collection.scan(
          RangeScan.new(
            from: ScanTerm.new("#{@shared_prefix}-10"),
            to: ScanTerm.new("#{@shared_prefix}-29"),
          ),
          Options::Scan.new(batch_byte_limit: b, mutation_state: @mutation_state),
        )
        validate_scan(scan_result, expected_ids)
      end
    end

    def test_prefix_scan_batch_byte_limit
      BATCH_BYTE_LIMIT_VALUES.each do |b|
        expected_ids = (0..9).map { |i| "#{@shared_prefix}-1#{i}" }
        scan_result = @collection.scan(PrefixScan.new("#{@shared_prefix}-1"),
                                       Options::Scan.new(batch_byte_limit: b, mutation_state: @mutation_state))
        validate_scan(scan_result, expected_ids)
      end
    end

    def test_sampling_scan_batch_byte_limit
      BATCH_BYTE_LIMIT_VALUES.each do |b|
        limit = 20
        scan_result = @collection.scan(SamplingScan.new(limit), Options::Scan.new(batch_byte_limit: b, mutation_state: @mutation_state))
        validate_sampling_scan(scan_result, limit)
      end
    end

    def test_range_scan_concurrency
      skip("Skipped until CXXCBC-345 is resolved")

      CONCURRENCY_VALUES.each do |c|
        expected_ids = (0..9).map { |i| "#{@shared_prefix}-1#{i}" } + (0..9).map { |i| "#{@shared_prefix}-2#{i}" }
        scan_result = @collection.scan(
          RangeScan.new(
            from: ScanTerm.new("#{@shared_prefix}-10"),
            to: ScanTerm.new("#{@shared_prefix}-29"),
          ),
          Options::Scan.new(concurrency: c, mutation_state: @mutation_state),
        )
        validate_scan(scan_result, expected_ids)
      end
    end

    def test_prefix_scan_concurrency
      skip("Skipped until CXXCBC-345 is resolved")

      CONCURRENCY_VALUES.each do |c|
        expected_ids = (0..9).map { |i| "#{@shared_prefix}-1#{i}" }
        scan_result = @collection.scan(PrefixScan.new("#{@shared_prefix}-1"),
                                       Options::Scan.new(concurrency: c, mutation_state: @mutation_state))
        validate_scan(scan_result, expected_ids)
      end
    end

    def test_sampling_scan_concurrency
      skip("Skipped until CXXCBC-345 is resolved")

      CONCURRENCY_VALUES.each do |c|
        limit = 20
        scan_result = @collection.scan(SamplingScan.new(limit), Options::Scan.new(concurrency: c, mutation_state: @mutation_state))
        validate_sampling_scan(scan_result, limit)
      end
    end

    def test_range_scan_batch_item_limit
      BATCH_ITEM_LIMIT_VALUES.each do |b|
        expected_ids = (0..9).map { |i| "#{@shared_prefix}-1#{i}" } + (0..9).map { |i| "#{@shared_prefix}-2#{i}" }
        scan_result = @collection.scan(
          RangeScan.new(
            from: ScanTerm.new("#{@shared_prefix}-10"),
            to: ScanTerm.new("#{@shared_prefix}-29"),
          ),
          Options::Scan.new(batch_item_limit: b, mutation_state: @mutation_state),
        )
        validate_scan(scan_result, expected_ids)
      end
    end

    def test_prefix_scan_batch_item_limit
      BATCH_ITEM_LIMIT_VALUES.each do |b|
        expected_ids = (0..9).map { |i| "#{@shared_prefix}-1#{i}" }
        scan_result = @collection.scan(PrefixScan.new("#{@shared_prefix}-1"),
                                       Options::Scan.new(batch_item_limit: b, mutation_state: @mutation_state))
        validate_scan(scan_result, expected_ids)
      end
    end

    def test_sampling_scan_batch_item_limit
      limit = 11
      BATCH_ITEM_LIMIT_VALUES.each do |b|
        scan_result = @collection.scan(SamplingScan.new(limit), Options::Scan.new(batch_item_limit: b, mutation_state: @mutation_state))
        validate_sampling_scan(scan_result, limit)
      end
    end

    def test_range_scan_multiple_options
      expected_ids = (0..9).map { |i| "#{@shared_prefix}-1#{i}" } + (0..9).map { |i| "#{@shared_prefix}-2#{i}" }
      scan_result = @collection.scan(
        RangeScan.new(
          from: ScanTerm.new("#{@shared_prefix}-10"),
          to: ScanTerm.new("#{@shared_prefix}-29"),
        ),
        Options::Scan.new(batch_byte_limit: 100, batch_item_limit: 20, ids_only: false, mutation_state: @mutation_state),
      )
      validate_scan(scan_result, expected_ids)
    end

    def test_range_scan_collection_does_not_exist
      collection = @bucket.scope("_default").collection(uniq_id(:nonexistent))
      assert_raises(Error::CollectionNotFound) do
        collection.scan(RangeScan.new,
                        Options::Scan.new(mutation_state: @mutation_state))
      end
    end

    def test_range_scan_same_from_to
      expected_ids = ["#{@shared_prefix}-10"]
      scan_result = @collection.scan(
        RangeScan.new(
          from: ScanTerm.new("#{@shared_prefix}-10"),
          to: ScanTerm.new("#{@shared_prefix}-10"),
        ),
        Options::Scan.new(mutation_state: @mutation_state),
      )
      validate_scan(scan_result, expected_ids)
    end

    def test_range_scan_same_from_to_exclusive
      expected_ids = []
      scan_result = @collection.scan(
        RangeScan.new(
          from: ScanTerm.new("#{@shared_prefix}-10", exclusive: true),
          to: ScanTerm.new("#{@shared_prefix}-10", exclusive: true),
        ),
        Options::Scan.new(mutation_state: @mutation_state),
      )
      validate_scan(scan_result, expected_ids)
    end

    def test_range_scan_inverted_bounds
      expected_ids = []
      scan_result = @collection.scan(
        RangeScan.new(
          from: ScanTerm.new("#{@shared_prefix}-20", exclusive: true),
          to: ScanTerm.new("#{@shared_prefix}-10", exclusive: true),
        ),
        Options::Scan.new(mutation_state: @mutation_state),
      )
      validate_scan(scan_result, expected_ids)
    end

    def test_sampling_scan_non_positive_limit
      collection = @bucket.scope("_default").collection(uniq_id(:nonexistent))
      assert_raises(Error::InvalidArgument) do
        collection.scan(SamplingScan.new(0),
                        Options::Scan.new(mutation_state: @mutation_state))
      end
    end

    def test_range_scan_zero_concurrency
      collection = @bucket.scope("_default").collection(uniq_id(:nonexistent))
      assert_raises(Error::InvalidArgument) do
        collection.scan(RangeScan.new, Options::Scan.new(concurrency: 0, mutation_state: @mutation_state))
      end
    end
  end

  class ScanNotSupportedTest < Minitest::Test
    include TestUtilities

    def setup
      skip("#{name}: Server supports range scan (#{env.server_version})") if !env.protostellar? && env.server_version.supports_range_scan?

      connect
      @bucket = @cluster.bucket(env.bucket)
      @collection = @bucket.default_collection
    end

    def test_range_scan_feature_not_available
      assert_raises(Error::FeatureNotAvailable) do
        @collection.scan(RangeScan.new,
                         Options::Scan.new(mutation_state: @mutation_state))
      end
    end
  end
end
