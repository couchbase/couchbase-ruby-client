# frozen_string_literal: true

#  Copyright 2026. Couchbase, Inc.
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
  class ViewTest < Minitest::Test
    include TestUtilities

    class NoRowsYet < StandardError; end

    def setup
      skip("#{name}: CAVES does not support views") if use_caves?
      skip("#{name}: The #{Couchbase::Protostellar::NAME} protocol does not support views") if env.protostellar?

      connect
      @bucket = @cluster.bucket(env.bucket)
      skip("#{name}: views need a couchstore bucket") unless couchstore_bucket?

      @tag = uniq_id(:view_raw).delete(".")
      @design_document = "dd_#{@tag}"
      create_design_document
      %w[k1 k2 k3].each do |key|
        @bucket.default_collection.upsert("#{@tag}_#{key}", {"tag" => @tag, "k" => key})
      end
    end

    def teardown
      @bucket&.view_indexes&.drop_design_document(@design_document, :production) if @design_document
    rescue Error::DesignDocumentNotFound
      nil
    ensure
      disconnect
    end

    def test_view_query_without_raw_parameters_returns_every_row
      assert_equal %w[k1 k2 k3], keys(Options::View.new)
    end

    def test_raw_string_value_is_sent_as_given
      options = Options::View.new
      options.raw("limit", "1")

      assert_equal %w[k1], keys(options)
    end

    def test_raw_parameter_with_symbol_key
      options = Options::View.new
      options.raw(:key, '"k2"')

      assert_equal %w[k2], keys(options)
    end

    def test_raw_parameter_set_in_constructor_block
      calls = 0
      options = Options::View.new do |o|
        calls += 1
        o.raw("limit", calls.to_s)
      end

      assert_equal 1, calls
      assert_equal %w[k1], keys(options)
    end

    def test_raw_non_string_value_is_sent_as_json
      options = Options::View.new
      options.raw("limit", 2)

      assert_equal %w[k1 k2], keys(options)
    end

    private

    # Views need a Couchbase bucket on couchstore. Older servers report a nil backend for couchstore.
    def couchstore_bucket?
      settings = @cluster.buckets.get_bucket(env.bucket)
      settings.bucket_type == :couchbase && settings.storage_backend != :magma
    end

    def create_design_document
      document = Management::DesignDocument.new do |dd|
        dd.name = @design_document
        dd.views = {
          "by_k" => Management::View.new("function (doc, meta) { if (doc.tag == '#{@tag}') { emit(doc.k, null); } }"),
        }
      end
      @bucket.view_indexes.upsert_design_document(document, :production)
    end

    # Every query here selects at least one row. Right after the design document is published, a query
    # can still return none.
    def keys(options)
      options.scan_consistency = :request_plus
      retry_for_duration(expected_errors: [Error::DesignDocumentNotFound, Error::ViewNotFound, NoRowsYet]) do
        rows = @bucket.view_query(@design_document, "by_k", options).rows.map(&:key).sort
        raise NoRowsYet if rows.empty?

        rows
      end
    end
  end
end
