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

require_relative "test_helper"

require "couchbase/raw_binary_transcoder"
require "couchbase/tracing/noop_tracer"

module Couchbase
  # RCBC-567: a failed operation raises after its C++ frame has unwound. The error must reach
  # the caller with the same class, message and backtrace origin.
  class OperationUnwindTest < Minitest::Test
    include TestUtilities

    # Calls the hook when the observability handler records a span reported by the backend,
    # the only caller that passes start_timestamp.
    class HookTracer < Tracing::NoopTracer
      attr_accessor :hook

      def request_span(name, parent: nil, start_timestamp: nil)
        @hook&.call unless start_timestamp.nil?
        super
      end
    end

    DOCUMENT_SIZE = 4 * 1024 * 1024

    def setup
      skip("#{name}: #{Couchbase::Protostellar::NAME} does not record backend spans") if env.protostellar?
      @tracer = HookTracer.new
      connect(Options::Cluster.new(tracer: @tracer))
      @collection = @cluster.bucket(env.bucket).default_collection
      @transcoder = RawBinaryTranscoder.new
      @doc_id = uniq_id(:operation_unwind)
      @content = "x" * DOCUMENT_SIZE
      @cas = @collection.upsert(@doc_id, @content, Options::Upsert.new(transcoder: @transcoder)).cas
      @get_options = Options::Get.new(transcoder: @transcoder)
      @entered = 0
    end

    def teardown
      @tracer&.hook = nil
      @collection&.remove(@doc_id)
      disconnect
    end

    def test_get_of_missing_document_raises_error
      missing_id = uniq_id(:missing)
      100.times do
        error = assert_raises(Error::DocumentNotFound) { @collection.get(missing_id) }

        assert_match(/\Aunable to fetch document: document_not_found \(101\)/, error.message)
        assert_equal "document_get", error.backtrace_locations.first.base_label
      end
    end

    def test_get_with_tracer_not_raising_returns_document
      @tracer.hook = lambda { @entered += 1 }
      res = @collection.get(@doc_id, @get_options)

      assert_operator @entered, :>, 0
      assert_equal @content, res.content
      assert_equal @cas, res.cas
    end
  end
end
