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

module Couchbase
  # A non-String entry in a parameter Hash must raise before the request is sent, rather than end the
  # iteration and send the request without it and every later entry.
  class BackendParametersTest < Minitest::Test
    include TestUtilities

    def setup
      skip("#{name}: the protostellar backend does not use the extension") if env.protostellar?
      connect
      @backend = @cluster.instance_variable_get(:@backend)
    end

    def teardown
      disconnect
    end

    def test_query_raw_parameter_symbol_key
      error = assert_raises(ArgumentError) do
        @backend.document_query("SELECT 1", {raw_parameters: {"before" => "1", sym: "2"}}, nil)
      end
      assert_equal "raw_parameters key must be a String, but given :sym", error.message
    end

    def test_query_named_parameter_symbol_key
      error = assert_raises(ArgumentError) do
        @backend.document_query("SELECT $sym", {named_parameters: {sym: "2"}}, nil)
      end
      assert_equal "named_parameters key must be a String, but given :sym", error.message
    end

    def test_query_raw_parameter_non_string_value
      error = assert_raises(ArgumentError) do
        @backend.document_query("SELECT 1", {raw_parameters: {"key" => 2}}, nil)
      end
      assert_equal "raw_parameters value for \"key\" must be a String, but given 2", error.message
    end

    def test_analytics_raw_parameter_symbol_key
      error = assert_raises(ArgumentError) do
        @backend.document_analytics("SELECT 1", {raw_parameters: {sym: "2"}}, nil)
      end
      assert_equal "raw_parameters key must be a String, but given :sym", error.message
    end

    def test_analytics_named_parameter_symbol_key
      error = assert_raises(ArgumentError) do
        @backend.document_analytics("SELECT $sym", {named_parameters: {sym: "2"}}, nil)
      end
      assert_equal "named_parameters key must be a String, but given :sym", error.message
    end

    def test_search_raw_parameter_symbol_key
      error = assert_raises(ArgumentError) do
        @backend.document_search(nil, nil, "no-such-index", "{}", {}, {raw_parameters: {sym: "2"}}, nil)
      end
      assert_equal "raw_parameters key must be a String, but given :sym", error.message
    end
  end
end
