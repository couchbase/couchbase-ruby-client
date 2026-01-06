# frozen_string_literal: true

#  Copyright 2025-Present Couchbase, Inc.
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

require_relative "tracing/test_span"
require_relative "tracing/test_tracer"

def assert_span(env, span, name, parent = nil)
  puts JSON.pretty_generate(@tracer.spans[0].to_h)

  assert_equal name, span.name
  assert_equal span.parent, parent
  assert_instance_of Time, span.start_time
  assert_instance_of Time, span.end_time
  assert_operator span.start_time, :<, span.end_time
  assert_equal "couchbase", span.attributes["db.system.name"]

  if env.server_version.supports_cluster_labels?
    assert_equal env.cluster_name, span.attributes["couchbase.cluster.name"]
    assert_equal env.cluster_uuid, span.attributes["couchbase.cluster.uuid"]
  else
    refute span.attributes.key?("couchbase.cluster.name")
    refute span.attributes.key?("couchbase.cluster.uuid")
  end
end

def assert_kv_span(env, span, op_name, parent = nil)
  assert_span env, span, op_name, parent

  assert_equal "kv", span.attributes["couchbase.service"]
  assert_equal @collection.bucket_name, span.attributes["db.namespace"]
  assert_equal @collection.scope_name, span.attributes["couchbase.scope.name"]
  assert_equal @collection.name, span.attributes["couchbase.collection.name"]
end

def assert_has_request_encoding_span(env, span)
  assert_predicate span.children.size, :positive?

  request_encoding_span = span.children[0] # The request encoding span is always the first child span

  assert_span env, request_encoding_span, "request_encoding", span
end

def assert_http_span(
  env,
  span,
  op_name,
  parent: nil,
  service: nil,
  bucket_name: nil,
  scope_name: nil,
  collection_name: nil,
  statement: nil
)
  assert_span env, span, op_name, parent

  if service.nil?
    assert_nil span.attributes["couchbase.service"]
  else
    assert_equal service, span.attributes["couchbase.service"]
  end
  if bucket_name.nil?
    assert_nil span.attributes["couchbase.bucket"]
  else
    assert_equal bucket_name, span.attributes["db.namespace"]
  end
  if scope_name.nil?
    assert_nil span.attributes["couchbase.scope.name"]
  else
    assert_equal scope_name, span.attributes["couchbase.scope.name"]
  end
  if collection_name.nil?
    assert_nil span.attributes["couchbase.collection.name"]
  else
    assert_equal collection_name, span.attributes["couchbase.collection.name"]
  end
  if statement.nil?
    assert_nil span.attributes["db.query.text"]
  else
    assert_equal statement, span.attributes["db.query.text"]
  end
end
