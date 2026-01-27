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

def assert_kv_dispatch_span(env, span, parent)
  assert_dispatch_span(env, span, parent)

  assert span.attributes.key?("couchbase.local_id")
  assert span.attributes.key?("couchbase.operation_id")
end

def assert_dispatch_span(env, span, parent)
  assert_span env, span, "dispatch_to_server", parent

  assert_equal "tcp", span.attributes["network.transport"]
  assert span.attributes.key?("server.address")
  assert span.attributes.key?("server.port")
  assert span.attributes.key?("network.peer.address")
  assert span.attributes.key?("network.peer.port")
end

def assert_compound_kv_span(
  env, span, op_name, collection, parent,
  child_op_names:, enforce_child_ordering:
)
  puts "Child op names #{child_op_names}"

  assert_span env, span, op_name, parent

  assert_equal "kv", span.attributes["couchbase.service"]
  assert_equal collection.bucket_name, span.attributes["db.namespace"]
  assert_equal collection.scope_name, span.attributes["couchbase.scope.name"]
  assert_equal collection.name, span.attributes["couchbase.collection.name"]

  refute_empty span.children

  if enforce_child_ordering
    span.children.each_with_index do |child_span, index|
      assert_kv_span env, child_span, child_op_names[index], collection, span
    end
  else
    puts "Actual Children spans: #{span.children.map(&:name)}"
    puts "Expected Child spans: #{child_op_names}"

    span.children.each do |child_span|
      refute_empty child_op_names, "Unexpected extra child span: #{child_span.name}"

      index = child_op_names.index(child_span.name)

      refute_nil index, "Unexpected child span: #{child_span.name}"

      span.children.delete_at(index)

      assert_kv_span env, child_span, child_span.name, collection, span
    end
  end
end

def assert_kv_span(env, span, op_name, collection, parent)
  assert_span env, span, op_name, parent

  assert_equal op_name, span.attributes["db.operation.name"]
  assert_equal "kv", span.attributes["couchbase.service"]
  assert_equal collection.bucket_name, span.attributes["db.namespace"]
  assert_equal collection.scope_name, span.attributes["couchbase.scope.name"]
  assert_equal collection.name, span.attributes["couchbase.collection.name"]
  assert span.attributes.key?("couchbase.retries")

  refute_empty span.children

  dispatch_spans = span.children.select { |child| child.name == "dispatch_to_server" }

  refute_empty dispatch_spans
  dispatch_spans.each do |ds|
    assert_dispatch_span(env, ds, span)
  end
end

def assert_has_request_encoding_span(env, span)
  refute_empty span.children

  request_encoding_span = span.children[0] # The request encoding span is always the first child span

  assert_span env, request_encoding_span, "request_encoding", span
end

def assert_compound_http_span(
  env,
  span,
  op_name,
  parent,
  child_count: nil,
  service: nil,
  bucket_name: nil,
  scope_name: nil,
  collection_name: nil,
  statement: nil,
  &
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

  refute_empty span.children, "Expected at least one child span for compound HTTP operation"

  assert_equal child_count, span.children.size unless child_count.nil?

  span.children.each_with_index(&)
end

def assert_http_span(
  env,
  span,
  op_name,
  parent,
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

  refute_empty span.children, "Expected at least one child span for HTTP operation"

  dispatch_spans = span.children.select { |child| child.name == "dispatch_to_server" }

  refute_empty dispatch_spans, "Expected at least one dispatch_to_server child span for HTTP operation"
  dispatch_spans.each do |ds|
    assert_dispatch_span(env, ds, span)
  end
end
