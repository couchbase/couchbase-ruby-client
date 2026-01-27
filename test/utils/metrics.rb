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

require_relative "metrics/test_meter"
require_relative "metrics/test_value_recorder"

def assert_operation_metrics(
  env,
  count,
  operation_name:,
  service: nil,
  bucket_name: nil,
  scope_name: nil,
  collection_name: nil,
  error: nil
)
  attributes = {
    "db.system.name" => "couchbase",
    "db.operation.name" => operation_name,
    "__unit" => "s",
  }

  if env.server_version.supports_cluster_labels?
    attributes["couchbase.cluster.name"] = env.cluster_name
    attributes["couchbase.cluster.uuid"] = env.cluster_uuid
  end

  attributes["couchbase.service"] = service unless service.nil?
  attributes["db.namespace"] = bucket_name unless bucket_name.nil?
  attributes["couchbase.scope.name"] = scope_name unless scope_name.nil?
  attributes["couchbase.collection.name"] = collection_name unless collection_name.nil?
  attributes["error.type"] = error unless error.nil?

  values = @meter.values(
    "db.client.operation.duration",
    attributes,
    :exact,
  )

  values.each do |v|
    assert_kind_of Integer, v
    assert_predicate v, :positive?
  end

  assert_equal count, values.size,
               "Expected exactly #{count} value(s) for meter db.client.operation.duration and attributes #{attributes.inspect}"
end
