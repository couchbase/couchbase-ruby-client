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

require "couchbase"
include Couchbase # rubocop:disable Style/MixinUsage for brevity

options = Cluster::ClusterOptions.new

options.authenticate("Administrator", "password")
cluster = Cluster.connect("couchbase://localhost", options)

bucket = cluster.bucket("travel-sample")
collection = bucket.default_collection

res = collection.upsert("foo", {"bar" => 42})
puts "Upsert: CAS=#{res.cas}"

res = collection.get("foo")
puts "Get: CAS=#{res.cas}, content=#{res.content.inspect}"

res = collection.remove("foo")
puts "Removed: #{res.inspect}"

cluster.disconnect
