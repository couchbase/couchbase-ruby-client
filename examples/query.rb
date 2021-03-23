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

cluster.bucket("travel-sample") # this is necessary for 6.0.x

options = Cluster::QueryOptions.new
options.named_parameters({type: "hotel"})
options.metrics = true
res = cluster.query("SELECT * FROM `travel-sample` WHERE type = $type LIMIT 10", options)
res.rows.each do |row|
  puts "#{row['travel-sample']['country']}. #{row['travel-sample']['name']}"
end
puts "Status: #{res.meta_data.status}. Execution time: #{res.meta_data.metrics.execution_time}"
