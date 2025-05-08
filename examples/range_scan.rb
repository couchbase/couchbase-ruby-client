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

require 'couchbase'

include Couchbase # rubocop:disable Style/MixinUsage -- for brevity

options = Options::Cluster.new
options.authenticate("Administrator", "password")
cluster = Cluster.connect("couchbase://localhost", options)

bucket = cluster.bucket("travel-sample")
collection = bucket.scope("inventory").collection("airline")

options = Options::Scan.new(ids_only: false)

puts "Range Scan (from 'airline_1' to 'airline_11')"

scan_type = RangeScan.new(
  from: ScanTerm.new("airline_1"),
  to: ScanTerm.new("airline_11", exclusive: true),
)
res = collection.scan(scan_type, options)

res.each do |item|
  puts " (#{item.id}) #{item.content['icao']}: #{item.content['name']}"
end

puts "\nPrefix Scan (with prefix 'airline_8')"

scan_type = PrefixScan.new("airline_8")
res = collection.scan(scan_type, options)

res.each do |item|
  puts " (#{item.id}) #{item.content['icao']}: #{item.content['name']}"
end

puts "\nSampling Scan (with limit 5)"

scan_type = SamplingScan.new(5)
res = collection.scan(scan_type, options)

res.each do |item|
  puts " (#{item.id}) #{item.content['icao']}: #{item.content['name']}"
end
