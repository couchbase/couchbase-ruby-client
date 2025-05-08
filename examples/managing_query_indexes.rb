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
include Couchbase # rubocop:disable Style/MixinUsage -- for brevity

def measure(msg)
  start = Time.now
  yield
  printf "%<msg>s in %<elapsed>.2f seconds\n", msg: msg, elapsed: Time.now - start
end

def display_indexes(indexes, bucket_name)
  puts "There are #{indexes.size} query indexes in the bucket \"#{bucket_name}\":"
  indexes.each do |index|
    print "  * [#{index.state}] #{index.name}"
    print " (primary)" if index.primary?
    print " on [#{index.index_key.join(', ')}]" unless index.index_key.empty?
    print " where #{index.condition}" if index.condition
    puts
  end
end

bucket_name = "beer-sample"

options = Options::Cluster.new
options.authenticate("Administrator", "password")
cluster = Cluster.connect("couchbase://localhost", options)

display_indexes(cluster.query_indexes.get_all_indexes(bucket_name), bucket_name)

index_name = "demo_index"
measure("Index \"#{index_name}\" has been dropped") do
  cluster.query_indexes.drop_index(bucket_name, index_name,
                                   Management::Options::Query(ignore_if_does_not_exist: true))
end

measure("Index \"#{index_name}\" has been created") do
  cluster.query_indexes.create_index(bucket_name, index_name, %w[`type` `name`],
                                     Management::Options::Query::CreateIndex(
                                       ignore_if_exists: true,
                                       condition: "abv > 2",
                                     ))
end

measure("Primary index \"#{bucket_name}\" has been dropped") do
  cluster.query_indexes.drop_primary_index(bucket_name,
                                           Management::Options::Query::DropPrimaryIndex(ignore_if_does_not_exists: true))
end

measure("Primary index \"#{bucket_name}\" has been created") do
  cluster.query_indexes.create_primary_index(bucket_name,
                                             Management::Options::Query::CreatePrimaryIndex(deferred: true))
end

display_indexes(cluster.query_indexes.get_all_indexes(bucket_name), bucket_name)

measure("Build of the indexes for \"#{bucket_name}\" has been triggered") do
  cluster.query_indexes.build_deferred_indexes(bucket_name)
end

display_indexes(cluster.query_indexes.get_all_indexes(bucket_name), bucket_name)

measure("Watching for primary index build completion for \"#{bucket_name}\" has been finished") do
  cluster.query_indexes.watch_indexes(bucket_name, [], 10_000_000,
                                      Management::Options::Query::WatchIndexes(watch_primary: true))
end

display_indexes(cluster.query_indexes.get_all_indexes(bucket_name), bucket_name)
