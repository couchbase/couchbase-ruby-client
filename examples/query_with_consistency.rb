#    Copyright 2020 Couchbase, Inc.
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

options = Management::QueryIndexManager::CreatePrimaryIndexOptions.new
options.ignore_if_exists = true
cluster.query_indexes.create_primary_index("default", options)

collection = cluster.bucket("default").default_collection

# METHOD 1
#
# The easiest way to enforce consistency is just set scan_consistency option to :request_plus
# Note: it waits until all documents queued to be indexed when the request has been receive,
# will be proceed and stored in the index. Often more granular approach is possible (skip to the next section)
puts "METHOD 1: using :request_plus"

random_number = rand(0..1_000_000_000)
collection.upsert("user:#{random_number}", {
  "name" => %w[Brass Doorknob],
  "email" => "brass.doorknob@example.com",
  "data" => random_number,
})

options = Cluster::QueryOptions.new
options.timeout = 10_000
options.positional_parameters(["Brass"])
options.scan_consistency = :request_plus
res = cluster.query("SELECT name, email, data, META(`default`).id FROM `default` WHERE $1 IN name", options)
res.rows.each do |row|
  puts "Name: #{row['name'].join(' ')}, e-mail: #{row['email']}, data: #{row['data']}"
  puts "--- Found our newly created document!" if row["data"] == random_number
  if ENV["REMOVE_DOOR_KNOBS"]
    puts "Removing #{row['id']} (requested via env)"
    collection.remove(row["id"])
  end
end

# METHOD 2
#
# More granular and light-weight approach is to use AT_PLUS consistency.
# The application need to to know exactly which mutation affect the result,
# and supply mutation tokens from those operations.
puts "METHOD 2: using MutationState"

random_number = rand(0..1_000_000_000)
res = collection.upsert("user:#{random_number}", {
  "name" => %w[Brass Doorknob],
  "email" => "brass.doorknob@example.com",
  "data" => random_number,
})

state = MutationState.new(res.mutation_token)
# state.add(*tokens) could be used to add more tokens

options = Cluster::QueryOptions.new
options.timeout = 10_000
options.positional_parameters(["Brass"])
options.consistent_with(state)
res = cluster.query("SELECT name, email, data, META(`default`).id FROM `default` WHERE $1 IN name", options)
res.rows.each do |row|
  puts "Name: #{row['name'].join(' ')}, e-mail: #{row['email']}, data: #{row['data']}"
  puts "--- Found our newly created document!" if row["data"] == random_number
  if ENV["REMOVE_DOOR_KNOBS"]
    puts "Removing #{row['id']} (requested via env)"
    collection.remove(row["id"])
  end
end
