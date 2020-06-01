require 'couchbase'

include Couchbase

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
    "name" => ["Brass", "Doorknob"],
    "email" => "brass.doorknob@example.com",
    "data" => random_number,
})

options = Cluster::QueryOptions.new
options.timeout = 10_000
options.positional_parameters(["Brass"])
options.scan_consistency = :request_plus
res = cluster.query("SELECT name, email, data, META(`default`).id FROM `default` WHERE $1 IN name", options)
res.rows.each do |row|
  puts "Name: #{row["name"].join(" ")}, e-mail: #{row["email"]}, data: #{row["data"]}"
  if row["data"] == random_number
    puts "--- Found our newly created document!"
  end
  if ENV['REMOVE_DOOR_KNOBS']
    puts "Removing #{row["id"]} (requested via env)"
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
    "name" => ["Brass", "Doorknob"],
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
  puts "Name: #{row["name"].join(" ")}, e-mail: #{row["email"]}, data: #{row["data"]}"
  if row["data"] == random_number
    puts "--- Found our newly created document!"
  end
  if ENV['REMOVE_DOOR_KNOBS']
    puts "Removing #{row["id"]} (requested via env)"
    collection.remove(row["id"])
  end
end
