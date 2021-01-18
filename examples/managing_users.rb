#    Copyright 2021 Couchbase, Inc.
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

# Creating Users
user = Management::User.new
user.username = "testUsername"
user.password = "testPassword"
user.display_name = "Constance Lambert"
user.roles = [
  # Roles required for the reading of data from the bucket
  Management::Role.new do |r|
    r.name = "data_reader"
    r.bucket = "*"
  end,
  Management::Role.new do |r|
    r.name = "query_select"
    r.bucket = "*"
  end,

  # Roles required for the writing of data into the bucket
  Management::Role.new do |r|
    r.name = "data_writer"
    r.bucket = "*"
  end,
  Management::Role.new do |r|
    r.name = "query_insert"
    r.bucket = "*"
  end,
  Management::Role.new do |r|
    r.name = "query_delete"
    r.bucket = "*"
  end,

  # Role required for the creation of indexes on the bucket
  Management::Role.new do |r|
    r.name = "query_manage_index"
    r.bucket = "*"
  end,
]
cluster.users.upsert_user(user)

# Listing Users
list_of_users = cluster.users.get_all_users
list_of_users.each do |current_user|
  puts "User's display name is: #{current_user.display_name}"
  current_roles = current_user.roles
  current_roles.each do |role|
    puts "    User has the role: #{role.name}, applicable to bucket #{role.bucket}"
  end
end

# Using a user created in the SDK to access data
bucket_name = "travel-sample"
options = Cluster::ClusterOptions.new
options.authenticate("testUsername", "testPassword")
user_cluster = Cluster.connect("couchbase://localhost", options)
user_bucket = user_cluster.bucket(bucket_name)
user_collection = user_bucket.default_collection

options = Management::QueryIndexManager::CreatePrimaryIndexOptions.new
options.ignore_if_exists = true
cluster.query_indexes.create_primary_index(bucket_name, options)

p returned_airline10_doc: user_collection.get("airline_10").content

airline11_object = {
  "callsign" => "MILE-AIR",
  "iata" => "Q5",
  "icao" => "MLA",
  "id" => 11,
  "name" => "40-Mile Air",
  "type" => "airline",
}
user_collection.upsert("airline_11", airline11_object)

p returned_airline11_doc: user_collection.get("airline_11").content

result = user_cluster.query("SELECT * from `#{bucket_name}` LIMIT 5")
result.rows.each_with_index do |row, idx|
  puts "#{idx}: #{row}"
end

user_cluster.disconnect
