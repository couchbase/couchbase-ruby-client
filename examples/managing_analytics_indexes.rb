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

def measure(msg)
  start = Time.now
  yield
  printf "%<msg>s in %<elapsed>.2f seconds\n", msg: msg, elapsed: Time.now - start
end

options = Cluster::ClusterOptions.new
options.authenticate("Administrator", "password")
cluster = Cluster.connect("couchbase://localhost", options)

manager = cluster.analytics_indexes
options = Management::Options::Analytics::DropDataset(
  ignore_if_does_not_exist: true,
  dataverse_name: "beer-data"
)
manager.drop_dataset("beers", options)
manager.drop_dataset("breweries", options)

manager.drop_dataverse("beer-data",
                       Management::Options::Analytics::DropDataverse(ignore_if_does_not_exist: true))

# Creates a dataverse with the name beer-data to be used to manage other metadata entities.
manager.create_dataverse("beer-data")

# Creates 2 datasets beers and breweries on the beer-sample bucket and filters the content for each dataset by the value
# of the type field of the record.
manager.create_dataset("beers", "beer-sample",
                       Management::Options::Analytics::CreateDataset(dataverse_name: "beer-data", condition: '`type` = "beer"'))

manager.create_dataset("breweries", "beer-sample",
                       Management::Options::Analytics::CreateDataset(dataverse_name: "beer-data", condition: '`type` = "brewery"'))

# Creates indexes on the identified fields for the specified types.
options = Management::Options::Analytics::CreateIndex(dataverse_name: "beer-data")
manager.create_index("beers_name_idx", "beers", {"name" => "string"}, options)
manager.create_index("breweries_name_idx", "beers", {"name" => "string"}, options)
manager.create_index("breweries_loc_idx", "beers", {"geo.lon" => "double", "geo.lat" => "double"}, options)

puts "---- Indexes currently defined on the cluster:"
manager.get_all_indexes.each_with_index do |index, i|
  puts "#{i}. #{index.dataverse_name}.#{index.dataset_name}.#{index.name} #{'(primary)' if index.primary?}"
end

# Drops one of the indexes
manager.drop_index("breweries_name_idx", "beers",
                   Management::Options::Analytics::DropIndex(dataverse_name: "beer-data"))

puts "---- Datasets currently defined on the cluster:"
manager.get_all_datasets.each_with_index do |dataset, i|
  puts "#{i}. #{dataset.dataverse_name}.#{dataset.name} (link: #{dataset.link_name}, bucket: #{dataset.bucket_name})"
end

# Connects all datasets to their Data Service buckets and starts shadowing
manager.connect_link(Management::Options::Analytics::ConnectLink(dataverse_name: "beer-data"))

puts "---- Pending mutations: #{manager.get_pending_mutations.inspect}"

manager.disconnect_link(Management::Options::Analytics::DisconnectLink(dataverse_name: "beer-data"))
