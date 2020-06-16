require 'couchbase'
include Couchbase

def measure(msg)
  start = Time.now
  yield
  printf "%s in %.2f seconds\n", msg, Time.now - start
end

options = Cluster::ClusterOptions.new
options.authenticate("Administrator", "password")
cluster = Cluster.connect("couchbase://localhost", options)

manager = cluster.analytics_indexes

options = Management::AnalyticsIndexManager::DropDatasetOptions.new
options.ignore_if_does_not_exist = true
options.dataverse_name = "beer-data"
manager.drop_dataset("beers", options)
manager.drop_dataset("breweries", options)

options = Management::AnalyticsIndexManager::DropDataverseOptions.new
options.ignore_if_does_not_exist = true
manager.drop_dataverse("beer-data", options)

# Creates a dataverse with the name beer-data to be used to manage other metadata entities.
manager.create_dataverse("beer-data")

# Creates 2 datasets beers and breweries on the beer-sample bucket and filters the content for each dataset by the value
# of the type field of the record.
options = Management::AnalyticsIndexManager::CreateDatasetOptions.new
options.dataverse_name = "beer-data"
options.condition = '`type` = "beer"'
manager.create_dataset("beers", "beer-sample", options)

options = Management::AnalyticsIndexManager::CreateDatasetOptions.new
options.dataverse_name = "beer-data"
options.condition = '`type` = "brewery"'
manager.create_dataset("breweries", "beer-sample", options)

# Creates indexes on the identified fields for the specified types.
options = Management::AnalyticsIndexManager::CreateIndexOptions.new
options.dataverse_name = "beer-data"
manager.create_index("beers_name_idx", "beers", {"name" => "string"}, options)
manager.create_index("breweries_name_idx", "beers", {"name" => "string"}, options)
manager.create_index("breweries_loc_idx", "beers", {"geo.lon" => "double", "geo.lat" => "double"}, options)

puts "---- Indexes currently defined on the cluster:"
manager.get_all_indexes.each_with_index do |index, i|
  puts "#{i}. #{index.dataverse_name}.#{index.dataset_name}.#{index.name} #{"(primary)" if index.primary?}"
end

# Drops one of the indexes
options = Management::AnalyticsIndexManager::DropIndexOptions.new
options.dataverse_name = "beer-data"
manager.drop_index("breweries_name_idx", "beers", options)

puts "---- Datasets currently defined on the cluster:"
manager.get_all_datasets.each_with_index do |dataset, i|
  puts "#{i}. #{dataset.dataverse_name}.#{dataset.name} (link: #{dataset.link_name}, bucket: #{dataset.bucket_name})"
end

# Connects all datasets to their Data Service buckets and starts shadowing
options = Management::AnalyticsIndexManager::ConnectLinkOptions.new
options.dataverse_name = "beer-data"
manager.connect_link(options)

puts "---- Pending mutations: #{manager.get_pending_mutations.inspect}"

options = Management::AnalyticsIndexManager::DisconnectLinkOptions.new
options.dataverse_name = "beer-data"
manager.disconnect_link(options)
