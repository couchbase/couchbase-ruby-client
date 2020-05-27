require 'couchbase'
include Couchbase

def measure(msg)
  start = Time.now
  yield
  printf "%s in %.2f seconds\n", msg, Time.now - start
end

def display_indexes(indexes, bucket_name)
  puts "There are #{indexes.size} query indexes in the bucket \"#{bucket_name}\":"
  indexes.each do |index|
    print "  * [#{index.state}] #{index.name}"
    if index.primary?
      print " (primary)"
    end
    unless index.index_key.empty?
      print " on [#{index.index_key.join(", ")}]"
    end
    if index.condition
      print " where #{index.condition}"
    end
    puts
  end
end

bucket_name = "beer-sample"

options = Cluster::ClusterOptions.new
options.authenticate("Administrator", "password")
cluster = Cluster.connect("couchbase://192.168.42.101", options)

display_indexes(cluster.query_indexes.get_all_indexes(bucket_name), bucket_name)

index_name = "demo_index"
options = Management::QueryIndexManager::DropIndexOptions.new
options.ignore_if_does_not_exist = true
measure("Index \"#{index_name}\" has been dropped") { cluster.query_indexes.drop_index(bucket_name, index_name, options) }

options = Management::QueryIndexManager::CreateIndexOptions.new
options.ignore_if_exists = true
options.condition = "abv > 2"
measure("Index \"#{index_name}\" has been created") { cluster.query_indexes.create_index(bucket_name, index_name, %w[`type` `name`], options) }

options = Management::QueryIndexManager::DropPrimaryIndexOptions.new
options.ignore_if_does_not_exist = true
measure("Primary index \"#{bucket_name}\" has been dropped") { cluster.query_indexes.drop_primary_index(bucket_name, options) }

options = Management::QueryIndexManager::CreatePrimaryIndexOptions.new
options.deferred = true
measure("Primary index \"#{bucket_name}\" has been created") { cluster.query_indexes.create_primary_index(bucket_name, options) }

display_indexes(cluster.query_indexes.get_all_indexes(bucket_name), bucket_name)

measure("Build of the indexes for \"#{bucket_name}\" has been triggered") { cluster.query_indexes.build_deferred_indexes(bucket_name) }

display_indexes(cluster.query_indexes.get_all_indexes(bucket_name), bucket_name)

options = Management::QueryIndexManager::WatchIndexesOptions.new
options.watch_primary = true
measure("Watching for primary index build completion for \"#{bucket_name}\" has been finished") { cluster.query_indexes.watch_indexes(bucket_name, [], 10_000_000, options) }

display_indexes(cluster.query_indexes.get_all_indexes(bucket_name), bucket_name)
