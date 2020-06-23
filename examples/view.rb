# coding: utf-8
#
require 'couchbase'

include Couchbase

options = Cluster::ClusterOptions.new
options.authenticate("Administrator", "password")
cluster = Cluster.connect("couchbase://localhost", options)

bucket = cluster.bucket("beer-sample")
collection = bucket.default_collection

options = Bucket::ViewOptions.new
options.reduce = true
options.group_level = 1
res = bucket.view_query("beer", "by_location", options)
puts "Breweries by country:"
res.rows.each do |row|
  puts "#{row.key.first}: #{row.value} breweries"
end

options = Bucket::ViewOptions.new
options.limit = 10
options.order = :descending
res = bucket.view_query("beer", "brewery_beers", options)
puts "\nTotal documents in 'beer/brewery_beers' index: #{res.meta_data.total_rows}"
puts "Last #{options.limit} documents:"
res.rows.each_with_index do |row|
  doc = collection.get(row.id)
  puts "#{row.id} (type: #{doc.content['type']}, key: #{row.key})"
end


random_number = rand(0..1_000_000_000)
unique_brewery_id = "random_brewery:#{random_number}"
collection.upsert("random_brewery:#{random_number}", {
    "name" => "Random brewery: #{random_number}",
    "type" => "brewery"
})
puts "\nRequest with consistency. Generated brewery name: #{unique_brewery_id}"
options = Bucket::ViewOptions.new
options.start_key = ["random_brewery:"]
options.scan_consistency = :request_plus
res = bucket.view_query("beer", "brewery_beers", options)
res.rows.each do |row|
  if row.id == unique_brewery_id
    puts "Found newly created document: #{collection.get(row.id).content}"
  end
end
