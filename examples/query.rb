require 'couchbase'

include Couchbase

options = Cluster::ClusterOptions.new
options.authenticate("Administrator", "password")
cluster = Cluster.connect("couchbase://localhost", options)

options = Cluster::QueryOptions.new
options.named_parameters({type: "hotel"})
options.metrics = true
res = cluster.query("SELECT * FROM `travel-sample` WHERE type = $type LIMIT 10", options)
res.rows.each do |row|
  puts "#{row["travel-sample"]["country"]}. #{row["travel-sample"]["name"]}"
end
puts "Status: #{res.meta_data.status}. Execution time: #{res.meta_data.metrics.execution_time}"
