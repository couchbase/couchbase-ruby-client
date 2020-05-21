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

bucket_name = "new_bucket"

all_buckets = cluster.buckets.get_all_buckets
puts "There are #{all_buckets.size} buckets on the cluster"
all_buckets.each do |bucket|
  puts " * #{bucket.name} (type: #{bucket.bucket_type}, quota: #{bucket.ram_quota_mb} MB)"
end

if all_buckets.any? { |bucket| bucket.name == bucket_name }
  measure("Bucket #{bucket_name.inspect} removed") { cluster.buckets.drop_bucket(bucket_name) }
end

settings = Management::BucketSettings.new
settings.name = bucket_name
settings.ram_quota_mb = 100
settings.flush_enabled = true
measure("New bucket #{bucket_name.inspect} created") { cluster.buckets.create_bucket(settings) }

sleep(1)

settings = cluster.buckets.get_bucket(bucket_name)
puts "Bucket #{bucket_name.inspect} settings:"
puts " * healthy?           : #{settings.healthy?}"
puts " * RAM quota          : #{settings.ram_quota_mb}"
puts " * number of replicas : #{settings.num_replicas}"
puts " * flush enabled:     : #{settings.flush_enabled}"
puts " * max TTL            : #{settings.max_expiry}"
puts " * compression mode   : #{settings.compression_mode}"
puts " * replica indexes    : #{settings.replica_indexes}"
puts " * ejection policy    : #{settings.ejection_policy}"

measure("Bucket #{bucket_name.inspect} flushed") { cluster.buckets.flush_bucket(bucket_name) }

cluster.disconnect
