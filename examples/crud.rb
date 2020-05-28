require 'couchbase'
include Couchbase

options = Cluster::ClusterOptions.new
options.authenticate("Administrator", "password")
cluster = Cluster.connect("couchbase://localhost", options)
bucket = cluster.bucket("travel-sample")
collection = bucket.default_collection

res = collection.upsert("foo", {"bar" => 42})
p res.cas

res = collection.get("foo")
p res.content
p res.cas

res = collection.remove("foo")
p res

cluster.disconnect
