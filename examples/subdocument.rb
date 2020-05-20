require 'couchbase'
include Couchbase

options = Cluster::ClusterOptions.new
options.authenticate("Administrator", "password")
cluster = Cluster.connect("couchbase://localhost", options)
bucket = cluster.bucket("default")
collection = bucket.default_collection

res = collection.mutate_in("customer123", [
    MutateInSpec.dict_upsert("fax", "311-555-0151")
])
puts "The document has been modified successfully: cas=#{res.cas}"

res = collection.mutate_in("customer123", [
    MutateInSpec.dict_upsert("_framework.model_type", "Customer").xattr,
    MutateInSpec.remove("addresses.billing[2]"),
    MutateInSpec.replace("email", "dougr96@hotmail.com"),
])
puts "The document has been modified successfully: cas=#{res.cas}"

res = collection.lookup_in"customer123", [
    LookupInSpec.get("addresses.delivery.country"),
    LookupInSpec.exists("purchases.pending[-1]"),
]
puts "The customer's delivery country is #{res.content(0)}"
if res.exists?(1)
  puts "The customer has pending purchases"
end
