require 'couchbase'
include Couchbase

def measure(msg)
  start = Time.now
  yield
  printf "%s in %.2f seconds\n", msg, Time.now - start
end

bucket_name = "travel-sample"
scope_name = "myapp"

options = Cluster::ClusterOptions.new
options.authenticate("Administrator", "password")
cluster = Cluster.connect("couchbase://localhost", options)
bucket = cluster.bucket(bucket_name)


scopes = bucket.collections.get_all_scopes
puts "There are #{scopes.size} on the bucket \"#{bucket_name}\""

scopes.each do |scope|
  puts "  * \"#{scope.name}\" (#{scope.collections.size})"
  scope.collections.each do |collection|
    puts "    * \"#{collection.name}\""
  end
end

if scopes.any? {|scope| scope.name == scope_name }
  measure("Scope \"#{scope_name}\" has been removed") { bucket.collections.drop_scope(scope_name) }
end
measure("Scope \"#{scope_name}\" has been created") { bucket.collections.create_scope(scope_name) }

scope = bucket.collections.get_scope(scope_name)
puts "Scope \"#{scope_name}\" has #{scope.collections.size} collections"

collection = Management::CollectionSpec.new
collection.scope_name = scope_name
collection.name = "users"
measure("Collection \"#{collection.name}\" on scope \"#{collection.scope_name}\" has been created") do
  bucket.collections.create_collection(collection)
end

scope = bucket.collections.get_scope(scope_name)
puts "Scope \"#{scope_name}\" has #{scope.collections.size} collections"
scope.collections.each do |collection|
  puts "  * \"#{collection.name}\""
end

measure("Collection \"#{collection.name}\" on scope \"#{collection.scope_name}\" has been dropped") do
  bucket.collections.drop_collection(collection)
end

scope = bucket.collections.get_scope(scope_name)
puts "Scope \"#{scope_name}\" has #{scope.collections.size} collections"
scope.collections.each do |collection|
  puts "  * \"#{collection.name}\""
end
