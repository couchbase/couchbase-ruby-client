require "couchbase"
include Couchbase

def measure(msg)
  start = Time.now
  yield
  printf "%s in %.2f seconds\n", msg, Time.now - start
end

options = Cluster::ClusterOptions.new
options.authenticate("Administrator", "password")
cluster = Cluster.connect("couchbase://localhost", options)

search_indexes = cluster.search_indexes.get_all_indexes
puts "There are #{search_indexes.size} search indexes on the cluster"
search_indexes.each do |index|
  num_docs = cluster.search_indexes.get_indexed_documents_count(index.name)
  puts "* #{index.name.inspect} contains #{num_docs} documents"
end

search_index_name = "my_index"
begin
  index = cluster.search_indexes.get_index(search_index_name)
  measure("index #{index.name.inspect} has been dropped") do
    cluster.search_indexes.drop_index(index.name)
  end
rescue Error::IndexNotFound
  # Ignored
end

index = Management::SearchIndex.new
index.type = "fulltext-index"
index.name = search_index_name
index.source_type = "couchbase"
index.source_name = "beer-sample"

measure("index #{search_index_name.inspect} has been created") do
  cluster.search_indexes.upsert_index(index)
end
num_indexed = 0
loop do
  sleep(1)
  num = cluster.search_indexes.get_indexed_documents_count(search_index_name)
  break if num_indexed == num
  num_indexed = num
  puts "#{index.name.inspect} indexed #{num_indexed}"
end

search_indexes = cluster.search_indexes.get_all_indexes
puts "There are #{search_indexes.size} search indexes on the cluster"
search_indexes.each do |index|
  num_docs = cluster.search_indexes.get_indexed_documents_count(index.name)
  puts "* #{index.name.inspect} contains #{num_docs} documents"
end

document = {
    title: "Hello world",
    content: "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
}
analysis = cluster.search_indexes.analyze_document(search_index_name, document)
puts "Analysis of document using definition of the index #{search_index_name}:"
pp analysis
