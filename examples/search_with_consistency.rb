require 'couchbase'

include Couchbase

options = Cluster::ClusterOptions.new
options.authenticate("Administrator", "password")
cluster = Cluster.connect("couchbase://localhost", options)

bucket_name = "default"
# create index definition, if it does not exist already
search_index_name = "knob_search"
begin
  cluster.search_indexes.get_index(search_index_name)
rescue Error::IndexNotFound
  index = Management::SearchIndex.new
  index.type = "fulltext-index"
  index.name = search_index_name
  index.source_type = "couchbase"
  index.source_name = bucket_name
  index.params = {
      mapping: {
          types: {
              "knob" => {
                  properties: {
                      "name" => {
                          fields: [
                              {
                                  name: "name",
                                  type: "text",
                                  include_in_all: true,
                                  include_term_vectors: true,
                                  index: true,
                                  store: true,
                                  docvalues: true,
                              }
                          ]
                      },
                  }
              }
          }
      }
  }

  cluster.search_indexes.upsert_index(index)

  num_indexed = 0
  loop do
    sleep(1)
    num = cluster.search_indexes.get_indexed_documents_count(search_index_name)
    break if num_indexed == num
    num_indexed = num
    puts "indexing #{search_index_name.inspect}: #{num_indexed} documents"
  end
end

collection = cluster.bucket(bucket_name).default_collection

# The application need to to know exactly which mutation affect the result,
# and supply mutation tokens from those operations.
random_string = ("a".."z").to_a.sample(10).join
res = collection.upsert("user:#{random_string}", {
    "name" => "Brass Doorknob",
    "email" => "brass.doorknob@example.com",
    "data" => random_string,
})

state = MutationState.new(res.mutation_token)
# state.add(*tokens) could be used to add more tokens

query = Cluster::SearchQuery.term("doorknob")
options = Cluster::SearchOptions.new
options.timeout = 10_000
options.consistent_with(state)
res = cluster.search_query(search_index_name, query, options)

res.rows.each do |row|
  if row.id == "user:#{random_string}"
    puts "--- Found our newly created document!"
  end
  if ENV['REMOVE_DOOR_KNOBS']
    puts "Removing #{row.id} (requested via env)"
    collection.remove(row.id)
  end
end
