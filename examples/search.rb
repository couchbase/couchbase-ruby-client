require 'couchbase'

include Couchbase

options = Cluster::ClusterOptions.new
options.authenticate("Administrator", "password")
cluster = Cluster.connect("couchbase://localhost", options)

# create index definition, if it does not exist already
search_index_name = "beer_search"

begin
  cluster.search_indexes.get_index(search_index_name)
rescue Error::IndexNotFound
  index = Management::SearchIndex.new
  index.type = "fulltext-index"
  index.name = search_index_name
  index.source_type = "couchbase"
  index.source_name = "beer-sample"
  index.params = {
      mapping: {
          default_datetime_parser: "dateTimeOptional",
          types: {
              "beer" => {
                  properties: {
                      "abv" => {
                          fields: [
                              {
                                  name: "abv",
                                  type: "number",
                                  include_in_all: true,
                                  index: true,
                                  store: true,
                                  docvalues: true,
                              }
                          ]
                      },
                      "category" => {
                          fields: [
                              {
                                  name: "category",
                                  type: "text",
                                  include_in_all: true,
                                  include_term_vectors: true,
                                  index: true,
                                  store: true,
                                  docvalues: true,
                              }
                          ]
                      },
                      "description" => {
                          fields: [
                              {
                                  name: "description",
                                  type: "text",
                                  include_in_all: true,
                                  include_term_vectors: true,
                                  index: true,
                                  store: true,
                                  docvalues: true,
                              }
                          ]
                      },
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
                      "style" => {
                          fields: [
                              {
                                  name: "style",
                                  type: "text",
                                  include_in_all: true,
                                  include_term_vectors: true,
                                  index: true,
                                  store: true,
                                  docvalues: true,
                              }
                          ]
                      },
                      "updated" => {
                          fields: [
                              {
                                  name: "updated",
                                  type: "datetime",
                                  include_in_all: true,
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

# search with facets advanced sort
query = Cluster::SearchQuery.match_phrase("hop beer")
options = Cluster::SearchOptions.new
options.limit = 10
options.fields = %w[name]
options.highlight_style = :html
options.highlight_fields = %w[name description]
options.sort = [
    Cluster::SearchSort.field("abv") do |spec|
      spec.type = :number
    end,
    Cluster::SearchSort.score do |spec|
      spec.desc = true
    end,
    "category",
    Cluster::SearchSort.id,
]

res = cluster.search_query(search_index_name, query, options)
res.rows.each_with_index do |row, idx|
  document = row.fields
  puts "\n#{idx}. #{document["name"]} (id: #{row.id.inspect})"
  row.fragments.each do |field, fragments|
    puts "  * #{field}"
    fragments.each do |fragment|
      puts "      - #{fragment.gsub(/\n/, ' ')}"
    end
  end
end

# search with facets
query = Cluster::SearchQuery.term("beer")
query.field = "type"

options = Cluster::SearchOptions.new
options.facets = {}

term_facet = Cluster::SearchFacet.term("name")
term_facet.size = 3
options.facets["by_name"] = term_facet

date_facet = Cluster::SearchFacet.date_range("updated")
date_facet.add("old", nil, Time.new(2013, 1, 1))
date_facet.size = 3
options.facets["by_time"] = date_facet

numeric_facet = Cluster::SearchFacet.numeric_range("abv")
numeric_facet.size = 3
numeric_facet.add("strong", 4.9, nil)
numeric_facet.add("light", nil, 4.89)
options.facets["by_strength"] = numeric_facet

res = cluster.search_query(search_index_name, query, options)

puts "\n==== Top 3 beer names (out of #{res.facets["by_name"].total}):"
res.facets["by_name"].terms.each_with_index do |term, idx|
  puts "#{idx}. #{term.term} (#{term.count} records)"
end

puts "\n==== Beer by strength (out of #{res.facets["by_strength"].total}):"
res.facets["by_strength"].numeric_ranges.each_with_index do |range, idx|
  puts "#{idx}. #{range.name}, ABV: [#{range.min}..#{range.max}] (#{range.count} records)"
end

puts "\n==== The most recently updated (out of #{res.facets["by_time"].total}):"
res.facets["by_time"].date_ranges.each_with_index do |range, idx|
  puts "#{idx}. #{range.name} [#{range.start_time}..#{range.end_time}] (#{range.count} records)"
end
