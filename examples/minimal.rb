# frozen_string_literal: true

require "couchbase/protostellar"

include Couchbase::Protostellar

cluster = Cluster.new("localhost",
                      ConnectOptions.new(username: "Administrator", password: "password"))

collection = cluster.bucket("foo").scope("bar").collection("baz")

collection.upsert("my_document", {"title" => "How to do that", "author" => "John"})
