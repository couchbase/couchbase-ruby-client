# frozen_string_literal: true

#  Copyright 2020-2021 Couchbase, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

require "couchbase"
include Couchbase # rubocop:disable Style/MixinUsage for brevity

def measure(msg)
  start = Time.now
  yield
  printf "%<msg>s in %<elapsed>.2f seconds\n", msg: msg, elapsed: Time.now - start
end

def display_indexes(manager, namespace)
  indexes = manager.get_all_design_documents(namespace)
  puts "\"#{namespace}\" namespace of the bucket \"#{manager.bucket_name}\" contains #{indexes.size} design documents:"
  indexes.each do |index|
    puts "  * #{index.name} (#{index.views.size} views)"
    index.views.each do |name, view|
      puts "    - #{name}"
      puts "      map:\n#{view.map.strip.gsub(/^/, '      | ')}" if view.has_map?
      puts "      reduce:\n#{view.reduce.strip.gsub(/^/, '      | ')}" if view.has_reduce?
    end
  end
end

bucket_name = "beer-sample"

options = Cluster::ClusterOptions.new
options.authenticate("Administrator", "password")
cluster = Cluster.connect("couchbase://localhost", options)

bucket = cluster.bucket(bucket_name)
manager = bucket.view_indexes

display_indexes(manager, :production)

design_document_name = "test"
begin
  manager.drop_design_document(design_document_name, :development)
rescue Error::DesignDocumentNotFound
  # ignore
end

view = Management::View.new
view.map_function = "function (doc, meta) { emit(meta.id, null) }"

design_document = Management::DesignDocument.new
design_document.name = design_document_name
design_document.views["get_all"] = view

manager.upsert_design_document(design_document, :development)

display_indexes(manager, :development)

# copy design document from development namespace to production
manager.publish_design_document(design_document_name)
display_indexes(manager, :production)
