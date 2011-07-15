# Author:: Couchbase <info@couchbase.com>
# Copyright:: 2011 Couchbase, Inc.
# License:: Apache License, Version 2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

module Couchbase
  module Couchdb
    attr_accessor :uuid_batch_count, :per_page

    def initialize(pool_uri, options = {})
      @uuid_batch_count = options.delete(:uuid_batch_count) || 500
      @per_page = options.delete(:per_page) || 20
      super
    end

    def design_docs(raw = false)
      docs = Couchbase.get("#{bucket.next_node.couch_api_base}/_all_docs",
                           :params => {:startkey => "_design/",
                                       :endkey => "_design0",
                                       :include_docs => true})
      Hash.new.tap do |rv|
        docs['rows'].each do |doc|
          key = doc['id'].sub(/^_design\//, '')
          next if key =~ /$dev_/
          rv[key] = raw ? doc : Document.new(self, doc)
        end
      end
    end

    def save_doc(doc)
      doc['id'] ||= next_uuid
      Couchbase.put("#{bucket.next_node.couch_api_base}/#{doc['id']}", {}, doc)
    end

    protected

    def next_uuid(count = @uuid_batch_count)
      @uuids ||= []
      if @uuids.empty?
        @uuids = Couchbase.get("#{bucket.next_node.couch_api_base}/_uuids",
                               :params => {:count => count})["uuids"]
      end
      @uuids.pop
    end

  end
end
