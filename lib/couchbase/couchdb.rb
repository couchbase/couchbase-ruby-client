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
    attr_accessor :per_page

    def initialize(pool_uri, options = {})
      @per_page = options[:per_page] || 20
      super
    end

    # Fetch design docs stored in current bucket
    def design_docs
      docs = http_get("#{next_node.couch_api_base}/_all_docs",
                      :params => {:startkey => "_design/", :endkey => "_design0", :include_docs => true})
      result = {}
      docs['rows'].each do |doc|
        doc = Document.wrap(self, doc)
        key = doc['_id'].sub(/^_design\//, '')
        next if @environment == :production && key =~ /\$dev_/
        result[key] = doc
      end
      result
    end

    # Update or create design doc with supplied views
    def save_design_doc(id, views, language = 'javascript')
      doc = Document.wrap(self, '_id' => "_design/#{id}", 'language' => language, 'views' => views)
      rv = http_put("#{next_node.couch_api_base}/#{doc['_id']}", {}, doc)
      doc['_rev'] = rv['rev']
      doc
    end

    def delete_design_doc(id, rev = nil)
      if rev.nil?
        ddoc = design_docs[id]
        return nil unless ddoc
        rev = ddoc['_rev']
        id = ddoc['_id']
      else
        id = "_desing/#{id}" unless id =~ /^_design\//
      end
      http_delete("#{next_node.couch_api_base}/#{id}", :params => {:rev => rev})
    end
  end
end
