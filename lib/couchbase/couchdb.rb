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

    # Initializes CouchDB related part of connection.
    #
    # @param [ String ] pool_uri Couchbase pool URI.
    #
    # @param [ Hash ] options Connection options. This is the hash the client
    #                         passed to Couchbase.new to start the session
    def initialize(pool_uri, options = {})
      super
    end

    # Fetch design docs stored in current bucket
    #
    # @return [ Hash ]
    def design_docs
      docs = http_get("#{next_node.couch_api_base}/_all_docs",
                      :params => {:startkey => "_design/", :endkey => "_design0", :include_docs => true})
      result = {}
      docs['rows'].each do |doc|
        doc = Document.wrap(self, doc)
        key = doc['_id'].sub(/^_design\//, '')
        next if @environment == :production && key =~ /dev_/
        result[key] = doc
      end
      result
    end

    # Update or create design doc with supplied views
    #
    # @param [ Hash, IO, String ] data The source object containing JSON
    #                                  encoded design document. It must have
    #                                  <tt>_id</tt> key set, this key should
    #                                  start with <tt>_design/</tt>.
    #
    # @return [ Couchbase::Document ] instance
    def save_design_doc(data)
      doc = parse_design_document(data)
      rv = http_put("#{next_node.couch_api_base}/#{doc['_id']}", {}, doc)
      doc['_rev'] = rv['rev']
      doc
    end

    # Fetch all documents from the bucket.
    #
    # @param [ Hash ] params Params for CouchDB <tt>/_all_docs</tt> query
    #
    # @return [ Couchbase::View ] View object
    def all_docs(params = {})
      View.new(self, "#{next_node.couch_api_base}/_all_docs", params)
    end

    # Delete design doc with given id and revision.
    #
    # @param [ String ] id Design document id. It might have '_design/'
    #                      prefix.
    #
    # @param [ String ] rev Document revision. It uses latest revision if
    #                        <tt>rev</tt> parameter is nil.
    #
    def delete_design_doc(id, rev = nil)
      ddoc = design_docs[id.sub(/^_design\//, '')]
      return nil unless ddoc
      http_delete("#{next_node.couch_api_base}/#{ddoc['_id']}",
                  :params => {:rev => rev || ddoc['_rev']})
    end

    protected

    def parse_design_document(doc)
      data = case doc
             when String
               Yajl::Parser.parse(doc)
             when IO
               Yajl::Parser.parse(doc.read)
             when Hash
               doc
             else
               raise ArgumentError, "Document should be Hash, String or IO instance"
             end

      if data['_id'].to_s !~ /^_design\//
        raise ArgumentError, "'_id' key must be set and start with '_design/'."
      end
      data['language'] ||= 'javascript'
      Document.wrap(self, data)
    end
  end
end
