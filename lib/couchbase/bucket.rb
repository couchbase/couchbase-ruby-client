# Author:: Couchbase <info@couchbase.com>
# Copyright:: 2011, 2012 Couchbase, Inc.
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

  class Bucket

    # Compare and swap value.
    #
    # @since 1.0.0
    #
    # Reads a key's value from the server and yields it to a block. Replaces
    # the key's value with the result of the block as long as the key hasn't
    # been updated in the meantime, otherwise raises
    # {Couchbase::Error::KeyExists}. CAS stands for "compare and swap", and
    # avoids the need for manual key mutexing. Read more info here:
    #
    # @see http://couchbase.com/docs/memcached-api/memcached-api-protocol-text_cas.html
    #
    # @param [String, Symbol] key
    #
    # @param [Hash] options the options for operation
    # @option options [String] :ttl (self.default_ttl) the time to live of this key
    # @option options [Symbol] :format (self.default_format) format of the value
    # @option options [Fixnum] :flags (self.default_flags) flags for this key
    #
    # @yieldparam [Object, Result] value old value in synchronous mode and
    #   +Result+ object in asynchronous mode.
    # @yieldreturn [Object] new value.
    #
    # @raise [Couchbase::Error::KeyExists] if the key was updated before the the
    #   code in block has been completed (the CAS value has been changed).
    #
    # @example Implement append to JSON encoded value
    #
    #     c.default_format = :document
    #     c.set("foo", {"bar" => 1})
    #     c.cas("foo") do |val|
    #       val["baz"] = 2
    #       val
    #     end
    #     c.get("foo")      #=> {"bar" => 1, "baz" => 2}
    #
    # @return [Fixnum] the CAS of new value
    def cas(key, options = {})
      options = options.merge(:extended => true)
      if async?
        get(key, options) do |ret|
          val = yield(ret) # get new value from caller
          set(ret.key, val, :cas => ret.cas)
        end
      else
        val, flags, ver = get(key, options)
        val = yield(val) # get new value from caller
        set(key, val, :cas => ver)
      end
    end
    alias :compare_and_swap :cas

    # Fetch design docs stored in current bucket
    #
    # @since 1.2.0
    #
    # @return [ Hash ]
    def design_docs
      docs = all_docs(:startkey => "_design/", :endkey => "_design0", :include_docs => true)
      docmap = {}
      docs.each do |doc|
        key = doc.id.sub(/^_design\//, '')
        next if self.environment == :production && key =~ /dev_/
        docmap[key] = doc
      end
      docmap
    end

    # Fetch all documents from the bucket.
    #
    # @since 1.2.0
    #
    # @param [ Hash ] params Params for CouchDB <tt>/_all_docs</tt> query
    #
    # @return [ Couchbase::View ] View object
    def all_docs(params = {})
      View.new(self, "_all_docs", params)
    end

    # Update or create design doc with supplied views
    #
    # @since 1.2.0
    #
    # @param [ Hash, IO, String ] data The source object containing JSON
    #                                  encoded design document. It must have
    #                                  <tt>_id</tt> key set, this key should
    #                                  start with <tt>_design/</tt>.
    #
    # @return [ true, false ]
    def save_design_doc(data)
      attrs = case data
              when String
                Yajl::Parser.parse(data)
              when IO
                Yajl::Parser.parse(data.read)
              when Hash
                data
              else
                raise ArgumentError, "Document should be Hash, String or IO instance"
              end

      if attrs['_id'].to_s !~ /^_design\//
        raise ArgumentError, "'_id' key must be set and start with '_design/'."
      end
      attrs['language'] ||= 'javascript'
      req = make_couch_request(attrs['_id'],
                               :body => Yajl::Encoder.encode(attrs),
                               :method => :put)
      res = Yajl::Parser.parse(req.perform)
      if res['ok']
        true
      else
        raise "Failed to save design document: #{res['error']}"
      end
    end

    # Delete design doc with given id and revision.
    #
    # @since 1.2.0
    #
    # @param [ String ] id Design document id. It might have '_design/'
    #                      prefix.
    #
    # @param [ String ] rev Document revision. It uses latest revision if
    #                        <tt>rev</tt> parameter is nil.
    #
    # @return [ true, false ]
    def delete_design_doc(id, rev = nil)
      ddoc = design_docs[id.sub(/^_design\//, '')]
      return nil unless ddoc
      path = Utils.build_query(ddoc['_id'], :rev => rev || ddoc['_rev'])
      req = make_couch_request(path, :method => :delete)
      res = Yajl::Parser.parse(req.perform)
      if res['ok']
        true
      else
        raise "Failed to save design document: #{res['error']}"
      end
    end

  end

end
