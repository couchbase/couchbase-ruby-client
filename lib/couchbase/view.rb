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

require 'yaji'

module Couchbase
  class View
    include Enumerable

    # Set up view endpoint and optional params
    #
    # @param [ Couchbase::Bucket ] connection Connection object which
    # stores all info about how to make requests to Couchbase views.
    #
    # @param [ String ] endpoint Full CouchDB view URI.
    #
    # @param [ Hash ] params Optional parameter which will be passed to
    # Couchbase::View#each
    #
    def initialize(connection, endpoint, params = {})
      @connection = connection
      @endpoint = endpoint
      @params = params
    end

    # Yields each document that was fetched by view. It doesn't instantiate
    # all the results because of streaming JSON parser. Returns Enumerator
    # unless block given.
    #
    # @example Use each method with block
    #
    #   view.each do |doc|
    #     # do something with doc
    #   end
    #
    # @example Use Enumerator version
    #
    #   enum = view.each  # request hasn't issued yet
    #   enum.map{|doc| doc.title.upcase}
    #
    # @example Pass options during view initialization
    #
    #   endpoint = "http://localhost:5984/default/_design/blog/_view/recent"
    #   view = View.new(conn, endpoint, :descending => true)
    #   view.each do |document|
    #     # do something with document
    #   end
    #
    # @param [ Hash ] params Params for Couchdb query. Some useful are:
    # :startkey, :startkey_docid, :descending.
    #
    def each(params = {})
      return enum_for(:each, params) unless block_given?
      fetch(params) {|doc| yield(doc)}
    end

    # Registers callback function for handling error objects in view
    # results stream.
    #
    # @yieldparam [String] from Location of the node where error occured
    # @yieldparam [String] reason The reason message describing what
    #   happened.
    #
    # @example Using <tt>#on_error</tt> to log all errors in view result
    #
    #     # JSON-encoded view result
    #     #
    #     # {
    #     #   "total_rows": 0,
    #     #   "rows": [ ],
    #     #   "errors": [
    #     #     {
    #     #       "from": "127.0.0.1:5984",
    #     #       "reason": "Design document `_design/testfoobar` missing in database `test_db_b`."
    #     #     },
    #     #     {
    #     #       "from": "http:// localhost:5984/_view_merge/",
    #     #       "reason": "Design document `_design/testfoobar` missing in database `test_db_c`."
    #     #     }
    #     #   ]
    #     # }
    #
    #     view.on_error do |from, reason|
    #       logger.warn("#{view.inspect} received the error '#{reason}' from #{from}")
    #     end
    #     docs = view.fetch
    #
    # @example More concise example to just count errors
    #
    #     errcount = 0
    #     view.on_error{|f,r| errcount += 1}.fetch
    #
    def on_error(&callback)
      @on_error = callback
      self  # enable call chains
    end

    # Performs query to CouchDB view. This method will stream results if block
    # given or return complete result set otherwise. In latter case it defines
    # method <tt>total_entries</tt> returning <tt>total_rows</tt> entry from
    # CouchDB result object.
    #
    # @param [Hash] params parameters for CouchDB query. See here the full
    #   list: http://wiki.apache.org/couchdb/HTTP_view_API#Querying_Options
    #
    # @yieldparam [Couchbase::Document] document
    #
    # @return [Array] with documents. There will be <tt>total_entries</tt>
    #   method defined on this array if it's possible.
    #
    # @raise [Couchbase::ViewError] when <tt>on_error</tt> callback is nil and
    #   error object found in the result stream.
    #
    def fetch(params = {})
      curl = @connection.curl_easy(@endpoint, :params => @params.merge(params))
      if block_given?
        iter = YAJI::Parser.new(curl).each(["rows/", "errors/"], :with_path => true)
        begin
          loop do
            path, obj = iter.next
            if path == "errors/"
              from, reason = obj["from"], obj["reason"]
              if @on_error
                @on_error.call(from, reason)
              else
                raise ViewError.new(from, reason)
              end
            else
              yield Document.new(self, obj)
            end
          end
        rescue StopIteration
        end
      else
        iter = YAJI::Parser.new(curl).each(["total_rows", "rows/", "errors/"], :with_path => true)
        docs = []
        begin
          path, obj = iter.next
          if path == "total_rows"
            # if total_rows key present, save it and take next object
            total_rows = obj
            path, obj = iter.next
          end
          loop do
            if path == "errors/"
              from, reason = obj["from"], obj["reason"]
              if @on_error
                @on_error.call(from, reason)
              else
                raise ViewError.new(from, reason)
              end
            else
              docs << Document.new(self, obj)
            end
            path, obj = iter.next
          end
        rescue StopIteration
        end
        docs.instance_eval("def total_entries; #{total_rows}; end")
        return docs
      end
    end

    def inspect
      %(#<#{self.class.name}:#{self.object_id} @endpoint=#{@endpoint.inspect} @params=#{@params.inspect}>)
    end
  end
end
