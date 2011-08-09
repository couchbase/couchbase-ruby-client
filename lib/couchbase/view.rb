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

    # Performs query to CouchDB view. This method will stream results if block
    # given or return complete result set otherwise. In latter case it defines
    # method <tt>total_entries</tt> returning <tt>total_rows</tt> entry from
    # CouchDB result object.
    #
    def fetch(params = {})
      curl = @connection.curl_easy(@endpoint, :params => @params.merge(params))
      if block_given?
        iter = YAJI::Parser.new(curl).each("rows/")
        loop { yield Document.new(self, iter.next) } rescue StopIteration
      else
        iter = YAJI::Parser.new(curl).each(["total_rows", "rows/"])
        docs = []
        begin
          total_rows = iter.next
          unless total_rows.is_a?(Numeric)
            # when reduce function used, it doesn't fill 'total_rows'
            docs << Document.new(self, total_rows)
            total_rows = nil
          end
          loop { docs << Document.new(self, iter.next) }
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
