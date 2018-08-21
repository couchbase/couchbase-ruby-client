# Author:: Couchbase <info@couchbase.com>
# Copyright:: 2011-2018 Couchbase, Inc.
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
  module Error
    class View < Base
      attr_reader :from, :reason

      def initialize(from, reason, prefix = "SERVER: ")
        @from = from
        @reason = reason
        super("#{prefix}#{from}: #{reason}")
      end
    end

    class HTTP < Base
      attr_reader :type, :reason

      def to_s
        str = super
        if @type || @reason
          str.sub(/ \(/, ": #{[@type, @reason].compact.join(': ')} (")
        else
          str
        end
      end
    end
  end

  # This class implements Couchbase View execution
  #
  # @see http://www.couchbase.com/docs/couchbase-manual-2.0/couchbase-views.html
  class View
    include Enumerable
    include Constants

    class ArrayWithTotalRows < Array # :nodoc:
      attr_reader :total_rows
      alias total_entries total_rows

      def initialize(rows, total_rows)
        super(rows)
        @total_rows = total_rows
      end
    end

    attr_reader :params

    # Set up view endpoint and optional params
    #
    # @param [Couchbase::Bucket] bucket Connection object which
    #   stores all info about how to make requests to Couchbase views.
    #
    # @param [String] endpoint Full Couchbase View URI.
    #
    # @param [Hash] params Optional parameter which will be passed to
    #   {View#fetch}
    #
    def initialize(bucket, ddoc, view, params = {})
      @bucket = bucket
      @ddoc = ddoc
      @view = view
      @params = {:connection_timeout => 75_000}.merge(params)
      @wrapper_class = params.delete(:wrapper_class) || ViewRow
      unless @wrapper_class.respond_to?(:wrap)
        raise ArgumentError, "wrapper class should reposond to :wrap, check the options"
      end
    end

    # Yields each document that was fetched by view. It doesn't instantiate
    # all the results because of streaming JSON parser. Returns Enumerator
    # unless block given.
    #
    # @param [Hash] params Params for Couchdb query. Some useful are:
    #   :start_key, :start_key_doc_id, :descending. See {View#fetch}.
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
    #   view = View.new(conn, 'blog', 'recent', :descending => true)
    #   view.each do |document|
    #     # do something with document
    #   end
    #
    def each(params = {})
      return enum_for(:each, params) unless block_given?
      fetch(params) { |doc| yield(doc) }
    end

    def first(params = {})
      params = params.merge(:limit => 1)
      fetch(params).first
    end

    def take(n, params = {})
      params = params.merge(:limit => n)
      fetch(params)
    end

    # Registers callback function for handling error objects in view
    # results stream.
    #
    # @yieldparam [String] from Location of the node where error occured
    # @yieldparam [String] reason The reason message describing what
    #   happened.
    #
    # @example Using +#on_error+ to log all errors in view result
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
      self # enable call chains
    end

    # Performs query to Couchbase view. This method will stream results if block
    # given or return complete result set otherwise. In latter case it defines
    # method +total_rows+ returning corresponding entry from
    # Couchbase result object.
    #
    # @note Avoid using +$+ symbol as prefix for properties in your
    #   documents, because server marks with it meta fields like flags and
    #   expiration, therefore dollar prefix is some kind of reserved. It
    #   won't hurt your application. Currently the {ViewRow}
    #   class extracts +$flags+, +$cas+ and +$expiration+ properties from
    #   the document and store them in {ViewRow#meta} hash.
    #
    # @param [Hash] params parameters for Couchbase query.
    # @option params [true, false] :include_docs (false) Include the
    #   full content of the documents in the return. Note that the document
    #   is fetched from the in memory cache where it may have been changed
    #   or even deleted. See also +:quiet+ parameter below to control error
    #   reporting during fetch.
    # @option params [true, false] :quiet (true) Do not raise error if
    #   associated document not found in the memory. If the parameter +true+
    #   will use +nil+ value instead.
    # @option params [true, false] :descending (false) Return the documents
    #   in descending by key order
    # @option params [String, Fixnum, Hash, Array] :key Return only
    #   documents that match the specified key. Will be JSON encoded.
    # @option params [Array] :keys The same as +:key+, but will work for
    #   set of keys. Will be JSON encoded.
    # @option params [String, Fixnum, Hash, Array] :startkey Return
    #   records starting with the specified key. +:start_key+ option should
    #   also work here. Will be JSON encoded.
    # @option params [String] :startkey_docid Document id to start with
    #   (to allow pagination for duplicate startkeys). +:start_key_doc_id+
    #   also should work.
    # @option params [String, Fixnum, Hash, Array] :endkey Stop returning
    #   records when the specified key is reached. +:end_key+ option should
    #   also work here. Will be JSON encoded.
    # @option params [String] :endkey_docid Last document id to include
    #   in the output (to allow pagination for duplicate startkeys).
    #   +:end_key_doc_id+ also should work.
    # @option params [true, false] :inclusive_end (true) Specifies whether
    #   the specified end key should be included in the result
    # @option params [Fixnum] :limit Limit the number of documents in the
    #   output.
    # @option params [Fixnum] :skip Skip this number of records before
    #   starting to return the results.
    # @option params [String, Symbol] :on_error (:continue) Sets the
    #   response in the event of an error. Supported values:
    #   :continue:: Continue to generate view information in the event of an
    #               error, including the error information in the view
    #               response stream.
    #   :stop::     Stop immediately when an error condition occurs. No
    #               further view information will be returned.
    # @option params [Fixnum] :connection_timeout (75000) Timeout before the
    #   view request is dropped (milliseconds)
    # @option params [true, false] :reduce (true) Use the reduction function
    # @option params [true, false] :group (false) Group the results using
    #   the reduce function to a group or single row.
    # @option params [Fixnum] :group_level Specify the group level to be
    #   used.
    # @option params [String, Symbol, false] :stale (:update_after) Allow
    #   the results from a stale view to be used. Supported values:
    #   false::         Force a view update before returning data
    #   :ok::           Allow stale views
    #   :update_after:: Allow stale view, update view after it has been
    #                   accessed
    # @option params [Hash] :body Accepts the same parameters, except
    #   +:body+ of course, but sends them in POST body instead of query
    #   string. It could be useful for really large and complex parameters.
    #
    # @yieldparam [Couchbase::ViewRow] document
    #
    # @return [Array] with documents. There will be +total_entries+
    #   method defined on this array if it's possible.
    #
    # @raise [Couchbase::Error::View] when +on_error+ callback is nil and
    #   error object found in the result stream.
    #
    # @example Query +recent_posts+ view with key filter
    #   doc.recent_posts(:body => {:keys => ["key1", "key2"]})
    #
    # @example Fetch second page of result set (splitted in 10 items per page)
    #   page = 2
    #   per_page = 10
    #   doc.recent_posts(:skip => (page - 1) * per_page, :limit => per_page)
    #
    # @example Simple join using Map/Reduce
    #   # Given the bucket with Posts(:id, :type, :title, :body) and
    #   # Comments(:id, :type, :post_id, :author, :body). The map function
    #   # below (in javascript) will build the View index called
    #   # "recent_posts_with_comments" which will behave like left inner join.
    #   #
    #   #   function(doc) {
    #   #     switch (doc.type) {
    #   #       case "Post":
    #   #         emit([doc.id, 0], null);
    #   #         break;
    #   #       case "Comment":
    #   #         emit([doc.post_id, 1], null);
    #   #         break;
    #   #     }
    #   #   }
    #   #
    #   post_id = 42
    #   doc.recent_posts_with_comments(:start_key => [post_id, 0],
    #                                  :end_key => [post_id, 1],
    #                                  :include_docs => true)
    def fetch(params = {})
      params = @params.merge(params)
      options = {}
      options[:include_docs] = params.delete(:include_docs) if params.key?(:include_docs)
      options[:format] = params.delete(:format) if params.key?(:format)
      options[:transcoder] = params.delete(:transcoder) if params.key?(:transcoder)
      options[:spatial] = params.delete(:spatial) if params.key?(:spatial)

      body = params.delete(:body) || {}
      body = MultiJson.dump(body) unless body.is_a?(String)
      res = @bucket.send(:__view_query, @ddoc, @view, Utils.encode_params(params), body, options)
      raise res[:error] if res[:error]
      meta = MultiJson.load(res[:meta])
      send_error(meta[S_ERRORS][S_FROM], meta[S_ERRORS][S_REASON]) if meta[S_ERRORS]
      if block_given?
        res[:rows].each do |row|
          yield @wrapper_class.wrap(row)
        end
      else
        rows = res[:rows].map { |row| @wrapper_class.wrap(row) }
        ArrayWithTotalRows.new(rows, meta[S_TOTAL_ROWS])
      end
    end

    # Returns a string containing a human-readable representation of the {View}
    #
    # @return [String]
    def inspect
      %(#<#{self.class.name}:#{object_id} @ddoc=#{@ddoc.inspect} @view=#{@view.inspect} @params=#{@params.inspect}>)
    end

    private

    def send_error(from, reason)
      raise Error::View.new(from, reason) unless @on_error
      @on_error.call(from, reason)
    end
  end
end
