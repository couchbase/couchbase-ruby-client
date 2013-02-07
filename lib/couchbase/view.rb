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

require 'base64'

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

      def parse_body!
        if @body
          hash = MultiJson.load(@body)
          @type = hash["error"]
          @reason = hash["reason"]
        end
      rescue MultiJson::DecodeError
        @type = @reason = nil
      end

      def to_s
        str = super
        if @type || @reason
          str.sub(/ \(/, ": #{[@type, @reason].compact.join(": ")} (")
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
      attr_accessor :total_rows
      alias total_entries total_rows
    end

    class AsyncHelper # :nodoc:
      include Constants
      EMPTY = []

      def initialize(wrapper_class, bucket, include_docs, quiet, block)
        @wrapper_class = wrapper_class
        @bucket = bucket
        @block = block
        @quiet = quiet
        @include_docs = include_docs
        @queue = []
        @first = @shift = 0
        @completed = false
      end

      # Register object in the emitter.
      def push(obj)
        if @include_docs
          @queue << obj
          @bucket.get(obj[S_ID], :extended => true, :quiet => @quiet) do |res|
            obj[S_DOC] = {
              S_VALUE => res.value,
              S_META => {
                S_ID => obj[S_ID],
                S_FLAGS => res.flags,
                S_CAS => res.cas
              }
            }
            check_for_ready_documents
          end
        else
          old_obj = @queue.shift
          @queue << obj
          block_call(old_obj) if old_obj
        end
      end

      def complete!
        if @include_docs
          @completed = true
        elsif !@queue.empty?
          obj = @queue.shift
          obj[S_IS_LAST] = true
          block_call obj
        end
      end

      private

      def block_call(obj)
        @block.call @wrapper_class.wrap(@bucket, obj)
      end

      def check_for_ready_documents
        shift = @shift
        queue = @queue
        while @first < queue.size + shift
          obj = queue[@first - shift]
          break unless obj[S_DOC]
          queue[@first - shift] = nil
          @first += 1
          if @completed && @first == queue.size + shift
            obj[S_IS_LAST] = true
          end
          block_call obj
        end
        if @first - shift > queue.size / 2
          queue[0, @first - shift] = EMPTY
          @shift = @first
        end
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
    def initialize(bucket, endpoint, params = {})
      @bucket = bucket
      @endpoint = endpoint
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
    #   endpoint = "http://localhost:5984/default/_design/blog/_view/recent"
    #   view = View.new(conn, endpoint, :descending => true)
    #   view.each do |document|
    #     # do something with document
    #   end
    #
    def each(params = {})
      return enum_for(:each, params) unless block_given?
      fetch(params) {|doc| yield(doc)}
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
      self  # enable call chains
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
    def fetch(params = {}, &block)
      params = @params.merge(params)
      include_docs = params.delete(:include_docs)
      quiet = params.delete(:quiet){ true }

      options = {:chunked => true, :extended => true, :type => :view}
      if body = params.delete(:body)
        body = MultiJson.dump(body) unless body.is_a?(String)
        options.update(:body => body, :method => params.delete(:method) || :post)
      end
      path = Utils.build_query(@endpoint, params)
      request = @bucket.make_http_request(path, options)

      if @bucket.async?
        if block
          fetch_async(request, include_docs, quiet, block)
        end
      else
        fetch_sync(request, include_docs, quiet, block)
      end
    end


    # Returns a string containing a human-readable representation of the {View}
    #
    # @return [String]
    def inspect
      %(#<#{self.class.name}:#{self.object_id} @endpoint=#{@endpoint.inspect} @params=#{@params.inspect}>)
    end

    private

    def send_error(*args)
      if @on_error
        @on_error.call(*args.take(2))
      else
        raise Error::View.new(*args)
      end
    end

    def fetch_async(request, include_docs, quiet, block)
      filter = ["/rows/", "/errors/"]
      parser = YAJI::Parser.new(:filter => filter, :with_path => true)
      helper = AsyncHelper.new(@wrapper_class, @bucket, include_docs, quiet, block)

      request.on_body do |chunk|
        if chunk.success?
          parser << chunk.value if chunk.value
          helper.complete! if chunk.completed?
        else
          send_error("http_error", chunk.error)
        end
      end

      parser.on_object do |path, obj|
        case path
        when "/errors/"
          from, reason = obj["from"], obj["reason"]
          send_error(from, reason)
        else
          helper.push(obj)
        end
      end

      request.perform
      nil
    end

    def fetch_sync(request, include_docs, quiet, block)
      res = []
      filter = ["/rows/", "/errors/"]
      unless block
        filter << "/total_rows"
        docs = ArrayWithTotalRows.new
      end
      parser = YAJI::Parser.new(:filter => filter, :with_path => true)
      last_chunk = nil

      request.on_body do |chunk|
        last_chunk = chunk
        res << chunk.value  if chunk.success?
      end

      parser.on_object do |path, obj|
        case path
        when "/total_rows"
          # if total_rows key present, save it and take next object
          docs.total_rows = obj
        when "/errors/"
          from, reason = obj["from"], obj["reason"]
          send_error(from, reason)
        else
          if include_docs
            val, flags, cas = @bucket.get(obj[S_ID], :extended => true, :quiet => quiet)
            obj[S_DOC] = {
              S_VALUE => val,
              S_META => {
                S_ID => obj[S_ID],
                S_FLAGS => flags,
                S_CAS => cas
              }
            }
          end
          doc = @wrapper_class.wrap(@bucket, obj)
          block ? block.call(doc) : docs << doc
        end
      end

      request.continue

      if last_chunk.success?
        while value = res.shift
          parser << value
        end
      else
        send_error("http_error", last_chunk.error, nil)
      end

      # return nil for call with block
      docs
    end
  end
end
