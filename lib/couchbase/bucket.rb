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
    # In asynchronous mode it will yield result twice, first for
    # {Bucket#get} with {Result#operation} equal to +:get+ and
    # second time for {Bucket#set} with {Result#operation} equal to +:set+.
    #
    # @see http://couchbase.com/docs/memcached-api/memcached-api-protocol-text_cas.html
    #
    # Setting the +:retry+ option to a positive number will cause this method
    # to rescue the {Couchbase::Error::KeyExists} error that happens when
    # an update collision is detected, and automatically get a fresh copy
    # of the value and retry the block. This will repeat as long as there
    # continues to be conflicts, up to the maximum number of retries specified.
    # For asynchronous mode, this means the block will be yielded once for
    # the initial {Bucket#get}, once for the final {Bucket#set} (successful
    # or last failure), and zero or more additional {Bucket#get} retries
    # in between, up to the maximum allowed by the +:retry+ option.
    #
    # @param [String, Symbol] key
    #
    # @param [Hash] options the options for "swap" part
    # @option options [Fixnum] :ttl (self.default_ttl) the time to live of this key
    # @option options [Symbol] :format (self.default_format) format of the value
    # @option options [Fixnum] :flags (self.default_flags) flags for this key
    # @option options [Fixnum] :retry (0) maximum number of times to autmatically retry upon update collision
    #
    # @yieldparam [Object, Result] value old value in synchronous mode and
    #   +Result+ object in asynchronous mode.
    # @yieldreturn [Object] new value.
    #
    # @raise [Couchbase::Error::KeyExists] if the key was updated before the the
    #   code in block has been completed (the CAS value has been changed).
    # @raise [ArgumentError] if the block is missing for async mode
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
    # @example Append JSON encoded value asynchronously
    #
    #     c.default_format = :document
    #     c.set("foo", {"bar" => 1})
    #     c.run do
    #       c.cas("foo") do |val|
    #         case val.operation
    #         when :get
    #           val["baz"] = 2
    #           val
    #         when :set
    #           # verify all is ok
    #           puts "error: #{ret.error.inspect}" unless ret.success?
    #         end
    #       end
    #     end
    #     c.get("foo")      #=> {"bar" => 1, "baz" => 2}
    #
    # @return [Fixnum] the CAS of new value
    def cas(key, options = {})
      retries_remaining = options.delete(:retry) || 0
      if async?
        block = Proc.new
        get(key) do |ret|
          val = block.call(ret) # get new value from caller
          set(ret.key, val, options.merge(:cas => ret.cas, :flags => ret.flags)) do |set_ret|
            if set_ret.error.is_a?(Couchbase::Error::KeyExists) && (retries_remaining > 0)
              cas(key, options.merge(:retry => retries_remaining - 1), &block)
            else
              block.call(set_ret)
            end
          end
        end
      else
        begin
          val, flags, ver = get(key, :extended => true)
          val = yield(val) # get new value from caller
          set(key, val, options.merge(:cas => ver, :flags => flags))
        rescue Couchbase::Error::KeyExists
          if retries_remaining > 0
            retries_remaining -= 1
            retry
          else
            raise
          end
        end
      end
    end
    alias :compare_and_swap :cas

    # Fetch design docs stored in current bucket
    #
    # @since 1.2.0
    #
    # @return [Hash]
    def design_docs
      req = make_http_request("/pools/default/buckets/#{bucket}/ddocs",
                              :type => :management, :extended => true)
      docmap = {}
      req.on_body do |body|
        res = MultiJson.load(body.value)
        res["rows"].each do |obj|
          if obj['doc']
            obj['doc']['value'] = obj['doc'].delete('json')
          end
          doc = DesignDoc.wrap(self, obj)
          key = doc.id.sub(/^_design\//, '')
          next if self.environment == :production && key =~ /dev_/
            docmap[key] = doc
        end
        yield(docmap) if block_given?
      end
      req.continue
      async? ? nil : docmap
    end

    # Update or create design doc with supplied views
    #
    # @since 1.2.0
    #
    # @param [Hash, IO, String] data The source object containing JSON
    #   encoded design document. It must have +_id+ key set, this key
    #   should start with +_design/+.
    #
    # @return [true, false]
    def save_design_doc(data)
      attrs = case data
              when String
                MultiJson.load(data)
              when IO
                MultiJson.load(data.read)
              when Hash
                data
              else
                raise ArgumentError, "Document should be Hash, String or IO instance"
              end
      rv = nil
      id = attrs.delete('_id').to_s
      attrs['language'] ||= 'javascript'
      if id !~ /\A_design\//
        rv = Result.new(:operation => :http_request,
                        :key => id,
                        :error => ArgumentError.new("'_id' key must be set and start with '_design/'."))
        yield rv if block_given?
        raise rv.error unless async?
      end
      req = make_http_request(id, :body => MultiJson.dump(attrs),
                              :method => :put, :extended => true)
      req.on_body do |res|
        rv = res
        val = MultiJson.load(res.value)
        if block_given?
          if res.success? && val['error']
            res.error = Error::View.new("save_design_doc", val['error'])
          end
          yield(res)
        end
      end
      req.continue
      unless async?
        rv.success? or raise res.error
      end
    end

    # Delete design doc with given id and revision.
    #
    # @since 1.2.0
    #
    # @param [String] id Design document id. It might have '_design/'
    #   prefix.
    #
    # @param [String] rev Document revision. It uses latest revision if
    #   +rev+ parameter is nil.
    #
    # @return [true, false]
    def delete_design_doc(id, rev = nil)
      ddoc = design_docs[id.sub(/^_design\//, '')]
      unless ddoc
        yield nil if block_given?
        return nil
      end
      path = Utils.build_query(ddoc.id, :rev => rev || ddoc.meta['rev'])
      req = make_http_request(path, :method => :delete, :extended => true)
      rv = nil
      req.on_body do |res|
        rv = res
        val = MultiJson.load(res.value)
        if block_given?
          if res.success? && val['error']
            res.error = Error::View.new("delete_design_doc", val['error'])
          end
          yield(res)
        end
      end
      req.continue
      unless async?
        rv.success? or raise res.error
      end
    end

    # Delete contents of the bucket
    #
    # @see http://www.couchbase.com/docs/couchbase-manual-2.0/restapi-flushing-bucket.html
    #
    # @since 1.2.0.beta
    #
    # @yieldparam [Result] ret the object with +error+, +status+ and +operation+
    #   attributes.
    #
    # @raise [Couchbase::Error::Protocol] in case of an error is
    #   encountered. Check {Couchbase::Error::Base#status} for detailed code.
    #
    # @return [true] always return true (see raise section)
    #
    # @example Simple flush the bucket
    #   c.flush    #=> true
    #
    # @example Asynchronous flush
    #   c.run do
    #     c.flush do |ret|
    #       ret.operation   #=> :flush
    #       ret.success?    #=> true
    #       ret.status      #=> 200
    #     end
    #   end
    def flush
      if !async? && block_given?
        raise ArgumentError, "synchronous mode doesn't support callbacks"
      end
      req = make_http_request("/pools/default/buckets/#{bucket}/controller/doFlush",
                              :type => :management, :method => :post, :extended => true)
      res = nil
      req.on_body do |r|
        res = r
        res.instance_variable_set("@operation", :flush)
        yield(res) if block_given?
      end
      req.continue
      true
    end

    # Create and register one-shot timer
    #
    # @return [Couchbase::Timer]
    def create_timer(interval, &block)
      Timer.new(self, interval, &block)
    end

    # Create and register periodic timer
    #
    # @return [Couchbase::Timer]
    def create_periodic_timer(interval, &block)
      Timer.new(self, interval, :periodic => true, &block)
    end

    # Wait for persistence condition
    #
    # @since 1.2.0.dp6
    #
    # This operation is useful when some confidence needed regarding the
    # state of the keys. With two parameters +:replicated+ and +:persisted+
    # it allows to set up the waiting rule.
    #
    # @param [String, Symbol, Array, Hash] keys The list of the keys to
    #   observe. Full form is hash with key-cas value pairs, but there are
    #   also shortcuts like just Array of keys or single key. CAS value
    #   needed to when you need to ensure that the storage persisted exactly
    #   the same version of the key you are asking to observe.
    # @param [Hash] options The options for operation
    # @option options [Fixnum] :timeout The timeout in microseconds
    # @option options [Fixnum] :replicated How many replicas should receive
    #   the copy of the key.
    # @option options [Fixnum] :persisted How many nodes should store the
    #   key on the disk.
    #
    # @raise [Couchbase::Error::Timeout] if the given time is up
    #
    # @return [Fixnum, Hash<String, Fixnum>] will return CAS value just like
    #   mutators or pairs key-cas in case of multiple keys.
    def observe_and_wait(*keys, &block)
      options = {:timeout => default_observe_timeout}
      options.update(keys.pop) if keys.size > 1 && keys.last.is_a?(Hash)
      verify_observe_options(options)
      if block && !async?
        raise ArgumentError, "synchronous mode doesn't support callbacks"
      end
      if keys.size == 0
        raise ArgumentError, "at least one key is required"
      end
      if keys.size == 1 && keys[0].is_a?(Hash)
        key_cas = keys[0]
      else
        key_cas = keys.flatten.reduce({}) do |h, kk|
          h[kk] = nil   # set CAS to nil
          h
        end
      end
      if async?
        do_observe_and_wait(key_cas, options, &block)
      else
        res = do_observe_and_wait(key_cas, options, &block) while res.nil?
        unless async?
          if keys.size == 1 && (keys[0].is_a?(String) || keys[0].is_a?(Symbol))
            return res.values.first
          else
            return res
          end
        end
      end
    end

    private

    def verify_observe_options(options)
      unless num_replicas
        raise Couchbase::Error::Libcouchbase, "cannot detect number of the replicas"
      end
      unless options[:persisted] || options[:replicated]
        raise ArgumentError, "either :persisted or :replicated option must be set"
      end
      if options[:persisted] && !(1..num_replicas + 1).include?(options[:persisted])
        raise ArgumentError, "persisted number should be in range (1..#{num_replicas + 1})"
      end
      if options[:replicated] && !(1..num_replicas).include?(options[:replicated])
        raise ArgumentError, "replicated number should be in range (1..#{num_replicas})"
      end
    end

    def do_observe_and_wait(keys, options, &block)
      acc = Hash.new do |h, k|
        h[k] = Hash.new(0)
        h[k][:cas] = [keys[k]] # first position is for master node
        h[k]
      end
      check_condition = lambda do
        ok = catch :break do
          acc.each do |key, stats|
            master = stats[:cas][0]
            if master.nil?
              # master node doesn't have the key
              throw :break
            end
            if options[:persisted] && (stats[:persisted] < options[:persisted] ||
                                       stats[:cas].count(master) != options[:persisted])
              throw :break
            end
            if options[:replicated] && (stats[:replicated] < options[:replicated] ||
                                        stats[:cas].count(master) != options[:replicated] + 1)
              throw :break
            end
          end
          true
        end
        if ok
          if async?
            options[:timer].cancel if options[:timer]
            keys.each do |k, _|
              block.call(Result.new(:key => k,
                                    :cas => acc[k][:cas][0],
                                    :operation => :observe_and_wait))
            end
            return :async
          else
            return keys.inject({}){|res, (k, _)| res[k] = acc[k][:cas][0]; res}
          end
        else
          options[:timeout] /= 2
          if options[:timeout] > 0
            if async?
              options[:timer] = create_timer(options[:timeout]) do
                do_observe_and_wait(keys, options, &block)
              end
              return :async
            else
              # do wait for timeout
              run { create_timer(options[:timeout]){} }
              # return nil to avoid recursive call
              return nil
            end
          else
            err = Couchbase::Error::Timeout.new("the observe request was timed out")
            err.instance_variable_set("@operation", :observe_and_wait)
            if async?
              keys.each do |k, _|
                block.call(Result.new(:key => k,
                                      :cas => acc[k][:cas][0],
                                      :operation => :observe_and_wait,
                                      :error => err))
              end
              return :async
            else
              err.instance_variable_set("@key", keys.keys)
              raise err
            end
          end
        end
      end
      collect = lambda do |results|
        results.each do |res|
          if res.completed?
            check_condition.call if async?
          else
            if res.from_master?
              acc[res.key][:cas][0] = res.cas
            else
              acc[res.key][:cas] << res.cas
            end
            acc[res.key][res.status] += 1
            if res.status == :persisted
              acc[res.key][:replicated] += 1
            end
          end
        end
      end
      if async?
        observe(keys.keys, options, &collect)
      else
        observe(keys.keys, options).each{|_, v| collect.call(v)}
        check_condition.call
      end
    end
  end

end
