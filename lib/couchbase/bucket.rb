# Author:: Couchbase <info@couchbase.com>
# Copyright:: 2011-2017 Couchbase, Inc.
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
    # Setting the +:retry+ option to a positive number will cause this method
    # to rescue the {Couchbase::Error::KeyExists} error that happens when
    # an update collision is detected, and automatically get a fresh copy
    # of the value and retry the block. This will repeat as long as there
    # continues to be conflicts, up to the maximum number of retries specified.
    #
    # @param [String, Symbol] key
    #
    # @param [Hash] options the options for "swap" part
    # @option options [Fixnum] :ttl (self.default_ttl) the time to live of this key
    # @option options [Symbol] :format (self.default_format) format of the value
    # @option options [Fixnum] :flags (self.default_flags) flags for this key
    # @option options [Fixnum] :retry (0) maximum number of times to autmatically retry upon update collision
    #
    # @yieldparam [Object] value old value
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
      retries_remaining = options.delete(:retry) || 0
      begin
        val, flags, ver = get(key, :extended => true)
        val = yield(val) # get new value from caller
        set(key, val, options.merge(:cas => ver, :flags => flags))
      rescue Couchbase::Error::KeyExists
        if retries_remaining > 0
          retries_remaining -= 1
          retry
        end
        raise
      end
    end
    alias compare_and_swap cas

    # Fetch design docs stored in current bucket
    #
    # @since 1.2.0
    #
    # @return [Hash]
    def design_docs
      req = __http_query(:management, :get, "/pools/default/buckets/#{bucket}/ddocs", nil, nil, nil, nil, nil)
      docmap = {}
      res = MultiJson.load(req[:chunks].join)
      res["rows"].each do |obj|
        obj['doc']['value'] = obj['doc'].delete('json') if obj['doc']
        doc = DesignDoc.wrap(self, obj)
        key = doc.id.sub(/^_design\//, '')
        next if environment == :production && key =~ /dev_/
        docmap[key] = doc
      end
      docmap
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
      id = attrs.delete('_id').to_s
      attrs['language'] ||= 'javascript'
      raise ArgumentError, "'_id' key must be set and start with '_design/'." if id !~ /\A_design\//
      res = __http_query(:view, :put, "/#{id}", MultiJson.dump(attrs), 'application/json', nil, nil, nil)
      return true if res[:status] == 201
      val = MultiJson.load(res[:chunks].join)
      raise Error::View.new("save_design_doc", val['error'])
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
      return false unless ddoc
      path = Utils.build_query(ddoc.id, :rev => rev || ddoc.meta['rev'])
      res = __http_query(:view, :delete, path, nil, nil, nil, nil, nil)
      return true if res[:status] == 200
      val = MultiJson.load(res[:chunks].join)
      raise Error::View.new("delete_design_doc", val['error'])
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
    def flush
      bucket.send(:__http_query, :management, :post, "/pools/default/buckets/#{bucket}/controller/doFlush",
                  nil, nil, nil, nil, nil)
      true
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
      raise ArgumentError, "at least one key is required" if keys.empty?
      key_cas = if keys.size == 1 && keys[0].is_a?(Hash)
                  keys[0]
                else
                  keys.flatten.each_with_object({}) do |kk, h|
                    h[kk] = nil # set CAS to nil
                  end
                end
      res = do_observe_and_wait(key_cas, options, &block) while res.nil?
      return res.values.first if keys.size == 1 && (keys[0].is_a?(String) || keys[0].is_a?(Symbol))
      return res
    end

    def fetch(key, ttl = 0)
      cached_obj = get(key)
      return cached_obj if cached_obj
      value = yield
      set(key, value, ttl: ttl)
      value
    end

    private

    def verify_observe_options(options)
      unless num_replicas
        raise Couchbase::Error::Libcouchbase, "cannot detect number of the replicas"
      end
      unless options[:persisted] || options[:replicated]
        raise ArgumentError, "either :persisted or :replicated option must be set"
      end
      if options[:persisted] && !(1..num_replicas + 1).cover?(options[:persisted])
        raise ArgumentError, "persisted number should be in range (1..#{num_replicas + 1})"
      end
      if options[:replicated] && !(1..num_replicas).cover?(options[:replicated])
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
          acc.each do |_key, stats|
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
        return keys.each_with_object({}) { |(k, _), res| res[k] = acc[k][:cas][0] } if ok
        options[:timeout] /= 2
        if options[:timeout] > 0
          # do wait for timeout
          run { create_timer(options[:timeout]) {} }
          # return nil to avoid recursive call
          return nil
        else
          err = Couchbase::Error::Timeout.new("the observe request was timed out")
          err.instance_variable_set("@operation", :observe_and_wait)
          err.instance_variable_set("@key", keys.keys)
          raise err
        end
      end
      collect = lambda do |results|
        results.each do |res|
          next if res.completed?
          if res.from_master?
            acc[res.key][:cas][0] = res.cas
          else
            acc[res.key][:cas] << res.cas
          end
          acc[res.key][res.status] += 1
          acc[res.key][:replicated] += 1 if res.status == :persisted
        end
      end
      observe(keys.keys, options).each { |_, v| collect.call(v) }
      check_condition.call
    end
  end
end
