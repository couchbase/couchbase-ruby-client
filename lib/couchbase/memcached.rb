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

require 'memcached'

module Couchbase

  # This module is included in the Couchbase::Connection and provides
  # routines for Memcached API

  module Memcached

    attr_reader :memcached, :default_format, :default_flags, :default_ttl

    # Initializes Memcached API. It builds a server list using moxi ports.
    #
    # @param [Hash] options The options for module
    # @option options [Symbol] :format format of the values. It can be on of
    #   the <tt>[:plain, :document, :marshal]</tt>. You can choose
    #   <tt>:plain</tt> if you no need any conversions to be applied to your
    #   data, but your data should be passed as String. Choose
    #   <tt>:document</tt> (default) format when you'll store hashes and plan
    #   to execute CouchDB views with map/reduce operations on them. And
    #   finally use <tt>:marshal</tt> format if you'd like to transparently
    #   serialize almost any ruby object with standard <tt>Marshal.dump</tt>
    #   and <tt>Marhal.load</tt> methods.
    def initialize(pool_uri, options = {})
      @default_format = options[:format] || :document
      @default_flags = ::Memcached::FLAGS
      @options = {
        :binary_protocol => true,
        :support_cas => true,
        :default_ttl => 0
      }.merge(options || {})
      @options[:experimental_features] = true
      if @credentials
        @options[:credentials] = [@credentials[:username], @credentials[:password]]
      end
      super
    end

    # Returns effective options from Memcached instance
    #
    # @return [Hash]
    def options
      @memcached.options
    end

    # Return the array of server strings used to configure this instance.
    #
    # @return [Array]
    def servers
      @memcached.servers
    end

    # Set the prefix key.
    #
    # @param [String] prefix the string to prepend before each key.
    def prefix_key=(prefix)
      @memcached.prefix_key(prefix)
    end
    alias :namespace= :prefix_key=

    # Return the current prefix key.
    #
    # @return [String]
    def prefix_key
      @memcached.prefix_key
    end
    alias :namespace :prefix_key

    # Return a hash of statistics responses from the set of servers. Each
    # value is an array with one entry for each server, in the same order the
    # servers were defined.
    #
    # @param [String] key The name of the statistical item. When key is nil,
    #                     the server will return all set of statistics information.
    #
    # @return [Hash]
    def stats(key = nil)
      @memcached.stats(key)
    end

    # Flushes all key/value pairs from all the servers.
    def flush
      @memcached.flush
    end

    # Gets a key's value from the server. It will use <tt>multiget</tt>
    # behaviour if you pass Array of keys, which is much faster than normal
    # mode.
    #
    # @overload get(key, options = {})
    #   Get single key.
    #
    #   @param [String] key
    #   @param [Hash] options the options for operation
    #   @option options [Symbol] :format (self.default_format) format of the value
    #   @return [Object] the value is associated with the key
    #   @raise [Memcached::NotFound] if the key does not exist on the server.
    #
    # @overload get(*keys, options = {})
    #   Get multiple keys aka multiget (mget).
    #
    #   @param [Array] keys the list of keys
    #   @param [Hash] options the options for operation
    #   @option options [Symbol] :format (self.default_format) format of the value
    #   @return [Hash] result map, where keys are keys which were requested
    #     and values are the values from the storage. Result will contain only
    #     existing keys and in this case no exception will be raised.
    def get(*args)
      options = args.last.is_a?(Hash) ? args.pop : {}
      raise ArgumentError, "You must provide at least one key" if args.empty?
      keys = args.length == 1 ? args.pop : args
      format = options[:format] || @default_format
      rv = @memcached.get(keys, format == :marshal)
      if keys.is_a?(Array)
        rv.keys.each do |key|
          rv[key] = decode(rv[key], format)
        end
        rv
      else
        decode(rv, format)
      end
    end

    # Shortcut to <tt>#get</tt> operation. Gets a key's value from the server
    # with default options. (@see #get method for additional info)
    #
    # @param [Array, String] keys list of String keys or single key
    #
    # @raise [Memcached::NotFound] if the key does not exist on the server.
    def [](keys)
      decode(@memcached.get(keys, @default_format == :marshal), @default_format)
    end

    # Set new expiration time for existing item. The <tt>ttl</tt> parameter
    # will use <tt>#default_ttl</tt> value if it is nil.
    #
    # @param [String] key
    #
    # @param [Fixnum] ttl
    def touch(key, ttl = @default_ttl)
      @memcached.touch(key, ttl)
    end

    # Set a key/value pair. Overwrites any existing value on the server.
    #
    # @param [String] key
    #
    # @param [Object] value
    #
    # @param [Hash] options the options for operation
    # @option options [String] :ttl (self.default_ttl) the time to live of this key
    # @option options [Symbol] :format (self.default_format) format of the value
    # @option options [Fixnum] :flags (self.default_flags) flags for this key
    def set(key, value)
      ttl = options[:ttl] || @default_ttl
      format = options[:format] || @default_format
      flags = options[:flags] || @default_flags
      @memcached.set(key, encode(value, format), ttl, format == :marshal, flags)
    end

    # Shortcut to <tt>#set</tt> operation. Sets key to given value using
    # default options.
    #
    # @param [String] key
    #
    # @param [Object] value
    def []=(key, value)
      @memcached.set(key, encode(value, @default_format),
                     @default_ttl, @default_format == :marshal, @default_flags)
    end

    # Reads a key's value from the server and yields it to a block. Replaces
    # the key's value with the result of the block as long as the key hasn't
    # been updated in the meantime, otherwise raises
    # <b>Memcached::NotStored</b>. CAS stands for "compare and swap", and
    # avoids the need for manual key mutexing. Read more info here:
    #
    #   http://docs.couchbase.org/memcached-api/memcached-api-protocol-text.html#memcached-api-protocol-text_cas
    #
    # @param [String] key
    #
    # @param [Hash] options the options for operation
    # @option options [String] :ttl (self.default_ttl) the time to live of this key
    # @option options [Symbol] :format (self.default_format) format of the value
    # @option options [Fixnum] :flags (self.default_flags) flags for this key
    #
    # @yieldparam [Object] value old value.
    # @yieldreturn [Object] new value.
    #
    # @raise [Memcached::ClientError] if CAS doesn't enabled for current
    #   connection. (:support_cas is true by default)
    # @raise [Memcached::NotStored] if the key was updated before the the
    #   code in block has been completed (the CAS value has been changed).
    #
    # @example Implement append to JSON encoded value
    #
    #     c.default_format  #=> :json
    #     c.set("foo", {"bar" => 1})
    #     c.cas("foo") do |val|
    #       val["baz"] = 2
    #       val
    #     end
    #     c.get("foo")      #=> {"bar" => 1, "baz" => 2}
    #
    def cas(key, options = {}, &block)
      ttl = options[:ttl] || @default_ttl
      format = options[:format] || @default_format
      flags = options[:flags] || @default_flags
      @memcached.cas(key, ttl, format == :marshal, flags) do |value|
        value = decode(value, format)
        encode(block.call(value), format)
      end
    end
    alias :compare_and_swap :cas

    # Add a key/value pair.
    #
    # @param [String] key
    #
    # @param [Object] value
    #
    # @param [Hash] options the options for operation
    # @option options [String] :ttl (self.default_ttl) the time to live of this key
    # @option options [Symbol] :format (self.default_format) format of the value
    # @option options [Fixnum] :flags (self.default_flags) flags for this key
    #
    # @raise [Memcached::NotStored] if the key already exists on the server.
    def add(key, value, options = {})
      ttl = options[:ttl] || @default_ttl
      format = options[:format] || @default_format
      flags = options[:flags] || @default_flags
      @memcached.add(key, encode(value, format), ttl, format == :marshal, flags)
    end


    # Replace a key/value pair.
    #
    # @param [String] key
    #
    # @param [Object] value
    #
    # @param [Hash] options the options for operation
    # @option options [String] :ttl (self.default_ttl) the time to live of this key
    # @option options [Symbol] :format (self.default_format) format of the value
    # @option options [Fixnum] :flags (self.default_flags) flags for this key
    #
    # @raise [Memcached::NotFound] if the key doesn't exists on the server.
    def replace(key, value, options = {})
      ttl = options[:ttl] || @default_ttl
      format = options[:format] || @default_format
      flags = options[:flags] || @default_flags
      @memcached.replace(key, encode(value, format), ttl, format == :marshal, flags)
    end

    # Appends a string to a key's value. Make sense for <tt>:plain</tt>
    # format, because server doesn't make assumptions about value structure
    # here.
    #
    # @param [String] key
    #
    # @param [Object] value
    #
    # @raise [Memcached::NotFound] if the key doesn't exists on the server.
    def append(key, value)
      @memcached.append(key, value)
    end


    # Prepends a string to a key's value. Make sense for <tt>:plain</tt>
    # format, because server doesn't make assumptions about value structure
    # here.
    #
    # @param [String] key
    #
    # @param [Object] value
    #
    # @raise [Memcached::NotFound] if the key doesn't exists on the server.
    def prepend(key, value)
      @memcached.prepend(key, value)
    end

    # Deletes a key/value pair from the server.
    #
    # @param [String] key
    #
    # @raise [Memcached::NotFound] if the key doesn't exists on the server.
    def delete(key)
      @memcached.delete(key)
    end

    # Increment a key's value. The key must be initialized to a plain integer
    # first via <tt>#set</tt>, <tt>#add</tt>, or <tt>#replace</tt> with
    # <tt>:format</tt> set to <tt>:plain</tt>.
    #
    # @param [String] key
    #
    # @param [Fixnum] offset the value to add
    def increment(key, offset = 1)
      @memcached.increment(key, offset)
    end
    alias :incr :increment

    # Decrement a key's value. The key must be initialized to a plain integer
    # first via <tt>#set</tt>, <tt>#add</tt>, or <tt>#replace</tt> with
    # <tt>:format</tt> set to <tt>:plain</tt>.
    #
    # @param [String] key
    #
    # @param [Fixnum] offset the value to substract
    def decrement(key, offset = 1)
      @memcached.decrement(key, offset)
    end
    alias :decr :decrement

    # Safely copy this instance.
    #
    # <tt>clone</tt> is useful for threading, since each thread must have its own unshared object.
    def clone
      double = super
      double.instance_variable_set("@memcached", @memcached.clone)
      double
    end
    alias :dup :clone #:nodoc:

    private

    # Setups memcached instance. Used for dynamic client reconfiguration
    # when server pushes new config.
    def setup(not_used)
      servers = nodes.map do |n|
        "#{n.hostname}:#{n.ports['proxy']}" if n.healthy?
      end.compact
      @memcached = ::Memcached.new(servers, @options)
      @default_ttl = @memcached.options[:default_ttl]
    end

    def encode(value, mode)
      case mode
      when :document
        Yajl::Encoder.encode(value)
      when :marshal, :plain
        value   # encoding handled by memcached library internals
      end
    end

    def decode(value, mode)
      case mode
      when :document
        Yajl::Parser.parse(value)
      when :marshal, :plain
        value   # encoding handled by memcached library internals
      end
    end
  end
end
