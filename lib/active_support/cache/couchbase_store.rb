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

require 'couchbase'
require 'securerandom'
require 'active_support/core_ext/array/extract_options'
require 'active_support/cache'
require 'monitor'

module ActiveSupport
  module Cache
    # This class implements Cache interface for Rails. To use it just
    # put following line in your config/application.rb file:
    #
    #   config.cache_store = :couchbase_store
    #
    # You can also pass additional connection options there
    #
    #   cache_options = {
    #     :bucket => 'protected',
    #     :username => 'protected',
    #     :password => 'secret',
    #     :expires_in => 30.seconds
    #   }
    #   config.cache_store = :couchbase_store, cache_options
    class CouchbaseStore < Store

      # Creates a new CouchbaseStore object, with the given options. For
      # more info see {{Couchbase::Bucket#initialize}}
      #
      #   ActiveSupport::Cache::CouchbaseStore.new(:bucket => "cache")
      #
      # If no options are specified, then CouchbaseStore will connect to
      # localhost port 8091 (default Couchbase Server port) and will use
      # bucket named "default" which is always open for unauthorized access
      # (if exists).
      def initialize(*args)
        args = [*(args.flatten)]
        options = args.extract_options! || {}
        @raise_errors = !options[:quiet] = !options.delete(:raise_errors)
        options[:default_ttl] ||= options.delete(:expires_in)
        options[:default_format] ||= :marshal
        options[:key_prefix] ||= options.delete(:namespace)
        @key_prefix = options[:key_prefix]
        options[:connection_pool] ||= options.delete(:connection_pool)
        args.push(options)

        if options[:connection_pool]
          if RUBY_VERSION.to_f < 1.9
            warn "connection_pool gem doesn't support ruby < 1.9"
          else
            @data = ::Couchbase::ConnectionPool.new(options[:connection_pool], *args)
          end
        end
        unless @data
          @data = ::Couchbase::Bucket.new(*args)
          @data.extend(Threadsafe)
        end
      end

      # Fetches data from the cache, using the given key.
      #
      # @since 1.2.0.dp5
      #
      # If there is data in the cache with the given key, then that data is
      # returned. If there is no such data in the cache (a cache miss),
      # then nil will be returned. However, if a block has been passed, that
      # block will be run in the event of a cache miss. The return value of
      # the block will be written to the cache under the given cache key,
      # and that return value will be returned.
      #
      # @param [String] name name for the key
      # @param [Hash] options
      # @option options [true, false] :force if this option is +true+ it
      #   will force cache miss.
      # @option options [Fixnum] :expires_in the expiration time on the
      #   cache in seconds. Values larger than 30*24*60*60 seconds (30 days)
      #   are interpreted as absolute times (from the epoch).
      # @option options [true, false] :unless_exists if this option is +true+
      #   it will write value only if the key doesn't exist in the database
      #   (it accepts +:unless_exist+ too).
      #
      # @return [Object]
      def fetch(name, options = nil)
        options ||= {}
        name = expanded_key(name)

        if block_given?
          unless options[:force]
            entry = instrument(:read, name, options) do |payload|
              payload[:super_operation] = :fetch if payload
              read_entry(name, options)
            end
          end

          if !entry.nil?
            instrument(:fetch_hit, name, options) { |payload| }
            entry
          else
            result = instrument(:generate, name, options) do |payload|
              yield
            end
            write(name, result, options)
            result
          end
        else
          read(name, options)
        end
      end

      # Writes the value to the cache, with the key
      #
      # @since 1.2.0.dp5
      #
      # @param [String] name name for the key
      # @param [Object] value value of the key
      # @param [Hash] options
      # @option options [Fixnum] :expires_in the expiration time on the
      #   cache in seconds. Values larger than 30*24*60*60 seconds (30 days)
      #   are interpreted as absolute times (from the epoch).
      #
      # @return [Fixnum, false] false in case of failure and CAS value
      #   otherwise (it could be used as true value)
      def write(name, value, options = nil)
        options ||= {}
        name = expanded_key name
        if options.delete(:raw)
          options[:format] = :plain
          value = value.to_s
          value.force_encoding(Encoding::BINARY) if defined?(Encoding)
        end

        instrument(:write, name, options) do |payload|
          write_entry(name, value, options)
        end
      end

      # Fetches data from the cache, using the given key.
      #
      # @since 1.2.0.dp5
      #
      # If there is data in the cache with the given key, then that data is
      # returned.  Otherwise, nil is returned.
      #
      # @param [String] name name for the key
      # @param [Hash] options
      # @option options [Fixnum] :expires_in the expiration time on the
      #   cache in seconds. Values larger than 30*24*60*60 seconds (30 days)
      #   are interpreted as absolute times (from the epoch).
      # @option options [true, false] :raw do not marshal the value if this
      #   option is +true+
      #
      # @return [Object]
      def read(name, options = nil)
        options ||= {}
        name = expanded_key name
        if options.delete(:raw)
          options[:format] = :plain
        end

        instrument(:read, name, options) do |payload|
          entry = read_entry(name, options)
          payload[:hit] = !!entry if payload
          entry
        end
      end

      # Read multiple values at once from the cache.
      #
      # @since 1.2.0.dp5
      #
      # Options can be passed in the last argument.
      #
      # Returns a hash mapping the names provided to the values found.
      #
      # @return [Hash] key-value pairs
      def read_multi(*names)
        options = names.extract_options!
        names = names.flatten.map{|name| expanded_key(name)}
        options[:assemble_hash] = true
        if options.delete(:raw)
          options[:format] = :plain
        end
        instrument(:read_multi, names, options) do
          @data.get(names, options)
        end
      rescue ::Couchbase::Error::Base => e
        logger.error("#{e.class}: #{e.message}") if logger
        raise if @raise_errors
        false
      end

      # Return true if the cache contains an entry for the given key.
      #
      # @since 1.2.0.dp5
      #
      # @return [true, false]
      def exists?(name, options = nil)
        options ||= {}
        name = expanded_key name

        instrument(:exists?, name) do
          !read_entry(name, options).nil?
        end
      end
      alias :exist? :exists?

      # Deletes an entry in the cache.
      #
      # @since 1.2.0.dp5
      #
      # @return [true, false] true if an entry is deleted
      def delete(name, options = nil)
        options ||= {}
        name = expanded_key name

        instrument(:delete, name) do
          delete_entry(name, options)
        end
      end

      # Increment an integer value in the cache.
      #
      # @since 1.2.0.dp5
      #
      # @param [String] name name for the key
      # @param [Fixnum] amount (1) the delta value
      # @param [Hash] options
      # @option options [Fixnum] :expires_in the expiration time on the
      #   cache in seconds. Values larger than 30*24*60*60 seconds (30 days)
      #   are interpreted as absolute times (from the epoch).
      # @option options [Fixnum] :initial (1) this option allows to initialize
      #   the value if the key is missing in the cache
      #
      # @return [Fixnum] new value
      def increment(name, amount = 1, options = nil)
        options ||= {}
        name = expanded_key name

        if ttl = options.delete(:expires_in)
          options[:ttl] ||= ttl
        end
        options[:create] = true
        instrument(:increment, name, options) do |payload|
          payload[:amount] = amount if payload
          @data.incr(name, amount, options)
        end
      rescue ::Couchbase::Error::Base => e
        logger.error("#{e.class}: #{e.message}") if logger
        raise if @raise_errors
        false
      end

      # Decrement an integer value in the cache.
      #
      # @since 1.2.0.dp5
      #
      # @param [String] name name for the key
      # @param [Fixnum] amount (1) the delta value
      # @param [Hash] options
      # @option options [Fixnum] :expires_in the expiration time on the
      #   cache in seconds. Values larger than 30*24*60*60 seconds (30 days)
      #   are interpreted as absolute times (from the epoch).
      # @option options [Fixnum] :initial this option allows to initialize
      #   the value if the key is missing in the cache
      #
      # @return [Fixnum] new value
      def decrement(name, amount = 1, options = nil)
        options ||= {}
        name = expanded_key name

        if ttl = options.delete(:expires_in)
          options[:ttl] ||= ttl
        end
        options[:create] = true
        instrument(:decrement, name, options) do |payload|
          payload[:amount] = amount if payload
          @data.decr(name, amount, options)
        end
      rescue ::Couchbase::Error::Base => e
        logger.error("#{e.class}: #{e.message}") if logger
        raise if @raise_errors
        false
      end

      # Get the statistics from the memcached servers.
      #
      # @since 1.2.0.dp5
      #
      # @return [Hash]
      def stats(*arg)
        @data.stats(*arg)
      end

      protected

      # Read an entry from the cache.
      def read_entry(key, options) # :nodoc:
        @data.get(key, options)
      rescue ::Couchbase::Error::Base => e
        logger.error("#{e.class}: #{e.message}") if logger
        raise if @raise_errors
        nil
      end

      # Write an entry to the cache.
      def write_entry(key, value, options) # :nodoc:
        method = if options[:unless_exists] || options[:unless_exist]
                   :add
                 else
                   :set
                 end
        if ttl = options.delete(:expires_in)
          options[:ttl] ||= ttl
        end
        @data.send(method, key, value, options)
      rescue ::Couchbase::Error::Base => e
        logger.error("#{e.class}: #{e.message}") if logger
        raise if @raise_errors || method == :add
        false
      end

      # Delete an entry from the cache.
      def delete_entry(key, options) # :nodoc:
        @data.delete(key, options)
      rescue ::Couchbase::Error::Base => e
        logger.error("#{e.class}: #{e.message}") if logger
        raise if @raise_errors
        false
      end

      private

      # Expand key to be a consistent string value. Invoke +cache_key+ if
      # object responds to +cache_key+. Otherwise, to_param method will be
      # called. If the key is a Hash, then keys will be sorted alphabetically.
      def expanded_key(key) # :nodoc:
        return validate_key(key.cache_key.to_s) if key.respond_to?(:cache_key)

        case key
        when Array
          if key.size > 1
            key = key.collect{|element| expanded_key(element)}
          else
            key = key.first
          end
        when Hash
          key = key.sort_by { |k,_| k.to_s }.collect{|k,v| "#{k}=#{v}"}
        end

        validate_key(key.respond_to?(:to_param) ? key.to_param : key)
      end

      def validate_key(key)
        if key_with_prefix(key).length > 250
          key = "#{key[0, max_length_before_prefix]}:md5:#{Digest::MD5.hexdigest(key)}"
        end
        return key
      end

      def key_with_prefix(key)
        (ns = @key_prefix) ? "#{ns}#{key}" : key
      end

      def max_length_before_prefix
        @max_length_before_prefix ||= 212 - (@key_prefix || '').size
      end

      module Threadsafe
        def self.extended(obj)
          obj.init_threadsafe
        end

        def get(*)
          @lock.synchronize do
            super
          end
        end

        def send(*)
          @lock.synchronize do
            super
          end
        end

        def delete(*)
          @lock.synchronize do
            super
          end
        end

        def incr(*)
          @lock.synchronize do
            super
          end
        end

        def decr(*)
          @lock.synchronize do
            super
          end
        end

        def stats(*)
          @lock.synchronize do
            super
          end
        end

        def init_threadsafe
          @lock = Monitor.new
        end
      end
    end
  end
end
