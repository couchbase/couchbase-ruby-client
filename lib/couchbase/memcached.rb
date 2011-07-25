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
    FLAGS = ::Memcached::FLAGS

    attr_reader :memcached, :data_mode

    # Initializes Memcached API. It builds server list using moxi ports.
    def initialize(pool_uri, options = {})
      @data_mode = options[:data_mode] || :json
      @options = options.dup
      if @credentials
        @options[:credentials] = [@credentials[:username], @credentials[:password]]
      end
      super
    end

    def clone
      double = super
      double.instance_variable_set("@memcached", @memcached.clone)
      double.instance_variable_set("@memcached", @memcached.options[:default_ttl])
      double.instance_variable_set("@memcached", @memcached.options[:default_ttl])
      double
    end
    alias :dup :clone #:nodoc:

    def options
      @memcached.options
    end

    def servers
      @memcached.servers
    end

    def servers=(servers)
      @memcached.set_servers(servers)
    end

    def prefix_key=(key)
      @memcached.prefix_key(key)
    end
    alias :namespace= :prefix_key=

    def prefix_key
      @memcached.prefix_key
    end
    alias :namespace :prefix_key

    def server_by_key(key)
      @memcached.server_by_key(key)
    end

    def stats(subcommand = nil)
      @memcached.stats(subcommand)
    end

    def quit
      @memcached.quit
    end

    def flush
      @memcached.flush
    end

    def reset(current_servers = nil)
      @memcached.reset(current_servers)
    end

    def get(keys, data_mode = @data_mode)
      decode(@memcached.get(keys, data_mode == :marshal), data_mode)
    end

    def [](keys)
      decode(@memcached.get(keys, @data_mode == :marshal), data_mode)
    end

    def get_from_last(keys, data_mode=@data_mode)
      decode(@memcached.get(keys, data_mode == :marshal), data_mode)
    end

    def set(key, value, ttl=@default_ttl, data_mode=@data_mode, flags=FLAGS)
      @memcached.set(key, encode(value, data_mode), ttl, data_mode == :marshal, flags)
    end

    def []=(key, value)
      @memcached.set(key, encode(value, @data_mode), @default_ttl, @data_mode == :marshal, FLAGS)
    end

    def cas(key, ttl=@default_ttl, data_mode=@data_mode, flags=FLAGS, &block)
      @memcached.cas(key, ttl, data_mode == :marshal, flags) do |value|
        value = decode(value, data_mode)
        encode(block.call(value), data_mode)
      end
    end
    alias :compare_and_swap :cas

    def add(key, value, ttl=@default_ttl, data_mode=@data_mode, flags=FLAGS)
      @memcached.add(key, encode(value, data_mode), ttl, data_mode == :marshal, flags)
    end

    def replace(key, value, ttl=@default_ttl, data_mode=@data_mode, flags=FLAGS)
      @memcached.replace(key, encode(value, data_mode), ttl, data_mode == :marshal, flags)
    end

    def append(key, value)
      @memcached.append(key, value)
    end

    def prepend(key, value)
      @memcached.prepend(key, value)
    end

    def delete(key)
      @memcached.delete(key)
    end

    def increment(key, offset=1)
      @memcached.increment(key, offset)
    end
    alias :incr :increment

    def decrement(key, offset=1)
      @memcached.decrement(key, offset)
    end
    alias :decr :decrement

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
      when :json
        Yajl::Encoder.encode(value)
      when :marshal, :plain
        value   # encoding handled by memcached library internals
      end
    end

    def decode(value, mode)
      case mode
      when :json
        Yajl::Parser.parse(value)
      when :marshal, :plain
        value   # encoding handled by memcached library internals
      end
    end
  end
end
