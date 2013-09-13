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

require 'couchbase/version'
require 'yaji'
require 'uri'
require 'couchbase/transcoder'
require 'couchbase_ext'
require 'couchbase/constants'
require 'couchbase/utils'
require 'couchbase/bucket'
require 'couchbase/view_row'
require 'couchbase/view'
require 'couchbase/result'
require 'couchbase/cluster'


# Couchbase ruby client
module Couchbase

  if RUBY_VERSION.to_f >= 1.9
    autoload(:ConnectionPool, 'couchbase/connection_pool')
  end

  class << self
    # The method +connect+ initializes new Bucket instance with all arguments passed.
    #
    # @since 1.0.0
    #
    # @see Bucket#initialize
    #
    # @example Use default values for all options
    #   Couchbase.connect
    #
    # @example Establish connection with couchbase default pool and default bucket
    #   Couchbase.connect("http://localhost:8091/pools/default")
    #
    # @example Select custom bucket
    #   Couchbase.connect("http://localhost:8091/pools/default", :bucket => 'blog')
    #
    # @example Specify bucket credentials
    #   Couchbase.connect("http://localhost:8091/pools/default", :bucket => 'blog', :username => 'bucket', :password => 'secret')
    #
    # @example Use URL notation
    #   Couchbase.connect("http://bucket:secret@localhost:8091/pools/default/buckets/blog")
    #
    # @return [Bucket] connection instance
    def connect(*options)
      Bucket.new(*(options.flatten))
    end
    alias :new :connect

    # Default connection options
    #
    # @since 1.1.0
    #
    # @example Using {Couchbase#connection_options} to change the bucket
    #   Couchbase.connection_options = {:bucket => 'blog'}
    #   Couchbase.bucket.name     #=> "blog"
    #
    # @return [Hash, String]
    attr_accessor :connection_options

    # @private the thread local storage
    def thread_storage
      Thread.current[:couchbase] ||= { :pid => Process.pid, :bucket => {} }
    end

    # @private resets thread local storage if process ids don't match
    # see 13.3.1: http://www.modrails.com/documentation/Users%20guide%20Apache.html
    def verify_connection!
      reset_thread_storage! if thread_storage[:pid] != Process.pid
    end

    # @private resets thread local storage
    def reset_thread_storage!
      Thread.current[:couchbase] = nil
    end

    # The connection instance for current thread
    #
    # @since 1.1.0
    #
    # @see Couchbase.connection_options
    #
    # @example
    #   Couchbase.bucket.set("foo", "bar")
    #
    # @example Set connection options using Hash
    #   Couchbase.connection_options = {:node_list => ["example.com:8091"]}
    #   Couchbase.bucket("slot1").set("foo", "bar")
    #   Couchbase.bucket("slot1").bucket #=> "default"
    #   Couchbase.connection_options[:bucket] = "test"
    #   Couchbase.bucket("slot2").bucket #=> "test"
    #
    # @example Set connection options using URI
    #   Couchbase.connection_options = "http://example.com:8091/pools"
    #   Couchbase.bucket("slot1").set("foo", "bar")
    #   Couchbase.bucket("slot1").bucket #=> "default"
    #   Couchbase.connection_options = "http://example.com:8091/pools/buckets/test"
    #   Couchbase.bucket("slot2").bucket #=> "test"
    #
    # @return [Bucket]
    def bucket(name = nil)
      verify_connection!
      name ||= case @connection_options
               when Hash
                 @connection_options[:bucket]
               when String
                 path = URI.parse(@connection_options).path
                 path[%r(^(/pools/([A-Za-z0-9_.-]+)(/buckets/([A-Za-z0-9_.-]+))?)?), 3] || "default"
               else
                 "default"
               end
      thread_storage[:bucket][name] ||= connect(connection_options)
    end

    # Set a connection instance for current thread
    #
    # @since 1.1.0
    #
    # @return [Bucket]
    def bucket=(connection, name = nil)
      verify_connection!
      name ||= @connection_options && @connection_options[:bucket] || "default"
      thread_storage[:bucket][name] = connection
    end
    alias set_bucket bucket=

  end

end
