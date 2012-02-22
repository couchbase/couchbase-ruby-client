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
require 'yajl/json_gem'
require 'uri'
require 'couchbase_ext'
require 'couchbase/bucket'

# Couchbase ruby client
module Couchbase

  class << self
    # The method +connect+ initializes new Bucket instance with all arguments passed.
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
    # @return [Bucket] connection instance
    def connect(*options)
      Bucket.new(*options)
    end
    alias :new :connect

    # Default connection options
    #
    # @example Using {Couchbase#connection_options} to change the bucket
    #   Couchbase.connection_options = {:bucket => 'blog'}
    #   Couchbase.bucket.name     #=> "blog"
    #
    # @return [Hash, String]
    attr_accessor :connection_options

    # @private the thread local storage
    def thread_storage
      Thread.current[:couchbase] ||= {}
    end

    # The connection instance for current thread
    #
    # @example
    #   Couchbase.bucket.set("foo", "bar")
    #
    # @return [Bucket]
    def bucket
      thread_storage[:bucket] ||= connect(*connection_options)
    end

    # Set a connection instance for current thread
    #
    # @return [Bucket]
    def bucket=(connection)
      thread_storage[:bucket] = connection
    end

  end

end
