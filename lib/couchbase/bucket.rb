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

require 'uri'

module Couchbase

  # This class in charge of all stuff connected to communication with
  # Couchbase. It includes CouchDB and Memcached APIs. Also it includes
  # methods for HTTP transport from RestClient.

  class Bucket
    include RestClient
    include Couchdb
    include Memcached

    attr_accessor :pool_uri, :environment, :type, :nodes,
      :streaming_uri, :name, :uri, :vbuckets

    # Initializes connection using +pool_uri+ and optional
    # +:bucket_name+ and +:bucket_password+ (for protected buckets). Bucket
    # name will be used as a username for all authorizations (SASL for
    # Memcached API and Basic for HTTP). It also accepts +:environment+
    # parameter wich intended to let library know what mode it should
    # use when it applicable (for example it skips/preserves design
    # documents with 'dev_' prefix for CouchDB API). You can specify
    # any string starting from 'dev' or 'test' to activate development
    # mode.
    #
    # Also starts thread which will simultanuously listen for
    # configuration update via +streaming_uri+. Server should push
    # update notification about adding or removing nodes from cluster.
    #
    # Raises ArgumentError when it cannot find specified bucket in given
    # pool.
    def initialize(pool_uri, options = {})
      @latch = Latch.new(:in_progress, :ready)
      @name = options[:bucket_name] || "default"
      @pool_uri = URI.parse(pool_uri)
      @environment = if options[:environment].to_s =~ /^(dev|test)/
                       :development
                     else
                       :production
                     end
      config = http_get("#{@pool_uri}/buckets").detect do |bucket|
                 bucket['name'] == @name
               end
      unless config
        raise ArgumentError,
          "There no such bucket with name '#{@name}' in pool #{pool_uri}"
      end
      @uri = @pool_uri.merge(config['uri'])
      @streaming_uri = @pool_uri.merge(config['streamingUri'])
      if password = options[:bucket_password]
        @credentials = {:username => @name, :password => password}
      end
      super

      # Considering all initialization stuff completed and now we can
      # start config listener
      listen_for_config_changes

      @latch.wait
    end

    # Select next node for work with Couchbase. Currently it makes sense
    # only for couchdb API, because memcached client works using moxi.
    def next_node
      nodes.shuffle.first
    end

    # Perform configuration using configuration cache. It turn all URIs
    # into full form (with schema, host and port).
    #
    # You can override this method in included modules or descendants if
    # you'd like to reconfigure them when new configuration arrives from
    # server.
    def setup(config)
      @type = config['bucketType']
      @nodes = config['nodes'].map do |node|
        Node.new(node['status'],
                 node['hostname'].split(':').first,
                 node['ports'],
                 node['couchApiBase'])
      end
      if @type == 'membase'
        @vbuckets = config['vBucketServerMap']['vBucketMap']
      end
      super
      @latch.toggle
    end

    private

    # Run background thread to listen for configuration changes.
    # Rewrites configuration for each update. Curl::Multi uses select()
    # call when waiting for data, so is should be efficient use ruby
    # threads here.
    def listen_for_config_changes
      Thread.new do
        multi = Curl::Multi.new
        multi.add(mk_curl(@streaming_uri.to_s))
        multi.perform
      end
    end

    def mk_curl(url)
      Curl::Easy.new(url) do |curl|
        curl.useragent = "couchbase-ruby-client/#{Couchbase::VERSION}"
        if @credentials
          curl.http_auth_types = :basic
          curl.username = @credentials[:username]
          curl.password = @credentials[:password]
        end
        curl.verbose = true if Kernel.respond_to?(:debugger)
        curl.on_body do |data|
          config = Yajl::Parser.parse(data)
          setup(config) if config
          data.length
        end
      end
    end
  end
end
