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

  class Cluster

    # Establish connection to the cluster for administration
    #
    # @param [Hash] options The connection parameter
    # @option options [String] :username The username
    # @option options [String] :password The password
    # @option options [String] :pool ("default") The pool name
    # @option options [String] :hostname ("localhost") The hostname
    # @option options [String] :port (8091) The port
    def initialize(options = {})
      if options[:username].nil? || options[:password].nil?
        raise ArgumentError, "username and password mandatory to connect to the cluster"
      end
      @connection = Bucket.new(options.merge(:type => :cluster))
    end

    # Create data bucket
    #
    # @param [String] name The name of the bucket
    # @param [Hash] options The bucket parameters
    # @option options [String] :bucket_type ("couchbase") The type of the
    #   bucket. Possible values are "memcached" and "couchbase".
    # @option options [Fixnum] :ram_quota (100) The RAM quota in megabytes.
    # @option options [Fixnum] :replica_number (1) The number of replicas of
    #   each document. Minimum 0, maximum 3.
    # @option options [String] :auth_type ("sasl") The authentication type.
    #   Possible values are "sasl" and "none". Note you should specify free
    #   port for "none"
    # @option options [Fixnum] :proxy_port The port for moxi
    # @option options [true, false] :replica_index (true) Disable or
    #   enable indexes for bucket replicas
    # @option options [true, false] :flush_enabled (false) Enables the
    #   'flush all' functionality on the specified bucket.
    # @option options [true, false] :parallel_db_and_view_compaction (false)
    #   Indicates whether database and view files on disk can be
    #   compacted simultaneously
    #
    def create_bucket(name, options = {})
      defaults = {
        :type => "couchbase",
        :ram_quota => 100,
        :replica_number => 1,
        :auth_type => "sasl",
        :sasl_password => "",
        :proxy_port => nil,
        :flush_enabled => false,
        :replica_index => true,
        :parallel_db_and_view_compaction => false
      }
      options = defaults.merge(options)
      params = {"name" => name}
      params["bucketType"] = options[:type]
      params["ramQuotaMB"] = options[:ram_quota]
      params["replicaNumber"] = options[:replica_number]
      params["authType"] = options[:auth_type]
      params["saslPassword"] = options[:sasl_password]
      params["proxyPort"] = options[:proxy_port]
      params["flushEnabled"] = !!options[:flush_enabled]
      params["replicaIndex"] = !!options[:replica_index]
      params["parallelDBAndViewCompaction"] = !!options[:parallel_db_and_view_compaction]
      payload = Utils.encode_params(params.reject!{|k, v| v.nil?})
      request = @connection.make_http_request("/pools/default/buckets",
                                              :content_type => "application/x-www-form-urlencoded",
                                              :type => :management,
                                              :method => :post,
                                              :extended => true,
                                              :body => payload)
      response = nil
      request.on_body do |r|
        response = r
        response.instance_variable_set("@operation", :create_bucket)
        yield(response) if block_given?
      end
      request.continue
      response
    end

    # Delete the data bucket
    #
    # @param [String] name The name of the bucket
    # @param [Hash] options
    def delete_bucket(name, options = {})
      request = @connection.make_http_request("/pools/default/buckets/#{name}",
                                              :type => :management,
                                              :method => :delete,
                                              :extended => true)
      response = nil
      request.on_body do |r|
        response = r
        response.instance_variable_set("@operation", :delete_bucket)
        yield(response) if block_given?
      end
      request.continue
      response
    end

  end

end
