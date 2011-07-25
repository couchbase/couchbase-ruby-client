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

module Couchbase
  class Connection
    include RestClient
    include Couchdb
    include Memcached

    attr_accessor :pool_uri, :bucket

    def initialize(pool_uri, options = {})
      @pool_uri = pool_uri
      @bucket_name = options[:bucket_name] || "default"
      desc = http_get("#{@pool_uri}/buckets").detect do |bucket|
               bucket['name'] == @bucket_name
             end
      unless desc
        raise ArgumentError,
          "There no such bucket with name '#{@bucket_name}' in pool #{pool_uri}"
      end
      if password = options[:bucket_password]
        @credentials = {:username => @bucket_name, :password => password}
      end
      @bucket = Bucket.new(pool_uri, desc, @credentials)
      super
    end
  end
end
