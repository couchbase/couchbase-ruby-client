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
    include Couchdb
    include Memcached

    attr_accessor :pool_uri, :buckets, :bucket_name, :bucket_password

    def initialize(pool_uri, options = {})
      @pool_uri = pool_uri
      @bucket_name = options[:bucket_name] || "default"
      @bucket_password = bucket_password
      @buckets = {}
      Couchbase.get("#{@pool_uri}/buckets").each do |bucket_info|
        @buckets[bucket_info["name"]] = Bucket.new(bucket_info)
      end
      super
    end

    def bucket(name = @bucket_name, password = @bucket_password)
      @buckets[bucket_name]
    end
  end
end
