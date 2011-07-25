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
  autoload :Bucket,         'couchbase/bucket'
  autoload :Couchdb,        'couchbase/couchdb'
  autoload :Document,       'couchbase/document'
  autoload :HttpStatus,     'couchbase/http_status'
  autoload :Memcached,      'couchbase/memcached'
  autoload :Node,           'couchbase/node'
  autoload :Pool,           'couchbase/pool'
  autoload :RestClient,     'couchbase/rest_client'
  autoload :VERSION,        'couchbase/version'

  class << self
    # The method +new+ initializes new Bucket instance with all arguments passed.
    #
    # === Examples
    #   Couchbase.new("http://localhost:8091/pools/default") #=> establish connection with couchbase default pool and default bucket
    #   Couchbase.new("http://localhost:8091/pools/default", :bucket_name => 'blog') #=> select custom bucket
    #   Couchbase.new("http://localhost:8091/pools/default", :bucket_name => 'blog', :bucket_password => 'secret') #=> specify password for bucket (and SASL auth for memcached client)
    def new(*args)
      Bucket.new(*args)
    end
  end

end
