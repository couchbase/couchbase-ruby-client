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
require 'couchbase_ext'
require 'couchbase/bucket'

# Couchbase ruby client
module Couchbase

  class << self
    # The method +new+ initializes new Bucket instance with all arguments passed.
    #
    # @example Use default values for all options
    #   Couchbase.new
    #
    # @example Establish connection with couchbase default pool and default bucket
    #   Couchbase.new("http://localhost:8091/pools/default")
    #
    # @example Select custom bucket
    #   Couchbase.new("http://localhost:8091/pools/default", :bucket => 'blog')
    #
    # @example Specify bucket credentials
    #   Couchbase.new("http://localhost:8091/pools/default", :bucket => 'blog', :username => 'bucket', :password => 'secret')
    #
    # @return [Bucket] connection instance
    def new(*args)
      Bucket.new(*args)
    end
  end

end
