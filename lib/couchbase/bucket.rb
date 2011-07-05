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
  class Bucket
    attr_accessor :type, :hash_algorithm, :replicas_count, :servers, :vbuckets, :nodes

    def initialize(bucket_info)
      @type = bucket_info['bucketType']
      @nodes = bucket_info['nodes'].map do |node|
        Node.new(node['status'], node['hostname'], node['ports'], node['couchApiBase'])
      end
      if @type == 'membase'
        server_map = bucket_info['vBucketServerMap']
        @hash_algorithm = server_map['hashAlgorithm']
        @replicas_count = server_map['numReplicas']
        @servers = server_map['serverList']
        @vbuckets = server_map['vBucketMap']
        @couch_api_base = server_map['couchApiBase']
      end
    end

    def next_node
      nodes.shuffle.first
    end
  end
end
